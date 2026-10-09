// Copyright 2026, Aiven, https://aiven.io/
//
// This file is under the Apache License, Version 2.0.
// See the file `LICENSE` for details.

// Package syslogframing ports journalpump/rsyslog.py (plan Task 19): the
// syslog message formatters (RFC5424, RFC3164, and the rsyslog
// "logline" custom template), the PRI computation, the RFC6587
// octet-counted / non-transparent TCP framing, truncation and
// escape_newlines handling, and the TCP+TLS client with its one-shot
// send retry. The acceptance criterion is byte-identical wire output to
// Python's SyslogTcpClient.
//
// Task 20 (the rsyslog sender) builds on this package; the package has
// no sender wiring of its own.
package syslogframing

import (
	"fmt"
	"strconv"
	"strings"
	"time"
)

// NilValue is Python's NILVALUE ("-").
const NilValue = "-"

// Fields are the inputs every formatter receives — the keyword arguments
// Python passes to _rfc_5424_formatter / _rfc_3164_formatter /
// the custom formatter.
type Fields struct {
	Pri         int
	RFC3339Date string
	RFC3164Date string
	Hostname    string
	AppID       string
	ProcID      string
	MsgID       string
	Msg         string
	// SD is Python's structured-data argument: nil means "no structured
	// data" (the formatter omits the block); a non-nil pointer — even to
	// "" — emits " []" exactly like a non-None Python value.
	SD *string
}

// RFC5424 formats f as an RFC 5424 syslog message (rsyslog.py:17-33),
// including the trailing newline, UTF-8 with invalid bytes replaced.
func RFC5424(f Fields) []byte {
	var b strings.Builder
	fmt.Fprintf(&b, "<%d>1 %s %s %s %s %s", f.Pri, f.RFC3339Date, f.Hostname, f.AppID, f.ProcID, f.MsgID)
	if f.SD != nil {
		b.WriteString(" [")
		b.WriteString(*f.SD)
		b.WriteByte(']')
	}
	b.WriteByte(' ')
	b.WriteString(f.Msg)
	b.WriteByte('\n')
	return []byte(b.String())
}

// RFC3164 formats f as an RFC 3164 syslog message (rsyslog.py:37-50).
func RFC3164(f Fields) []byte {
	var b strings.Builder
	fmt.Fprintf(&b, "<%d>%s %s %s[%s]: %s\n", f.Pri, f.RFC3164Date, f.Hostname, f.AppID, f.ProcID, f.Msg)
	return []byte(b.String())
}

// GenerateFormat ports _generate_format (rsyslog.py:72-96): the simple
// rsyslog-%token% → Python-brace placeholder converter. Unknown tokens
// become "-"; literal braces are doubled so a later format pass emits
// them verbatim; a trailing newline is appended when missing.
func GenerateFormat(logline string) string {
	frmt := make([]byte, 0, len(logline)+1)
	inToken := false
	var token strings.Builder
	for _, c := range []byte(logline) {
		switch {
		case c == '%' && !inToken:
			inToken = true
			token.Reset()
		case c == '%' && inToken:
			if repl, ok := loglineVars[token.String()]; ok {
				frmt = append(frmt, repl...)
			} else {
				frmt = append(frmt, '-')
			}
			token.Reset()
			inToken = false
		case inToken:
			token.WriteByte(c)
		default:
			frmt = append(frmt, c)
			if c == '{' || c == '}' {
				frmt = append(frmt, c)
			}
		}
	}
	if len(frmt) == 0 || frmt[len(frmt)-1] != '\n' {
		frmt = append(frmt, '\n')
	}
	return string(frmt)
}

// loglineVars is _LOGLINE_VARS (rsyslog.py:57-69): rsyslog token →
// braces placeholder. The empty token maps to "%".
var loglineVars = map[string]string{
	"pri":                      "{pri}",
	"protocol-version":         "1",
	"timestamp":                "{rfc3164date}",
	"timestamp:::date-rfc3339": "{rfc3339date}",
	"HOSTNAME":                 "{hostname}",
	"app-name":                 "{app_id}",
	"procid":                   "{proc_id}",
	"msgid":                    "{msg_id}",
	"msg":                      "{msg}",
	"structured-data":          "{sd}",
	"":                         "%",
}

// FormatCustom renders a generated logline template (the output of
// GenerateFormat) with f — the Go equivalent of Python's
// custom.format(**kwargs). Only the placeholder names the generator can
// emit are recognized; an unescaped/unknown placeholder is an error just
// as Python's str.format raises.
func FormatCustom(template string, f Fields) ([]byte, error) {
	values := map[string]string{
		"pri":         strconv.Itoa(f.Pri),
		"rfc3339date": f.RFC3339Date,
		"rfc3164date": f.RFC3164Date,
		"hostname":    f.Hostname,
		"app_id":      f.AppID,
		"proc_id":     f.ProcID,
		"msg_id":      f.MsgID,
		"msg":         f.Msg,
		"sd":          derefOr(f.SD, ""),
	}
	var b strings.Builder
	for i := 0; i < len(template); {
		c := template[i]
		switch c {
		case '{':
			if i+1 < len(template) && template[i+1] == '{' {
				b.WriteByte('{')
				i += 2
				continue
			}
			end := strings.IndexByte(template[i:], '}')
			if end < 0 {
				return nil, fmt.Errorf("unmatched '{' in logline template")
			}
			name := template[i+1 : i+end]
			value, ok := values[name]
			if !ok {
				return nil, fmt.Errorf("unknown logline field %q", name)
			}
			b.WriteString(value)
			i += end + 1
		case '}':
			if i+1 < len(template) && template[i+1] == '}' {
				b.WriteByte('}')
				i += 2
				continue
			}
			return nil, fmt.Errorf("single '}' in logline template")
		default:
			b.WriteByte(c)
			i++
		}
	}
	return []byte(b.String()), nil
}

func derefOr(p *string, def string) string {
	if p == nil {
		return def
	}
	return *p
}

// Format is the RFC selector used by the client (rsyslog.py:123-132):
// it resolves an RFC name to a formatter and mirrors Python's errors.
func Format(rfc, logFormat string) (func(Fields) ([]byte, error), error) {
	switch rfc {
	case "RFC5424":
		return func(f Fields) ([]byte, error) { return RFC5424(f), nil }, nil
	case "RFC3164":
		return func(f Fields) ([]byte, error) { return RFC3164(f), nil }, nil
	case "CUSTOM":
		if logFormat == "" {
			return nil, fmt.Errorf("log_format must be given when using CUSTOM format")
		}
		tmpl := GenerateFormat(logFormat)
		return func(f Fields) ([]byte, error) { return FormatCustom(tmpl, f) }, nil
	default:
		return nil, fmt.Errorf("Unknown message format %q requested", rfc) //nolint:staticcheck // error-text parity with rsyslog.py:132
	}
}

// Priority ports the PRI computation (rsyslog.py:231-234): a valid
// facility (0..23) and severity (0..7) combine; anything out of range
// falls back to 13.
func Priority(facility, severity int) int {
	if facility >= 0 && facility <= 23 && severity >= 0 && severity <= 7 {
		return facility*8 + severity
	}
	return 13
}

// RFC3164Date ports the timestamp reformat (rsyslog.py:243-244): parse
// the first 19 characters as "%Y-%m-%dT%H:%M:%S" (UTC) and render
// "%b %d %H:%M:%S". An unparseable prefix is an error, matching Python's
// ValueError.
func RFC3164Date(timestamp string) (string, error) {
	prefix := timestamp
	if len(prefix) > 19 {
		prefix = prefix[:19]
	}
	parsed, err := time.ParseInLocation("2006-01-02T15:04:05", prefix, time.UTC)
	if err != nil {
		return "", fmt.Errorf("invalid timestamp %q: %w", timestamp, err)
	}
	return parsed.Format("Jan 02 15:04:05"), nil
}
