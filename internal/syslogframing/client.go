// Copyright 2026, Aiven, https://aiven.io/
//
// This file is under the Apache License, Version 2.0.
// See the file `LICENSE` for details.

package syslogframing

import (
	"crypto/tls"
	"crypto/x509"
	"errors"
	"fmt"
	"io"
	"net"
	"os"
	"strconv"
	"strings"
	"syscall"
	"time"
)

// Conn is the consumer-side socket seam (thread B custom-fake rule): the
// real *net.Conn / *tls.Conn satisfies it, and tests inject a recording
// stub. Write must send all bytes (sendall semantics) or report an error.
type Conn interface {
	Write(p []byte) (int, error)
	Close() error
}

// Config carries the SyslogTcpClient constructor arguments
// (rsyslog.py:100-141).
type Config struct {
	Server              string
	Port                int
	RFC                 string // "RFC5424" | "RFC3164" | "CUSTOM"
	MaxMsg              int    // <= 0 means DefaultMaxMsg
	Protocol            string // "" == "PLAINTEXT"
	CACerts             string
	KeyFile             string
	CertFile            string
	LogFormat           string
	OctetCountedFraming bool
	EscapeNewlines      bool

	// Dial is a test/transport seam: when set it replaces the network
	// dial and the TLS wrapping, returning an already-connected Conn
	// (Python's tests patch _connect and assign .socket).
	Dial func() (Conn, error)
	// TLSConfig, when non-nil, wraps the dialed connection for the real
	// path (SNI uses Server). Mutually exclusive with Dial.
	TLSConfig *tls.Config
	// Now is the clock seam for socket deadline paths synctest cannot
	// drive (plan Task 19); defaults to time.Now.
	Now func() time.Time
}

// Client ports journalpump.rsyslog.SyslogTcpClient.
type Client struct {
	cfg     Config
	format  func(Fields) ([]byte, error)
	framing Framing
	now     func() time.Time
	conn    Conn
}

// NewClient builds the formatter, TLS context and initial connection,
// mirroring SyslogTcpClient.__init__ (including its error cases).
func NewClient(cfg Config) (*Client, error) {
	format, err := Format(cfg.RFC, cfg.LogFormat)
	if err != nil {
		return nil, err
	}
	if cfg.Protocol == "" {
		cfg.Protocol = "PLAINTEXT"
	}
	if cfg.TLSConfig == nil && cfg.Dial == nil && (cfg.CACerts != "" || cfg.Protocol == "SSL") {
		tlsCfg, err := buildTLSConfig(cfg)
		if err != nil {
			return nil, err
		}
		cfg.TLSConfig = tlsCfg
	}
	now := cfg.Now
	if now == nil {
		now = time.Now
	}
	c := &Client{
		cfg:     cfg,
		format:  format,
		framing: NewFraming(cfg.MaxMsg, cfg.OctetCountedFraming),
		now:     now,
	}
	if err := c.connect(); err != nil {
		return nil, err
	}
	return c, nil
}

// buildTLSConfig mirrors ssl.create_default_context(cafile=cacerts) with
// minimum_version TLSv1_2 and an optional client certificate chain.
func buildTLSConfig(cfg Config) (*tls.Config, error) {
	tlsCfg := &tls.Config{MinVersion: tls.VersionTLS12, ServerName: cfg.Server}
	if cfg.CACerts != "" {
		pem, err := os.ReadFile(cfg.CACerts)
		if err != nil {
			return nil, err
		}
		pool := x509.NewCertPool()
		if !pool.AppendCertsFromPEM(pem) {
			return nil, fmt.Errorf("no certificates found in %s", cfg.CACerts)
		}
		tlsCfg.RootCAs = pool
	}
	if cfg.CertFile != "" {
		keyFile := cfg.KeyFile
		if keyFile == "" {
			keyFile = cfg.CertFile
		}
		cert, err := tls.LoadX509KeyPair(cfg.CertFile, keyFile)
		if err != nil {
			return nil, err
		}
		tlsCfg.Certificates = []tls.Certificate{cert}
	}
	return tlsCfg, nil
}

// connect ports _connect (rsyslog.py:143-165).
func (c *Client) connect() error {
	if c.cfg.Dial != nil {
		conn, err := c.cfg.Dial()
		if err != nil {
			return err
		}
		c.conn = conn
		return nil
	}
	addr := net.JoinHostPort(c.cfg.Server, strconv.Itoa(c.cfg.Port))
	conn, err := net.Dial("tcp", addr)
	if err != nil {
		var dnsErr *net.DNSError
		if errors.As(err, &dnsErr) {
			return fmt.Errorf("Invalid address %s:%d", c.cfg.Server, c.cfg.Port) //nolint:staticcheck // error-text parity with rsyslog.py:161
		}
		return err
	}
	if c.cfg.TLSConfig != nil {
		// Passing ServerName enables SNI so the server can pick the right
		// certificate (rsyslog.py:151-153).
		conn = tls.Client(conn, c.cfg.TLSConfig)
	}
	c.conn = conn
	return nil
}

// close ports SyslogTcpClient.close (rsyslog.py:167-175).
func (c *Client) close() {
	if c.conn == nil {
		return
	}
	_ = c.conn.Close()
	c.conn = nil
}

// Close releases the connection.
func (c *Client) Close() { c.close() }

// Send ports SyslogTcpClient.send (rsyslog.py:177-209): frame the message
// and write it, retrying exactly once when the failure is retriable.
func (c *Client) Send(message []byte) error {
	for retry := true; ; retry = false {
		if c.conn == nil {
			if err := c.connect(); err != nil {
				c.close()
				if !retry || !shouldRetry(err) {
					return err
				}
				continue
			}
		}
		frame := c.framing.Frame(message)
		if err := writeAll(c.conn, frame); err == nil {
			return nil
		} else { //nolint:revive // retry decision below needs err in scope
			c.close()
			if !retry || !shouldRetry(err) {
				return err
			}
		}
	}
}

// writeAll gives Conn sendall semantics.
func writeAll(conn Conn, p []byte) error {
	for len(p) > 0 {
		n, err := conn.Write(p)
		if err != nil {
			return err
		}
		if n == 0 {
			return io.ErrShortWrite
		}
		p = p[n:]
	}
	return nil
}

// shouldRetry ports _should_retry (rsyslog.py:211-216): EPIPE / ECONNRESET
// / ETIMEDOUT, or the TLS unexpected-EOF (Python's ssl.SSLEOFError).
func shouldRetry(err error) bool {
	if errors.Is(err, syscall.EPIPE) || errors.Is(err, syscall.ECONNRESET) || errors.Is(err, syscall.ETIMEDOUT) {
		return true
	}
	// ssl.SSLEOFError ("EOF occurred in violation of protocol") surfaces
	// from crypto/tls as an unexpected EOF.
	return errors.Is(err, io.ErrUnexpectedEOF)
}

// LogInput are the SyslogTcpClient.log keyword arguments
// (rsyslog.py:218-230). Pointer fields follow Python's Optional
// semantics: nil is None, a non-nil empty string is a present "".
type LogInput struct {
	Facility  int
	Severity  int
	Timestamp string
	Hostname  *string
	Program   *string
	PID       any
	MsgID     *string
	Msg       *string
	SD        *string
}

// Log ports SyslogTcpClient.log (rsyslog.py:218-258): defaults, optional
// newline escaping, RFC3164 timestamp reformatting, formatting, send.
func (c *Client) Log(in LogInput) error {
	appID := defaultStr(in.Program)
	hostname := defaultStr(in.Hostname)
	procID := defaultPID(in.PID)
	msgID := defaultStr(in.MsgID)
	message := defaultStr(in.Msg)
	if c.cfg.EscapeNewlines {
		message = strings.ReplaceAll(message, "\r", `\r`)
		message = strings.ReplaceAll(message, "\n", `\n`)
	}
	rfc3164, err := RFC3164Date(in.Timestamp)
	if err != nil {
		return err
	}
	body, err := c.format(Fields{
		Pri:         Priority(in.Facility, in.Severity),
		RFC3339Date: in.Timestamp,
		RFC3164Date: rfc3164,
		Hostname:    hostname,
		AppID:       appID,
		ProcID:      procID,
		MsgID:       msgID,
		Msg:         message,
		SD:          in.SD,
	})
	if err != nil {
		return err
	}
	return c.Send(body)
}

// defaultStr is Python's `v if v else NILVALUE` for an Optional[str].
func defaultStr(v *string) string {
	if v == nil || *v == "" {
		return NilValue
	}
	return *v
}

// defaultPID is Python's `str(pid) if pid else NILVALUE`: any falsy value
// (nil, "", integer 0) becomes NILVALUE.
func defaultPID(pid any) string {
	switch v := pid.(type) {
	case nil:
		return NilValue
	case string:
		if v == "" {
			return NilValue
		}
		return v
	case int:
		if v == 0 {
			return NilValue
		}
		return strconv.Itoa(v)
	case int64:
		if v == 0 {
			return NilValue
		}
		return strconv.FormatInt(v, 10)
	default:
		return fmt.Sprintf("%v", v)
	}
}
