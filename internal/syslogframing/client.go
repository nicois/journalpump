// Copyright 2026, Aiven, https://aiven.io/
//
// This file is under the Apache License, Version 2.0.
// See the file `LICENSE` for details.

package syslogframing

import (
	"context"
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

// writeDeadliner is the optional deadline seam used to unblock a stalled
// write when the caller's context is canceled. net.Conn and *tls.Conn
// implement it; a plain stub may not, in which case cancellation can only
// be observed between writes.
type writeDeadliner interface {
	SetWriteDeadline(t time.Time) error
}

// Config carries the SyslogTcpClient constructor arguments
// (rsyslog.py:100-141).
type Config struct {
	Server              string
	Port                int
	RFC                 string // "RFC5424" | "RFC3164" | "CUSTOM"
	MaxMsg              int    // 0 means DefaultMaxMsg; negatives preserved
	Protocol            string // "" == "PLAINTEXT"
	CACerts             string
	KeyFile             string
	CertFile            string
	LogFormat           string
	OctetCountedFraming bool
	EscapeNewlines      bool

	// Dial is a test/transport seam: when set it replaces the network
	// dial, returning an already-connected Conn (Python's tests patch
	// _connect and assign .socket). It receives the caller's context so a
	// custom dial can honor cancellation; TLS wrapping (when TLSConfig is
	// set) still applies to the returned Conn.
	Dial func(ctx context.Context) (Conn, error)
	// TLSConfig, when non-nil, wraps the dialed connection and its
	// handshake is completed during construction (Python's
	// SSLSocket.connect completes the handshake inside _connect). SNI
	// uses Server when TLSConfig.ServerName is empty.
	TLSConfig *tls.Config
	// Now is the clock seam for socket deadlines synctest cannot drive
	// (plan Task 19); defaults to time.Now.
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

// NewClient builds the formatter, TLS context and initial (fully
// handshaked) connection, mirroring SyslogTcpClient.__init__. The context
// bounds the dial and the TLS handshake.
func NewClient(ctx context.Context, cfg Config) (*Client, error) {
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
	if cfg.TLSConfig != nil && cfg.TLSConfig.ServerName == "" {
		cfg.TLSConfig = cfg.TLSConfig.Clone()
		cfg.TLSConfig.ServerName = cfg.Server
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
	if err := c.connect(ctx); err != nil {
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

// connect ports _connect (rsyslog.py:143-165), extended to complete the
// TLS handshake inside the connection step (Python's
// ssl.wrap_socket + socket.connect does so) and to honor ctx.
func (c *Client) connect(ctx context.Context) error {
	var raw Conn
	var err error
	if c.cfg.Dial != nil {
		raw, err = c.cfg.Dial(ctx)
	} else {
		addr := net.JoinHostPort(c.cfg.Server, strconv.Itoa(c.cfg.Port))
		var d net.Dialer
		var netConn net.Conn
		netConn, err = d.DialContext(ctx, "tcp", addr)
		if err != nil {
			var dnsErr *net.DNSError
			if errors.As(err, &dnsErr) {
				return fmt.Errorf("Invalid address %s:%d", c.cfg.Server, c.cfg.Port) //nolint:staticcheck // error-text parity with rsyslog.py:161
			}
			return err
		}
		raw = netConn
	}
	if err != nil {
		return err
	}
	if c.cfg.TLSConfig != nil {
		netConn, ok := raw.(net.Conn)
		if !ok {
			_ = raw.Close()
			return fmt.Errorf("TLS requires a net.Conn, got %T", raw)
		}
		tlsConn := tls.Client(netConn, c.cfg.TLSConfig)
		if err := tlsConn.HandshakeContext(ctx); err != nil {
			_ = tlsConn.Close()
			return err
		}
		c.conn = tlsConn
		return nil
	}
	c.conn = raw
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
// and write it, retrying exactly once when the failure is retriable. A
// canceled ctx aborts before the write and unblocks a stalled write via a
// write deadline.
func (c *Client) Send(ctx context.Context, message []byte) error {
	for retry := true; ; retry = false {
		if err := ctx.Err(); err != nil {
			c.close()
			return err
		}
		if c.conn == nil {
			if err := c.connect(ctx); err != nil {
				c.close()
				if !retry || !shouldRetry(err) {
					return err
				}
				continue
			}
		}
		frame := c.framing.Frame(message)
		deadlineForced, err := writeWithContext(ctx, c.conn, frame, c.now)
		if err == nil {
			if deadlineForced {
				// The cancel callback ran concurrently with (or just after)
				// a COMPLETED write, so it forced an expired write deadline
				// onto the retained connection. Retire it rather than reuse
				// a poisoned socket: the next Send reconnects cleanly. The
				// message itself was fully written, so the send succeeds.
				c.close()
			}
			return nil
		}
		c.close()
		if ctxErr := ctx.Err(); ctxErr != nil {
			return ctxErr
		}
		if !retry || !shouldRetry(err) {
			return err
		}
	}
}

// writeWithContext gives Conn sendall semantics and unblocks a stalled
// write when ctx is canceled by forcing the write deadline into the past.
//
// The returned deadlineForced is true when the cancel callback actually
// ran. stop() alone does NOT wait for an in-flight callback, so a
// cancellation racing a successful write could otherwise return success
// while the callback later forces an expired deadline on the retained
// connection; waiting on the done channel makes the outcome deterministic
// and lets Send retire the connection.
func writeWithContext(ctx context.Context, conn Conn, p []byte, now func() time.Time) (deadlineForced bool, err error) {
	done := make(chan struct{})
	stop := context.AfterFunc(ctx, func() {
		if d, ok := conn.(writeDeadliner); ok {
			_ = d.SetWriteDeadline(now())
		}
		close(done)
	})
	defer func() {
		if stop() {
			// Callback was successfully prevented; it will never run.
			return
		}
		// Callback already started (or is starting): wait for it to finish
		// so no deadline write can race the caller after we return.
		<-done
		deadlineForced = true
	}()
	for len(p) > 0 {
		if cerr := ctx.Err(); cerr != nil {
			return false, cerr
		}
		n, werr := conn.Write(p)
		if werr != nil {
			return false, werr
		}
		if n == 0 {
			return false, io.ErrShortWrite
		}
		p = p[n:]
	}
	return false, nil
}

// shouldRetry ports _should_retry (rsyslog.py:211-216) EXACTLY: the
// isinstance(ex, OSError) branch returns first, so only EPIPE /
// ECONNRESET / ETIMEDOUT are retried. An ssl.SSLEOFError is an OSError
// subclass and therefore takes that branch — its errno is not one of the
// three, so Python does NOT retry it and neither does this port.
func shouldRetry(err error) bool {
	return errors.Is(err, syscall.EPIPE) || errors.Is(err, syscall.ECONNRESET) || errors.Is(err, syscall.ETIMEDOUT)
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
func (c *Client) Log(ctx context.Context, in LogInput) error {
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
	return c.Send(ctx, body)
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
