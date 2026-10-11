// Copyright 2026, Aiven, https://aiven.io/
//
// This file is under the Apache License, Version 2.0.
// See the file `LICENSE` for details.

package kafka

import (
	"context"
	"crypto/ecdsa"
	"crypto/elliptic"
	"crypto/rand"
	"crypto/tls"
	"crypto/x509"
	"crypto/x509/pkix"
	"encoding/binary"
	"encoding/pem"
	"io"
	"math/big"
	"net"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/twmb/franz-go/pkg/kgo"
)

// makeBrokerCert generates a CA and a leaf certificate for "broker.test". The
// leaf has DNS-only SANs, so verifying it against an IP ServerName must fail —
// which is what proves the dialer uses the BROKER hostname. It returns the
// trust pool, the leaf tls.Certificate, and the CA PEM bytes.
func makeBrokerCert(t *testing.T) (*x509.CertPool, tls.Certificate, []byte) {
	t.Helper()
	caKey, err := ecdsa.GenerateKey(elliptic.P256(), rand.Reader)
	if err != nil {
		t.Fatalf("ca key: %v", err)
	}
	caTmpl := &x509.Certificate{
		SerialNumber:          big.NewInt(1),
		Subject:               pkix.Name{CommonName: "test-ca"},
		NotBefore:             time.Now().Add(-time.Hour),
		NotAfter:              time.Now().Add(time.Hour),
		IsCA:                  true,
		KeyUsage:              x509.KeyUsageCertSign | x509.KeyUsageDigitalSignature,
		BasicConstraintsValid: true,
	}
	caDER, err := x509.CreateCertificate(rand.Reader, caTmpl, caTmpl, &caKey.PublicKey, caKey)
	if err != nil {
		t.Fatalf("ca cert: %v", err)
	}
	caCert, err := x509.ParseCertificate(caDER)
	if err != nil {
		t.Fatalf("parse ca: %v", err)
	}

	leafKey, err := ecdsa.GenerateKey(elliptic.P256(), rand.Reader)
	if err != nil {
		t.Fatalf("leaf key: %v", err)
	}
	leafTmpl := &x509.Certificate{
		SerialNumber: big.NewInt(2),
		Subject:      pkix.Name{CommonName: "broker.test"},
		NotBefore:    time.Now().Add(-time.Hour),
		NotAfter:     time.Now().Add(time.Hour),
		KeyUsage:     x509.KeyUsageDigitalSignature,
		ExtKeyUsage:  []x509.ExtKeyUsage{x509.ExtKeyUsageServerAuth},
		DNSNames:     []string{"broker.test"},
	}
	leafDER, err := x509.CreateCertificate(rand.Reader, leafTmpl, caCert, &leafKey.PublicKey, caKey)
	if err != nil {
		t.Fatalf("leaf cert: %v", err)
	}
	leaf := tls.Certificate{Certificate: [][]byte{leafDER}, PrivateKey: leafKey}
	pool := x509.NewCertPool()
	pool.AddCert(caCert)
	caPEM := pem.EncodeToMemory(&pem.Block{Type: "CERTIFICATE", Bytes: caDER})
	return pool, leaf, caPEM
}

// startTLSEcho starts a TLS listener that echoes one length-prefixed message.
func startTLSEcho(t *testing.T, cert tls.Certificate) net.Listener {
	t.Helper()
	ln, err := tls.Listen("tcp", "127.0.0.1:0", &tls.Config{Certificates: []tls.Certificate{cert}, MinVersion: tls.VersionTLS12})
	if err != nil {
		t.Fatalf("tls listen: %v", err)
	}
	t.Cleanup(func() { _ = ln.Close() })
	go func() {
		for {
			conn, err := ln.Accept()
			if err != nil {
				return
			}
			go func(c net.Conn) {
				defer func() { _ = c.Close() }()
				var n int32
				if err := binary.Read(c, binary.BigEndian, &n); err != nil {
					return
				}
				buf := make([]byte, n)
				if _, err := io.ReadFull(c, buf); err != nil {
					return
				}
				_, _ = c.Write(buf)
			}(conn)
		}
	}()
	return ln
}

// proxyLikeDialer returns the inner dial func that ignores the requested
// address and connects to addr, modelling a SOCKS5 proxy that already resolved
// the broker to the listener.
func proxyLikeDialer(addr string) func(context.Context, string, string) (net.Conn, error) {
	return func(ctx context.Context, network, _ string) (net.Conn, error) {
		return (&net.Dialer{}).DialContext(ctx, network, addr)
	}
}

// TestKafkaDialerWrapsTLSWithBrokerServerName is the item-1 regression: with a
// custom dialer (a SOCKS5-like inner delivering a plain conn to the broker),
// the TLS handshake must be performed INSIDE the dialer and must verify against
// the BROKER hostname, not the resolved IP or the proxy.
func TestKafkaDialerWrapsTLSWithBrokerServerName(t *testing.T) {
	pool, leaf, _ := makeBrokerCert(t)
	ln := startTLSEcho(t, leaf)
	d := &kafkaDialer{
		backoff: newReconnectBackoff(reconnectBackoffBase, reconnectBackoffMax),
		dial:    proxyLikeDialer(ln.Addr().String()),
		tlsCfg:  &tls.Config{RootCAs: pool, MinVersion: tls.VersionTLS12},
	}

	conn, err := d.DialContext(context.Background(), "tcp", "broker.test:9092")
	if err != nil {
		t.Fatalf("dial with TLS through the proxy-like dialer: %v (TLS not folded into the dialer, or wrong ServerName)", err)
	}
	defer func() { _ = conn.Close() }()

	if err := binary.Write(conn, binary.BigEndian, int32(3)); err != nil {
		t.Fatalf("write len: %v", err)
	}
	if _, err := conn.Write([]byte("abc")); err != nil {
		t.Fatalf("write: %v", err)
	}
	buf := make([]byte, 3)
	if _, err := io.ReadFull(conn, buf); err != nil {
		t.Fatalf("read echo: %v (not a TLS session?)", err)
	}
	if string(buf) != "abc" {
		t.Fatalf("echo = %q, want abc", buf)
	}
}

// TestKafkaDialerTLSVerificationTargetsBrokerHost is the negative half: the same
// cert cannot verify against an IP ServerName, so dialing the broker by IP must
// FAIL. This proves the successful case verified the hostname.
func TestKafkaDialerTLSVerificationTargetsBrokerHost(t *testing.T) {
	pool, leaf, _ := makeBrokerCert(t)
	ln := startTLSEcho(t, leaf)
	d := &kafkaDialer{
		backoff: newReconnectBackoff(reconnectBackoffBase, reconnectBackoffMax),
		dial:    proxyLikeDialer(ln.Addr().String()),
		tlsCfg:  &tls.Config{RootCAs: pool, MinVersion: tls.VersionTLS12},
	}
	if _, err := d.DialContext(context.Background(), "tcp", "127.0.0.1:9092"); err == nil {
		t.Fatal("dial with an IP ServerName: want a certificate verification error, got nil")
	}
}

// TestClientOptsSocks5WithTLSFoldsTLSIntoDialer is the configuration-level
// guard: with socks5_proxy AND SSL both set, the TLS config must live on the
// dialer (not kgo.DialTLSConfig), and the option set must construct a client.
func TestClientOptsSocks5WithTLSFoldsTLSIntoDialer(t *testing.T) {
	_, _, caPEM := makeBrokerCert(t)
	caPath := filepath.Join(t.TempDir(), "ca.pem")
	if err := os.WriteFile(caPath, caPEM, 0o600); err != nil {
		t.Fatalf("write ca: %v", err)
	}
	raw := baseConfig()
	raw["security_protocol"] = "SSL"
	raw["socks5_proxy"] = "socks5h://proxy:1080"
	raw["ca"] = caPath
	pc, err := resolveProducerConfig(raw)
	if err != nil {
		t.Fatalf("resolveProducerConfig: %v", err)
	}
	d, err := pc.newDialer()
	if err != nil {
		t.Fatalf("newDialer: %v", err)
	}
	if d.tlsCfg == nil {
		t.Fatal("newDialer did not fold the TLS config into the socks5 dialer; TLS would be bypassed")
	}
	opts, err := pc.clientOpts()
	if err != nil {
		t.Fatalf("clientOpts: %v", err)
	}
	cl, err := kgo.NewClient(append(opts, pc.producerOpts()...)...)
	if err != nil {
		t.Fatalf("kgo.NewClient rejected socks5+TLS options: %v", err)
	}
	cl.Close()
}

// TestKgoRejectsDialerAndDialTLSConfig documents WHY TLS must be folded into the
// dialer: franz-go refuses a client configured with both.
func TestKgoRejectsDialerAndDialTLSConfig(t *testing.T) {
	cl, err := kgo.NewClient(
		kgo.SeedBrokers("localhost:9092"),
		kgo.Dialer((&net.Dialer{}).DialContext),
		kgo.DialTLSConfig(&tls.Config{}),
	)
	if err == nil {
		cl.Close()
		t.Fatal("kgo.NewClient accepted both Dialer and DialTLSConfig; the fold assumption is stale")
	}
}
