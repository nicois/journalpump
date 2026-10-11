// Copyright 2026, Aiven, https://aiven.io/
//
// This file is under the Apache License, Version 2.0.
// See the file `LICENSE` for details.

package httpx

import (
	"context"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"
)

func TestDoReturnsStatusAndBody(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if got := r.Header.Get("x-test"); got != "1" {
			t.Errorf("header x-test = %q", got)
		}
		w.WriteHeader(http.StatusCreated)
		_, _ = w.Write([]byte("hello"))
	}))
	defer srv.Close()

	status, body, err := Do(context.Background(), srv.Client(), http.MethodPost, srv.URL, []byte("payload"), map[string]string{"x-test": "1"}, time.Second)
	if err != nil {
		t.Fatal(err)
	}
	if status != http.StatusCreated {
		t.Errorf("status = %d", status)
	}
	if string(body) != "hello" {
		t.Errorf("body = %q", body)
	}
}

func TestDoAppliesPerRequestDeadline(t *testing.T) {
	release := make(chan struct{})
	srv := httptest.NewServer(http.HandlerFunc(func(_ http.ResponseWriter, _ *http.Request) {
		<-release
	}))
	defer func() { close(release); srv.Close() }()

	start := time.Now()
	_, _, err := Do(context.Background(), srv.Client(), http.MethodGet, srv.URL, nil, nil, 100*time.Millisecond)
	elapsed := time.Since(start)
	if err == nil {
		t.Fatal("expected a deadline error")
	}
	if elapsed > 2*time.Second {
		t.Errorf("deadline not applied, took %s", elapsed)
	}
}

func TestRequestsProxyUsesEnvironment(t *testing.T) {
	t.Setenv("http_proxy", "http://proxy.example:8080")
	t.Setenv("no_proxy", "")
	t.Setenv("NO_PROXY", "")

	req, _ := http.NewRequest(http.MethodGet, "http://target.example/", nil)
	u, err := RequestsProxy(req)
	if err != nil {
		t.Fatal(err)
	}
	if u == nil || u.String() != "http://proxy.example:8080" {
		t.Fatalf("proxy = %v, want http://proxy.example:8080", u)
	}

	// A matching no_proxy bypasses the proxy (requests layer 1, plain suffix).
	t.Setenv("no_proxy", "target.example")
	if u, err := RequestsProxy(req); err != nil || u != nil {
		t.Fatalf("no_proxy target not bypassed: %v, %v", u, err)
	}
}

func TestRequestsProxyFallsBackToAllProxy(t *testing.T) {
	t.Setenv("http_proxy", "")
	t.Setenv("no_proxy", "")
	t.Setenv("NO_PROXY", "")
	t.Setenv("ALL_PROXY", "http://all.example:3128")

	req, _ := http.NewRequest(http.MethodGet, "http://target.example/", nil)
	u, err := RequestsProxy(req)
	if err != nil {
		t.Fatal(err)
	}
	if u == nil || u.String() != "http://all.example:3128" {
		t.Fatalf("ALL_PROXY fallback = %v", u)
	}
}

func TestNewClientNilProxyUsesNoProxy(t *testing.T) {
	client := NewClient(ClientOptions{})
	tr, ok := client.Transport.(*http.Transport)
	if !ok {
		t.Fatalf("transport = %T", client.Transport)
	}
	if tr.Proxy != nil {
		t.Errorf("nil ProxySelector should leave transport.Proxy nil")
	}
}

func TestNewClientInstallsProxySelector(t *testing.T) {
	client := NewClient(ClientOptions{Proxy: RequestsProxy})
	tr := client.Transport.(*http.Transport)
	if tr.Proxy == nil {
		t.Fatal("transport.Proxy not set")
	}
}
