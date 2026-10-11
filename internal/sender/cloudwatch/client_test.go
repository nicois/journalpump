// Copyright 2026, Aiven, https://aiven.io/
//
// This file is under the Apache License, Version 2.0.
// See the file `LICENSE` for details.

package cloudwatch

import (
	"context"
	"crypto/x509"
	"encoding/json"
	"encoding/pem"
	"errors"
	"io"
	"net"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/cloudwatchlogs"
	"github.com/aws/smithy-go"
)

// TestResolveConfigRegionProfilePrecedence pins botocore's env precedence:
// AWS_DEFAULT_REGION (not AWS_REGION) and AWS_DEFAULT_PROFILE (over
// AWS_PROFILE).
func TestResolveConfigRegionProfilePrecedence(t *testing.T) {
	cfg := map[string]any{}

	sc, err := resolveConfig(cfg, []string{
		"AWS_REGION=from-region",
		"AWS_DEFAULT_REGION=from-default-region",
		"AWS_PROFILE=from-profile",
		"AWS_DEFAULT_PROFILE=from-default-profile",
	})
	if err != nil {
		t.Fatalf("resolveConfig: %v", err)
	}
	if sc.region != "from-default-region" {
		t.Errorf("region = %q, want from-default-region (botocore ignores AWS_REGION)", sc.region)
	}
	if sc.profile != "from-default-profile" {
		t.Errorf("profile = %q, want from-default-profile", sc.profile)
	}

	// AWS_PROFILE is the fallback when AWS_DEFAULT_PROFILE is unset.
	sc, err = resolveConfig(cfg, []string{"AWS_PROFILE=fallback"})
	if err != nil {
		t.Fatalf("resolveConfig: %v", err)
	}
	if sc.profile != "fallback" {
		t.Errorf("profile = %q, want fallback", sc.profile)
	}

	// An explicit aws_region config wins over the environment.
	sc, err = resolveConfig(map[string]any{"aws_region": "explicit"}, []string{"AWS_DEFAULT_REGION=env"})
	if err != nil {
		t.Fatalf("resolveConfig: %v", err)
	}
	if sc.region != "explicit" {
		t.Errorf("region = %q, want explicit", sc.region)
	}
}

// TestResolveConfigPartialCredentials ports botocore's PartialCredentialsError:
// either half of an explicit pair alone is rejected at construction.
func TestResolveConfigPartialCredentials(t *testing.T) {
	tests := []struct {
		name string
		cfg  map[string]any
	}{
		{"access only", map[string]any{"aws_access_key_id": "a"}},
		{"secret only", map[string]any{"aws_secret_access_key": "b"}},
		{"explicit empty access only", map[string]any{"aws_access_key_id": ""}},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			if _, err := resolveConfig(tc.cfg, nil); err == nil {
				t.Fatal("partial credentials accepted, want error")
			}
		})
	}
	// Both present (including explicit empty strings) is a valid static pair.
	sc, err := resolveConfig(map[string]any{"aws_access_key_id": "", "aws_secret_access_key": ""}, nil)
	if err != nil {
		t.Fatalf("full pair rejected: %v", err)
	}
	if !sc.hasCreds {
		t.Error("hasCreds = false for an explicit full pair")
	}
	// Neither present falls back to the ambient chain.
	sc, err = resolveConfig(map[string]any{}, nil)
	if err != nil {
		t.Fatalf("resolveConfig: %v", err)
	}
	if sc.hasCreds {
		t.Error("hasCreds = true without explicit keys")
	}
}

// TestResolveConfigCABundlePrecedence pins AWS_CA_BUNDLE over
// REQUESTS_CA_BUNDLE, and the REQUESTS_CA_BUNDLE fallback.
func TestResolveConfigCABundlePrecedence(t *testing.T) {
	sc, err := resolveConfig(nil, []string{"REQUESTS_CA_BUNDLE=/requests.pem"})
	if err != nil {
		t.Fatalf("resolveConfig: %v", err)
	}
	if sc.caBundle != "/requests.pem" {
		t.Errorf("caBundle = %q, want /requests.pem", sc.caBundle)
	}
	sc, err = resolveConfig(nil, []string{"AWS_CA_BUNDLE=/aws.pem", "REQUESTS_CA_BUNDLE=/requests.pem"})
	if err != nil {
		t.Fatalf("resolveConfig: %v", err)
	}
	if sc.caBundle != "/aws.pem" {
		t.Errorf("caBundle = %q, want /aws.pem (AWS_CA_BUNDLE wins)", sc.caBundle)
	}
}

// TestResolveConfigEnvCredentials pins botocore's env-credential precedence:
// the env pair is captured when the config supplies none, the legacy pair is
// the fallback, and a partial env pair is rejected.
func TestResolveConfigEnvCredentials(t *testing.T) {
	sc, err := resolveConfig(nil, []string{
		"AWS_ACCESS_KEY_ID=envkey",
		"AWS_SECRET_ACCESS_KEY=envsecret",
		"AWS_SESSION_TOKEN=envsession",
	})
	if err != nil {
		t.Fatalf("resolveConfig: %v", err)
	}
	if !sc.hasEnvCreds || sc.envAccessKey != "envkey" || sc.envSecretKey != "envsecret" || sc.envSession != "envsession" {
		t.Errorf("env creds = %+v", sc)
	}

	sc, err = resolveConfig(nil, []string{"AWS_ACCESS_KEY=legacy", "AWS_SECRET_KEY=legacysecret"})
	if err != nil {
		t.Fatalf("resolveConfig: %v", err)
	}
	if !sc.hasEnvCreds || sc.envAccessKey != "legacy" {
		t.Errorf("legacy env creds = %+v", sc)
	}

	// A config pair suppresses env credentials entirely.
	sc, err = resolveConfig(map[string]any{"aws_access_key_id": "c", "aws_secret_access_key": "d"},
		[]string{"AWS_ACCESS_KEY_ID=envkey", "AWS_SECRET_ACCESS_KEY=envsecret"})
	if err != nil {
		t.Fatalf("resolveConfig: %v", err)
	}
	if !sc.hasCreds || sc.hasEnvCreds {
		t.Errorf("config pair did not suppress env creds: %+v", sc)
	}

	if _, err := resolveConfig(nil, []string{"AWS_ACCESS_KEY_ID=envkey"}); err == nil {
		t.Error("partial env credentials accepted, want error")
	}
}

// TestSDKRetryResolution uses the real SDK loader to pin the retry behavior:
// the standard-mode default is 3 attempts, AWS_MAX_ATTEMPTS overrides it, and
// AWS_RETRY_MODE=legacy is rejected by the SDK (a documented deviation —
// botocore accepts it).
func TestSDKRetryResolution(t *testing.T) {
	base := sdkConfig{region: "us-east-1", accessKey: "k", secretKey: "s", hasCreds: true}

	// attempts reads the retryer the REAL CloudWatch client resolves (the
	// SDK applies the mode's default attempt count when none is set).
	attempts := func(t *testing.T) int {
		t.Helper()
		loaded, err := loadAWSConfig(context.Background(), base)
		if err != nil {
			t.Fatalf("loadAWSConfig: %v", err)
		}
		return cloudwatchlogs.NewFromConfig(loaded).Options().Retryer.MaxAttempts()
	}

	t.Setenv("AWS_MAX_ATTEMPTS", "")
	t.Setenv("AWS_RETRY_MODE", "")
	if got := attempts(t); got != 3 {
		t.Errorf("default max attempts = %d, want 3 (standard/adaptive default)", got)
	}

	t.Setenv("AWS_MAX_ATTEMPTS", "9")
	if got := attempts(t); got != 9 {
		t.Errorf("AWS_MAX_ATTEMPTS max attempts = %d, want 9", got)
	}
	t.Setenv("AWS_MAX_ATTEMPTS", "")

	t.Setenv("AWS_RETRY_MODE", "adaptive")
	loaded, err := loadAWSConfig(context.Background(), base)
	if err != nil {
		t.Fatalf("loadAWSConfig adaptive: %v", err)
	}
	if loaded.RetryMode != aws.RetryModeAdaptive {
		t.Errorf("adaptive mode = %q, want adaptive", loaded.RetryMode)
	}
	if got := attempts(t); got != 3 {
		t.Errorf("adaptive default max attempts = %d, want 3", got)
	}

	// The SDK rejects botocore's default mode value; this is the deviation the
	// parity doc records (botocore treats legacy as its default).
	t.Setenv("AWS_RETRY_MODE", "legacy")
	if _, err := loadAWSConfig(context.Background(), base); err == nil {
		t.Error("AWS_RETRY_MODE=legacy accepted, want SDK rejection")
	}
}

// TestBotocoreProxies pins urllib.getproxies_environment: lowercase wins over
// uppercase, a present-but-empty lowercase REMOVES the entry, no ALL_PROXY.
func TestBotocoreProxies(t *testing.T) {
	tests := []struct {
		name    string
		environ []string
		key     string
		want    string
	}{
		{"uppercase used when no lowercase", []string{"HTTP_PROXY=http://upper"}, "http", "http://upper"},
		{"lowercase wins", []string{"HTTP_PROXY=http://upper", "http_proxy=http://lower"}, "http", "http://lower"},
		{"empty lowercase removes uppercase", []string{"HTTP_PROXY=http://upper", "http_proxy="}, "http", ""},
		{"no_proxy", []string{"no_proxy=example.com"}, "no", "example.com"},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			got := botocoreProxies(tc.environ)[tc.key]
			if got != tc.want {
				t.Errorf("proxies[%q] = %q, want %q", tc.key, got, tc.want)
			}
		})
	}
}

// TestProxySelectorRouting exercises the selector through a real proxy server,
// including a proxied LOOPBACK target (botocore does not bypass loopback) and
// a conflicting-case pair.
func TestProxySelectorRouting(t *testing.T) {
	var mu sync.Mutex
	var proxied []string
	proxy := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		mu.Lock()
		proxied = append(proxied, r.Host)
		mu.Unlock()
		_, _ = io.WriteString(w, "ok")
	}))
	defer proxy.Close()

	target := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		_, _ = io.WriteString(w, "direct")
	}))
	defer target.Close()

	do := func(environ []string) string {
		mu.Lock()
		proxied = nil
		mu.Unlock()
		tr := &http.Transport{Proxy: botocoreProxyFunc(environ)}
		client := &http.Client{Transport: tr}
		resp, err := client.Get(target.URL)
		if err != nil {
			t.Fatalf("GET: %v", err)
		}
		defer func() { _ = resp.Body.Close() }()
		body, _ := io.ReadAll(resp.Body)
		mu.Lock()
		n := len(proxied)
		mu.Unlock()
		if n > 0 {
			return "proxied:" + string(body)
		}
		return "direct:" + string(body)
	}

	// Loopback target IS proxied when http_proxy is set (no special-case).
	if got := do([]string{"http_proxy=" + proxy.URL}); got != "proxied:ok" {
		t.Errorf("loopback routing = %q, want proxied", got)
	}
	// Conflicting case: lowercase wins, so the good proxy is used.
	if got := do([]string{"HTTP_PROXY=http://127.0.0.1:1", "http_proxy=" + proxy.URL}); got != "proxied:ok" {
		t.Errorf("conflicting-case routing = %q, want proxied via the lowercase proxy", got)
	}
	// no_proxy bypasses the loopback target.
	if got := do([]string{"http_proxy=" + proxy.URL, "no_proxy=127.0.0.1"}); got != "direct:direct" {
		t.Errorf("no_proxy routing = %q, want direct", got)
	}
	// no_proxy "*" bypasses everything.
	if got := do([]string{"http_proxy=" + proxy.URL, "no_proxy=*"}); got != "direct:direct" {
		t.Errorf("no_proxy=* routing = %q, want direct", got)
	}
	// The proxy is selected for the https scheme key too.
	req, _ := http.NewRequest(http.MethodGet, "https://logs.us-east-1.amazonaws.com", nil)
	u, err := botocoreProxyFunc([]string{"https_proxy=" + proxy.URL})(req)
	if err != nil || u == nil {
		t.Fatalf("https selection = %v, err %v; want the proxy", u, err)
	}
	// ALL_PROXY is recorded under its own key but is NOT a fallback: an https
	// request with no https_proxy is NOT proxied.
	if u, _ := botocoreProxyFunc([]string{"ALL_PROXY=" + proxy.URL})(req); u != nil {
		t.Errorf("ALL_PROXY used as a fallback: %v", u)
	}
	// A scheme-relative value is completed with http, not left malformed.
	if u, err := botocoreProxyFunc([]string{"https_proxy=//proxy.example:8080"})(req); err != nil || u == nil || u.String() != "http://proxy.example:8080" {
		t.Errorf("scheme-relative proxy = %v err %v, want http://proxy.example:8080", u, err)
	}
}

// TestClientCustomCATrustSet drives the FULL production construction path: the
// AWS_CA_BUNDLE (and REQUESTS_CA_BUNDLE) bundle must reach the real SDK client
// through the BuildableClient, completing a verified TLS handshake. Without the
// bundle the handshake must fail.
func TestClientCustomCATrustSet(t *testing.T) {
	server := httptest.NewTLSServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		_, _ = io.WriteString(w, "{}")
	}))
	defer server.Close()

	caPath := writeCertPEM(t, server.Certificate())

	base := func() sdkConfig {
		return sdkConfig{region: "us-east-1", accessKey: "k", secretKey: "s", hasCreds: true, baseEndpoint: server.URL}
	}

	for _, env := range []string{"AWS_CA_BUNDLE", "REQUESTS_CA_BUNDLE"} {
		t.Run(env, func(t *testing.T) {
			sc, err := resolveConfig(nil, []string{env + "=" + caPath})
			if err != nil {
				t.Fatalf("resolveConfig: %v", err)
			}
			merged := base()
			merged.caBundle = sc.caBundle
			c, err := buildSDKClient(context.Background(), merged)
			if err != nil {
				t.Fatalf("buildSDKClient: %v", err)
			}
			if err := c.CreateLogGroup(context.Background(), "group"); err != nil {
				t.Fatalf("CreateLogGroup with %s trusts: %v", env, err)
			}
		})
	}

	// Without the CA the handshake must fail (proves the CA is actually used).
	c, err := buildSDKClient(context.Background(), base())
	if err != nil {
		t.Fatalf("buildSDKClient: %v", err)
	}
	if err := c.CreateLogGroup(context.Background(), "group"); err == nil {
		t.Error("handshake succeeded without the custom CA")
	}
}

// writeCertPEM writes an httptest certificate to a temp PEM file and returns
// its path.
func writeCertPEM(t *testing.T, cert *x509.Certificate) string {
	t.Helper()
	path := filepath.Join(t.TempDir(), "ca.pem")
	pemBytes := pem.EncodeToMemory(&pem.Block{Type: "CERTIFICATE", Bytes: cert.Raw})
	if err := os.WriteFile(path, pemBytes, 0o600); err != nil {
		t.Fatalf("write cert: %v", err)
	}
	return path
}

// captureServer records the requests the real SDK client sends.
type captureServer struct {
	mu       sync.Mutex
	requests []capturedRequest
	handler  func(call int, body string) (int, string)
}

type capturedRequest struct {
	Target string
	Body   string
}

func (s *captureServer) serve() *httptest.Server {
	return httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		body, _ := io.ReadAll(r.Body)
		s.mu.Lock()
		s.requests = append(s.requests, capturedRequest{Target: r.Header.Get("X-Amz-Target"), Body: string(body)})
		call := len(s.requests)
		s.mu.Unlock()
		status, resp := 200, "{}"
		if s.handler != nil {
			status, resp = s.handler(call, string(body))
		}
		w.Header().Set("Content-Type", "application/x-amz-json-1.1")
		w.WriteHeader(status)
		_, _ = io.WriteString(w, resp)
	}))
}

// testSDKClient builds a real SDK client pointed at a capture server.
func testSDKClient(t *testing.T, url string) logsClient {
	t.Helper()
	sc := sdkConfig{
		region:       "us-east-1",
		accessKey:    "key",
		secretKey:    "secret",
		hasCreds:     true,
		baseEndpoint: url,
	}
	c, err := buildSDKClient(context.Background(), sc)
	if err != nil {
		t.Fatalf("buildSDKClient: %v", err)
	}
	return c
}

// TestSDKClientWireShape drives the REAL SDK client against a capture server:
// the request target/body, the first-put token OMISSION, then the token, plus
// multi-page describe_log_streams.
func TestSDKClientWireShape(t *testing.T) {
	srv := &captureServer{}
	server := srv.serve()
	defer server.Close()
	c := testSDKClient(t, server.URL)

	// CreateLogGroup wire shape.
	if err := c.CreateLogGroup(context.Background(), "group"); err != nil {
		t.Fatalf("CreateLogGroup: %v", err)
	}
	// First put has NO sequence token.
	next, status, err := c.PutLogEvents(context.Background(), "group", "stream", "", []logEvent{{Timestamp: 123, Message: "hi"}})
	if err != nil {
		t.Fatalf("PutLogEvents: %v", err)
	}
	if status != 200 || next != "" {
		t.Errorf("put result = (%q,%d)", next, status)
	}
	// Second put carries the token.
	if _, _, err := c.PutLogEvents(context.Background(), "group", "stream", "tok1", []logEvent{{Timestamp: 124, Message: "yo"}}); err != nil {
		t.Fatalf("PutLogEvents: %v", err)
	}

	srv.mu.Lock()
	reqs := append([]capturedRequest(nil), srv.requests...)
	srv.mu.Unlock()
	if len(reqs) != 3 {
		t.Fatalf("requests = %d, want 3", len(reqs))
	}
	if !strings.HasSuffix(reqs[0].Target, "CreateLogGroup") {
		t.Errorf("first target = %q, want ...CreateLogGroup", reqs[0].Target)
	}
	var create struct {
		LogGroupName string `json:"logGroupName"`
	}
	if err := json.Unmarshal([]byte(reqs[0].Body), &create); err != nil || create.LogGroupName != "group" {
		t.Errorf("create body = %q, err %v", reqs[0].Body, err)
	}
	if strings.Contains(reqs[1].Body, "sequenceToken") {
		t.Errorf("first put body unexpectedly carries a token: %q", reqs[1].Body)
	}
	var firstPut struct {
		LogGroupName  string `json:"logGroupName"`
		LogStreamName string `json:"logStreamName"`
		LogEvents     []struct {
			Timestamp int64  `json:"timestamp"`
			Message   string `json:"message"`
		} `json:"logEvents"`
		SequenceToken *string `json:"sequenceToken"`
	}
	if err := json.Unmarshal([]byte(reqs[1].Body), &firstPut); err != nil {
		t.Fatalf("first put body: %v", err)
	}
	if firstPut.LogGroupName != "group" || firstPut.LogStreamName != "stream" {
		t.Errorf("first put names = (%q,%q), want (group,stream)", firstPut.LogGroupName, firstPut.LogStreamName)
	}
	if len(firstPut.LogEvents) != 1 || firstPut.LogEvents[0].Timestamp != 123 || firstPut.LogEvents[0].Message != "hi" {
		t.Errorf("first put events = %+v", firstPut.LogEvents)
	}
	if firstPut.SequenceToken != nil {
		t.Errorf("first put token = %v, want omitted", *firstPut.SequenceToken)
	}

	var secondPut struct {
		SequenceToken *string `json:"sequenceToken"`
		LogEvents     []struct {
			Timestamp int64  `json:"timestamp"`
			Message   string `json:"message"`
		} `json:"logEvents"`
	}
	if err := json.Unmarshal([]byte(reqs[2].Body), &secondPut); err != nil {
		t.Fatalf("second put body: %v", err)
	}
	if secondPut.SequenceToken == nil || *secondPut.SequenceToken != "tok1" {
		t.Errorf("second put token = %v, want tok1", secondPut.SequenceToken)
	}
	if len(secondPut.LogEvents) != 1 || secondPut.LogEvents[0].Timestamp != 124 || secondPut.LogEvents[0].Message != "yo" {
		t.Errorf("second put events = %+v", secondPut.LogEvents)
	}
}

// TestSDKClientDescribePagination drives the REAL paginator across two pages.
func TestSDKClientDescribePagination(t *testing.T) {
	srv := &captureServer{handler: func(call int, _ string) (int, string) {
		if call == 1 {
			return 200, `{"logStreams":[{"logStreamName":"other"}],"nextToken":"page2"}`
		}
		return 200, `{"logStreams":[{"logStreamName":"stream","uploadSequenceToken":"recovered"}]}`
	}}
	server := srv.serve()
	defer server.Close()
	c := testSDKClient(t, server.URL)

	token, found, err := c.UploadSequenceToken(context.Background(), "group", "stream")
	if err != nil {
		t.Fatalf("UploadSequenceToken: %v", err)
	}
	if !found || token != "recovered" {
		t.Fatalf("found=%v token=%q, want found/recovered", found, token)
	}
	srv.mu.Lock()
	reqs := append([]capturedRequest(nil), srv.requests...)
	srv.mu.Unlock()
	if len(reqs) != 2 {
		t.Fatalf("describe pages = %d, want 2", len(reqs))
	}
	// The second page must carry the token the first response returned.
	var second map[string]any
	if err := json.Unmarshal([]byte(reqs[1].Body), &second); err != nil {
		t.Fatalf("second describe body: %v", err)
	}
	if second["nextToken"] != "page2" {
		t.Errorf("second describe nextToken = %v, want page2", second["nextToken"])
	}
	if second["logGroupName"] != "group" {
		t.Errorf("second describe logGroupName = %v, want group", second["logGroupName"])
	}
}

// TestSDKClientCancellation drives the REAL client with a cancelled context and
// asserts it returns promptly without a network round trip.
func TestSDKClientCancellation(t *testing.T) {
	srv := &captureServer{}
	server := srv.serve()
	defer server.Close()
	c := testSDKClient(t, server.URL)

	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	done := make(chan error, 1)
	go func() { done <- c.CreateLogGroup(ctx, "group") }()
	select {
	case err := <-done:
		if err == nil {
			t.Fatal("cancelled context returned nil")
		}
	case <-time.After(2 * time.Second):
		t.Fatal("cancelled request did not return promptly")
	}
}

// fakeErrorAPI is a smithy.APIError for wrapAPIError tests.
type fakeErrorAPI struct{ code, msg string }

func (e fakeErrorAPI) Error() string                 { return e.code }
func (e fakeErrorAPI) ErrorCode() string             { return e.code }
func (e fakeErrorAPI) ErrorMessage() string          { return e.msg }
func (e fakeErrorAPI) ErrorFault() smithy.ErrorFault { return smithy.FaultUnknown }

// TestHTTPClientReadTimeoutBoundsStalledResponse proves the transport applies
// an inactivity bound: a server that accepts the connection but never writes a
// response must not hold the request open forever.
func TestHTTPClientReadTimeoutBoundsStalledResponse(t *testing.T) {
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("listen: %v", err)
	}
	defer func() { _ = ln.Close() }()
	accepted := make(chan net.Conn, 1)
	go func() {
		for {
			conn, err := ln.Accept()
			if err != nil {
				return
			}
			select {
			case accepted <- conn:
			default:
				_ = conn.Close()
			}
		}
	}()

	sc := sdkConfig{proxy: botocoreProxyFunc(nil), readTimeout: 200 * time.Millisecond}
	hc, err := sc.newHTTPClient()
	if err != nil {
		t.Fatalf("newHTTPClient: %v", err)
	}
	req, _ := http.NewRequest(http.MethodGet, "http://"+ln.Addr().String()+"/", nil)
	start := time.Now()
	resp, err := hc.Do(req)
	if err == nil {
		_ = resp.Body.Close()
		t.Fatal("stalled response did not time out")
	}
	if d := time.Since(start); d > 3*time.Second {
		t.Errorf("stalled response returned after %v, want a bounded timeout", d)
	}
	select {
	case conn := <-accepted:
		_ = conn.Close()
	default:
	}
}

// TestSDKCredentialPrecedence pins botocore's precedence through the real
// client: an environment credential pair wins over the selected profile.
func TestSDKCredentialPrecedence(t *testing.T) {
	dir := t.TempDir()
	cfgPath := filepath.Join(dir, "config")
	credPath := filepath.Join(dir, "credentials")
	if err := os.WriteFile(cfgPath, []byte("[profile probe]\nregion = us-east-2\n"), 0o600); err != nil {
		t.Fatalf("write config: %v", err)
	}
	if err := os.WriteFile(credPath, []byte("[probe]\naws_access_key_id = PROFKEY\naws_secret_access_key = PROFSECRET\n"), 0o600); err != nil {
		t.Fatalf("write credentials: %v", err)
	}
	t.Setenv("AWS_CONFIG_FILE", cfgPath)
	t.Setenv("AWS_SHARED_CREDENTIALS_FILE", credPath)

	// Env pair present: it must win over the selected profile.
	sc := sdkConfig{
		region:       "us-east-1",
		profile:      "probe",
		envAccessKey: "ENVKEY",
		envSecretKey: "ENVSECRET",
		hasEnvCreds:  true,
	}
	loaded, err := loadAWSConfig(context.Background(), sc)
	if err != nil {
		t.Fatalf("loadAWSConfig: %v", err)
	}
	creds, err := loaded.Credentials.Retrieve(context.Background())
	if err != nil {
		t.Fatalf("retrieve: %v", err)
	}
	if creds.AccessKeyID != "ENVKEY" {
		t.Errorf("access key = %q, want ENVKEY (env wins over profile)", creds.AccessKeyID)
	}

	// Without env credentials the selected profile supplies the identity.
	sc.hasEnvCreds, sc.envAccessKey, sc.envSecretKey = false, "", ""
	loaded, err = loadAWSConfig(context.Background(), sc)
	if err != nil {
		t.Fatalf("loadAWSConfig: %v", err)
	}
	creds, err = loaded.Credentials.Retrieve(context.Background())
	if err != nil {
		t.Fatalf("retrieve: %v", err)
	}
	if creds.AccessKeyID != "PROFKEY" {
		t.Errorf("access key = %q, want PROFKEY (profile fallback)", creds.AccessKeyID)
	}
}

// TestBuildSDKClientBoundedConstruction is a construction-path regression:
// an already-cancelled context must not leave construction blocked (the
// documented residual is the container-credential hostname lookup, which does
// not observe ctx).
func TestBuildSDKClientBoundedConstruction(t *testing.T) {
	sc := sdkConfig{region: "us-east-1", accessKey: "k", secretKey: "s", hasCreds: true}
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	done := make(chan struct{})
	go func() {
		defer close(done)
		_, _ = buildSDKClient(ctx, sc)
	}()
	select {
	case <-done:
	case <-time.After(5 * time.Second):
		t.Fatal("construction did not return for a cancelled context")
	}
}

// TestWrapAPIError checks the smithy API error mapping.
func TestWrapAPIError(t *testing.T) {
	wrapped := wrapAPIError(fakeErrorAPI{code: "AccessDeniedException", msg: "no"})
	var api *apiError
	if !errors.As(wrapped, &api) {
		t.Fatalf("wrapAPIError = %v, want *apiError", wrapped)
	}
	if api.Code != "AccessDeniedException" || api.Message != "no" {
		t.Errorf("apiError = %+v", api)
	}
	if !errors.Is(wrapAPIError(nil), nil) {
		t.Error("nil error not preserved")
	}
}
