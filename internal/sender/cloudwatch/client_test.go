// Copyright 2026, Aiven, https://aiven.io/
//
// This file is under the Apache License, Version 2.0.
// See the file `LICENSE` for details.

package cloudwatch

import (
	"context"
	"crypto/ecdsa"
	"crypto/elliptic"
	"crypto/rand"
	"crypto/tls"
	"crypto/x509"
	"crypto/x509/pkix"
	"encoding/json"
	"encoding/pem"
	"errors"
	"io"
	"math/big"
	"net"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	awshttp "github.com/aws/aws-sdk-go-v2/aws/transport/http"
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

// TestResolveConfigCABundlePrecedence pins botocore's order: AWS_CA_BUNDLE,
// then the selected profile's ca_bundle, then REQUESTS_CA_BUNDLE.
func TestResolveConfigCABundlePrecedence(t *testing.T) {
	cfgPath := filepath.Join(t.TempDir(), "config")
	if err := os.WriteFile(cfgPath, []byte("[default]\nca_bundle = /profile.pem\n"), 0o600); err != nil {
		t.Fatalf("write config: %v", err)
	}
	cfgEnv := "AWS_CONFIG_FILE=" + cfgPath

	// REQUESTS_CA_BUNDLE is the last fallback.
	sc, err := resolveConfig(nil, []string{cfgEnv, "REQUESTS_CA_BUNDLE=/requests.pem"})
	if err != nil {
		t.Fatalf("resolveConfig: %v", err)
	}
	if sc.caBundle != "/profile.pem" {
		t.Errorf("caBundle = %q, want /profile.pem (profile beats REQUESTS_CA_BUNDLE)", sc.caBundle)
	}

	// Without a profile bundle, REQUESTS_CA_BUNDLE applies.
	if err := os.WriteFile(cfgPath, []byte("[default]\nregion = us-east-1\n"), 0o600); err != nil {
		t.Fatalf("write config: %v", err)
	}
	sc, err = resolveConfig(nil, []string{cfgEnv, "REQUESTS_CA_BUNDLE=/requests.pem"})
	if err != nil {
		t.Fatalf("resolveConfig: %v", err)
	}
	if sc.caBundle != "/requests.pem" {
		t.Errorf("caBundle = %q, want /requests.pem", sc.caBundle)
	}

	// AWS_CA_BUNDLE wins over the profile bundle.
	if err := os.WriteFile(cfgPath, []byte("[default]\nca_bundle = /profile.pem\n"), 0o600); err != nil {
		t.Fatalf("write config: %v", err)
	}
	sc, err = resolveConfig(nil, []string{cfgEnv, "AWS_CA_BUNDLE=/aws.pem", "REQUESTS_CA_BUNDLE=/requests.pem"})
	if err != nil {
		t.Fatalf("resolveConfig: %v", err)
	}
	if sc.caBundle != "/aws.pem" {
		t.Errorf("caBundle = %q, want /aws.pem (AWS_CA_BUNDLE wins)", sc.caBundle)
	}

	// A non-default profile reads [profile NAME].
	if err := os.WriteFile(cfgPath, []byte("[profile probe]\nca_bundle = /probe.pem\n"), 0o600); err != nil {
		t.Fatalf("write config: %v", err)
	}
	sc, err = resolveConfig(nil, []string{cfgEnv, "AWS_DEFAULT_PROFILE=probe", "REQUESTS_CA_BUNDLE=/requests.pem"})
	if err != nil {
		t.Fatalf("resolveConfig: %v", err)
	}
	if sc.caBundle != "/probe.pem" {
		t.Errorf("caBundle = %q, want /probe.pem", sc.caBundle)
	}

	// The default profile may also be written as [profile default]; the
	// loader (not a hand-rolled section scanner) must honour it.
	if err := os.WriteFile(cfgPath, []byte("[profile default]\nca_bundle = /profiledefault.pem\n"), 0o600); err != nil {
		t.Fatalf("write config: %v", err)
	}
	sc, err = resolveConfig(nil, []string{cfgEnv, "REQUESTS_CA_BUNDLE=/requests.pem"})
	if err != nil {
		t.Fatalf("resolveConfig: %v", err)
	}
	if sc.caBundle != "/profiledefault.pem" {
		t.Errorf("caBundle = %q, want /profiledefault.pem ([profile default] ca_bundle)", sc.caBundle)
	}
}

// TestSDKCredentialNativeChain verifies the SDK's NATIVE credential chain
// resolves the operator-visible sources after the round-5 simplification (the
// hand-injected provider and forced default profile are gone): the standard
// environment pair, web identity, container credentials, and IMDS. The
// journalpump config pair is injected separately (the native chain cannot see
// journalpump config keys).
func TestSDKCredentialNativeChain(t *testing.T) {
	reset := func(t *testing.T) {
		t.Helper()
		for _, k := range []string{
			"AWS_ACCESS_KEY_ID", "AWS_SECRET_ACCESS_KEY", "AWS_SESSION_TOKEN",
			"AWS_SECURITY_TOKEN", "AWS_ACCESS_KEY", "AWS_SECRET_KEY",
			"AWS_WEB_IDENTITY_TOKEN_FILE", "AWS_ROLE_ARN", "AWS_ROLE_SESSION_NAME",
			"AWS_CONTAINER_CREDENTIALS_FULL_URI", "AWS_CONTAINER_CREDENTIALS_RELATIVE_URI",
			"AWS_CONTAINER_AUTHORIZATION_TOKEN", "AWS_EC2_METADATA_SERVICE_ENDPOINT",
			"AWS_EC2_METADATA_DISABLED", "AWS_DEFAULT_PROFILE", "AWS_PROFILE",
			"AWS_ENDPOINT_URL", "AWS_ENDPOINT_URL_STS", "AWS_DEFAULT_REGION", "AWS_REGION",
		} {
			t.Setenv(k, "")
		}
		// An empty default profile, so each case's own source is what resolves.
		t.Setenv("AWS_CONFIG_FILE", writeConfig(t, "[default]\nregion = us-east-1\n"))
		t.Setenv("AWS_SHARED_CREDENTIALS_FILE", writeConfig(t, ""))
	}

	retrieve := func(t *testing.T) aws.Credentials {
		t.Helper()
		sc, err := resolveConfig(nil, os.Environ())
		if err != nil {
			t.Fatalf("resolveConfig: %v", err)
		}
		loaded, err := loadAWSConfig(context.Background(), sc)
		if err != nil {
			t.Fatalf("loadAWSConfig: %v", err)
		}
		creds, err := loaded.Credentials.Retrieve(context.Background())
		if err != nil {
			t.Fatalf("retrieve: %v", err)
		}
		return creds
	}

	// Standard environment pair with a session token.
	t.Run("env", func(t *testing.T) {
		reset(t)
		t.Setenv("AWS_ACCESS_KEY_ID", "ENVKEY")
		t.Setenv("AWS_SECRET_ACCESS_KEY", "ENVSECRET")
		t.Setenv("AWS_SESSION_TOKEN", "ENVTOKEN")
		creds := retrieve(t)
		if creds.AccessKeyID != "ENVKEY" || creds.SecretAccessKey != "ENVSECRET" || creds.SessionToken != "ENVTOKEN" {
			t.Errorf("env creds = %+v", creds)
		}
	})

	// Web identity: the branch the round-4 forced default profile broke.
	t.Run("web_identity", func(t *testing.T) {
		reset(t)
		srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
			w.Header().Set("Content-Type", "text/xml")
			_, _ = io.WriteString(w, assumeRoleWithWebIdentityResponse)
		}))
		defer srv.Close()
		tokenFile := filepath.Join(t.TempDir(), "token")
		if err := os.WriteFile(tokenFile, []byte("web-identity-token"), 0o600); err != nil {
			t.Fatalf("write token: %v", err)
		}
		t.Setenv("AWS_WEB_IDENTITY_TOKEN_FILE", tokenFile)
		t.Setenv("AWS_ROLE_ARN", "arn:aws:iam::123456789012:role/web")
		// The web-identity provider builds its STS call before the config's
		// BaseEndpoint resolver runs, so the service-specific endpoint env var
		// (the documented SDK override) is what redirects it.
		t.Setenv("AWS_ENDPOINT_URL_STS", srv.URL)
		if creds := retrieve(t); creds.AccessKeyID != "WEBKEY" {
			t.Errorf("access key = %q, want WEBKEY (web identity)", creds.AccessKeyID)
		}
	})

	// Container credentials via a full URI.
	t.Run("container_full_uri", func(t *testing.T) {
		reset(t)
		srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
			_, _ = io.WriteString(w, `{"AccessKeyId":"CONTKEY","SecretAccessKey":"CONTSECRET","Token":"CONTTOKEN","Expiration":"2099-01-01T00:00:00Z"}`)
		}))
		defer srv.Close()
		t.Setenv("AWS_CONTAINER_CREDENTIALS_FULL_URI", srv.URL)
		if creds := retrieve(t); creds.AccessKeyID != "CONTKEY" {
			t.Errorf("access key = %q, want CONTKEY (container credentials)", creds.AccessKeyID)
		}
	})

	// IMDS instance metadata via the local endpoint.
	t.Run("imds", func(t *testing.T) {
		reset(t)
		srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			switch r.URL.Path {
			case "/latest/api/token":
				w.Header().Set("X-Aws-Ec2-Metadata-Token-Ttl-Seconds", "21600")
				_, _ = io.WriteString(w, "imds-token")
			case "/latest/meta-data/iam/security-credentials/":
				_, _ = io.WriteString(w, "test-role")
			default:
				_, _ = io.WriteString(w, `{"Code":"Success","Type":"AWS-HMAC","AccessKeyId":"IMDSKEY","SecretAccessKey":"IMDSSECRET","Token":"IMDSTOKEN","Expiration":"2099-01-01T00:00:00Z"}`)
			}
		}))
		defer srv.Close()
		t.Setenv("AWS_EC2_METADATA_SERVICE_ENDPOINT", srv.URL)
		if creds := retrieve(t); creds.AccessKeyID != "IMDSKEY" {
			t.Errorf("access key = %q, want IMDSKEY (IMDS)", creds.AccessKeyID)
		}
	})
}

// assumeRoleWithWebIdentityResponse is a minimal STS response for the web
// identity credential provider.
const assumeRoleWithWebIdentityResponse = `<AssumeRoleWithWebIdentityResponse xmlns="https://sts.amazonaws.com/doc/2011-06-15/">
  <AssumeRoleWithWebIdentityResult>
    <Credentials>
      <AccessKeyId>WEBKEY</AccessKeyId>
      <SecretAccessKey>WEBSECRET</SecretAccessKey>
      <SessionToken>WEBTOKEN</SessionToken>
      <Expiration>2099-01-01T00:00:00Z</Expiration>
    </Credentials>
  </AssumeRoleWithWebIdentityResult>
</AssumeRoleWithWebIdentityResponse>`

// TestSDKRetryResolution uses the real SDK loader to pin the retry behavior:
// botocore's 5-attempt default when nothing overrides it, AWS_MAX_ATTEMPTS and
// profile max_attempts overrides, standard/adaptive mode defaults (3), and the
// documented rejection of an explicit legacy mode.
func TestSDKRetryResolution(t *testing.T) {
	cfgPath := writeConfig(t, "[profile probe]\nmax_attempts = 7\n")

	// retryer resolves the config exactly as the production path does
	// (resolveConfig decides the botocore default, then the SDK loads it).
	retryer := func(t *testing.T) aws.Retryer {
		t.Helper()
		sc, err := resolveConfig(nil, os.Environ())
		if err != nil {
			t.Fatalf("resolveConfig: %v", err)
		}
		sc.region, sc.accessKey, sc.secretKey, sc.hasCreds = "us-east-1", "k", "s", true
		loaded, err := loadAWSConfig(context.Background(), sc)
		if err != nil {
			t.Fatalf("loadAWSConfig: %v", err)
		}
		return cloudwatchlogs.NewFromConfig(loaded).Options().Retryer
	}

	resetRetryEnv := func(t *testing.T) {
		t.Helper()
		t.Setenv("AWS_MAX_ATTEMPTS", "")
		t.Setenv("AWS_RETRY_MODE", "")
		t.Setenv("AWS_DEFAULT_PROFILE", "")
		t.Setenv("AWS_PROFILE", "")
		t.Setenv("AWS_CONFIG_FILE", writeConfig(t, "[default]\n"))
	}

	// Nothing overrides → botocore's 5.
	resetRetryEnv(t)
	if got := retryer(t).MaxAttempts(); got != 5 {
		t.Errorf("default max attempts = %d, want 5 (botocore default)", got)
	}

	// AWS_MAX_ATTEMPTS wins.
	t.Setenv("AWS_MAX_ATTEMPTS", "9")
	if got := retryer(t).MaxAttempts(); got != 9 {
		t.Errorf("AWS_MAX_ATTEMPTS max attempts = %d, want 9", got)
	}
	t.Setenv("AWS_MAX_ATTEMPTS", "")

	// Profile max_attempts wins.
	t.Setenv("AWS_CONFIG_FILE", cfgPath)
	t.Setenv("AWS_DEFAULT_PROFILE", "probe")
	if got := retryer(t).MaxAttempts(); got != 7 {
		t.Errorf("profile max attempts = %d, want 7", got)
	}
	t.Setenv("AWS_DEFAULT_PROFILE", "")
	t.Setenv("AWS_CONFIG_FILE", writeConfig(t, "[default]\n"))

	// The default profile may also be written as [profile default]; a
	// hand-rolled section scanner missed it and wrongly applied the 5-attempt
	// override on top of the profile's 7.
	t.Setenv("AWS_CONFIG_FILE", writeConfig(t, "[profile default]\nmax_attempts = 7\n"))
	if got := retryer(t).MaxAttempts(); got != 7 {
		t.Errorf("[profile default] max attempts = %d, want 7", got)
	}
	t.Setenv("AWS_CONFIG_FILE", writeConfig(t, "[default]\n"))

	// Explicit standard/adaptive modes keep botocore's 3-attempt default.
	for _, mode := range []string{"standard", "adaptive"} {
		t.Setenv("AWS_RETRY_MODE", mode)
		r := retryer(t)
		if got := r.MaxAttempts(); got != 3 {
			t.Errorf("%s max attempts = %d, want 3", mode, got)
		}
	}
	t.Setenv("AWS_RETRY_MODE", "")

	// The ONE deviation: an explicit legacy mode fails construction.
	t.Setenv("AWS_RETRY_MODE", "legacy")
	sc, err := resolveConfig(nil, os.Environ())
	if err != nil {
		t.Fatalf("resolveConfig: %v", err)
	}
	sc.region, sc.accessKey, sc.secretKey, sc.hasCreds = "us-east-1", "k", "s", true
	if _, err := loadAWSConfig(context.Background(), sc); err == nil {
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

// TestClientCustomCATrustSet drives the FULL production construction path:
// the resolved bundle must reach the real SDK client through the
// BuildableClient, completing a verified TLS handshake. Two servers with
// DIFFERENT CAs prove which bundle wins, and without a bundle the handshake
// must fail.
func TestClientCustomCATrustSet(t *testing.T) {
	serverA, caA := selfSignedServer(t)
	defer serverA.Close()
	serverB, caB := selfSignedServer(t)
	defer serverB.Close()

	client := func(t *testing.T, server *httptest.Server, caBundle string) logsClient {
		t.Helper()
		c, err := buildSDKClient(context.Background(), sdkConfig{
			region: "us-east-1", accessKey: "k", secretKey: "s", hasCreds: true,
			baseEndpoint: server.URL, caBundle: caBundle,
		})
		if err != nil {
			t.Fatalf("buildSDKClient: %v", err)
		}
		return c
	}
	mustTrust := func(t *testing.T, c logsClient, server string) {
		t.Helper()
		if err := c.CreateLogGroup(context.Background(), "group"); err != nil {
			t.Fatalf("handshake with the resolved bundle failed against %s: %v", server, err)
		}
	}
	mustReject := func(t *testing.T, c logsClient, server string) {
		t.Helper()
		if err := c.CreateLogGroup(context.Background(), "group"); err == nil {
			t.Fatalf("handshake succeeded against %s without its CA", server)
		}
	}

	// AWS_CA_BUNDLE wins over the profile and REQUESTS_CA_BUNDLE.
	t.Run("AWS_CA_BUNDLE", func(t *testing.T) {
		cfg := writeConfig(t, "[default]\nca_bundle = "+caB+"\n")
		sc, err := resolveConfig(nil, []string{"AWS_CA_BUNDLE=" + caA, "AWS_CONFIG_FILE=" + cfg, "REQUESTS_CA_BUNDLE=" + caB})
		if err != nil {
			t.Fatalf("resolveConfig: %v", err)
		}
		mustTrust(t, client(t, serverA, sc.caBundle), "A")
		mustReject(t, client(t, serverB, sc.caBundle), "B")
	})

	// The profile ca_bundle beats REQUESTS_CA_BUNDLE.
	t.Run("profile ca_bundle", func(t *testing.T) {
		cfg := writeConfig(t, "[default]\nca_bundle = "+caB+"\n")
		sc, err := resolveConfig(nil, []string{"AWS_CONFIG_FILE=" + cfg, "REQUESTS_CA_BUNDLE=" + caA})
		if err != nil {
			t.Fatalf("resolveConfig: %v", err)
		}
		mustTrust(t, client(t, serverB, sc.caBundle), "B")
		mustReject(t, client(t, serverA, sc.caBundle), "A")
	})

	// REQUESTS_CA_BUNDLE applies when neither AWS nor profile supplies one.
	t.Run("REQUESTS_CA_BUNDLE", func(t *testing.T) {
		cfg := writeConfig(t, "[default]\nregion = us-east-1\n")
		sc, err := resolveConfig(nil, []string{"AWS_CONFIG_FILE=" + cfg, "REQUESTS_CA_BUNDLE=" + caA})
		if err != nil {
			t.Fatalf("resolveConfig: %v", err)
		}
		mustTrust(t, client(t, serverA, sc.caBundle), "A")
		mustReject(t, client(t, serverB, sc.caBundle), "B")
	})

	// Without the CA the handshake must fail (proves the CA is actually used).
	mustReject(t, client(t, serverA, ""), "A")
}

// writeConfig writes a shared config file and returns its path.
func writeConfig(t *testing.T, contents string) string {
	t.Helper()
	path := filepath.Join(t.TempDir(), "config")
	if err := os.WriteFile(path, []byte(contents), 0o600); err != nil {
		t.Fatalf("write config: %v", err)
	}
	return path
}

// selfSignedServer starts a TLS server with its OWN freshly generated
// self-signed CA/leaf cert (httptest.NewTLSServer reuses one shared cert, so it
// cannot distinguish two trust bundles) and returns it plus the PEM path of its
// cert.
func selfSignedServer(t *testing.T) (*httptest.Server, string) {
	t.Helper()
	key, err := ecdsa.GenerateKey(elliptic.P256(), rand.Reader)
	if err != nil {
		t.Fatalf("generate key: %v", err)
	}
	tmpl := &x509.Certificate{
		SerialNumber:          big.NewInt(1),
		Subject:               pkix.Name{CommonName: "127.0.0.1"},
		NotBefore:             time.Now().Add(-time.Hour),
		NotAfter:              time.Now().Add(time.Hour),
		KeyUsage:              x509.KeyUsageDigitalSignature | x509.KeyUsageCertSign,
		ExtKeyUsage:           []x509.ExtKeyUsage{x509.ExtKeyUsageServerAuth},
		BasicConstraintsValid: true,
		IsCA:                  true,
		IPAddresses:           []net.IP{net.ParseIP("127.0.0.1")},
		DNSNames:              []string{"localhost"},
	}
	der, err := x509.CreateCertificate(rand.Reader, tmpl, tmpl, &key.PublicKey, key)
	if err != nil {
		t.Fatalf("create cert: %v", err)
	}
	srv := httptest.NewUnstartedServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		_, _ = io.WriteString(w, "{}")
	}))
	srv.TLS = &tls.Config{Certificates: []tls.Certificate{{Certificate: [][]byte{der}, PrivateKey: key}}}
	srv.StartTLS()

	path := filepath.Join(t.TempDir(), "ca.pem")
	pemBytes := pem.EncodeToMemory(&pem.Block{Type: "CERTIFICATE", Bytes: der})
	if err := os.WriteFile(path, pemBytes, 0o600); err != nil {
		t.Fatalf("write cert: %v", err)
	}
	return srv, path
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

// TestSDKCredentialConfigAndProfilePrecedence drives the production resolver
// (resolveConfig → loadAWSConfig → Retrieve) with BOTH credential sources set:
// an explicit journalpump config pair wins; an explicit AWS_PROFILE selects
// that profile's identity even when the environment pair is also present
// (documented §2 deviation (b)); and with no explicit profile the canonical
// environment pair wins over the default profile.
func TestSDKCredentialConfigAndProfilePrecedence(t *testing.T) {
	dir := t.TempDir()
	cfgPath := filepath.Join(dir, "config")
	credPath := filepath.Join(dir, "credentials")
	if err := os.WriteFile(cfgPath, []byte("[profile probe]\nregion = us-east-2\n[default]\nregion = us-east-1\n"), 0o600); err != nil {
		t.Fatalf("write config: %v", err)
	}
	if err := os.WriteFile(credPath, []byte("[probe]\naws_access_key_id = PROFKEY\naws_secret_access_key = PROFSECRET\n[default]\naws_access_key_id = DEFKEY\naws_secret_access_key = DEFSECRET\n"), 0o600); err != nil {
		t.Fatalf("write credentials: %v", err)
	}
	t.Setenv("AWS_CONFIG_FILE", cfgPath)
	t.Setenv("AWS_SHARED_CREDENTIALS_FILE", credPath)
	t.Setenv("AWS_DEFAULT_PROFILE", "")
	t.Setenv("AWS_PROFILE", "")
	t.Setenv("AWS_WEB_IDENTITY_TOKEN_FILE", "")
	t.Setenv("AWS_CONTAINER_CREDENTIALS_FULL_URI", "")
	t.Setenv("AWS_CONTAINER_CREDENTIALS_RELATIVE_URI", "")
	t.Setenv("AWS_EC2_METADATA_DISABLED", "true")
	// Conflicting environment credentials, present throughout.
	t.Setenv("AWS_ACCESS_KEY_ID", "ENVKEY")
	t.Setenv("AWS_SECRET_ACCESS_KEY", "ENVSECRET")

	// retrieveResolved runs the real resolver over the current environment.
	retrieveResolved := func(t *testing.T, cfg map[string]any) aws.Credentials {
		t.Helper()
		sc, err := resolveConfig(cfg, os.Environ())
		if err != nil {
			t.Fatalf("resolveConfig: %v", err)
		}
		loaded, err := loadAWSConfig(context.Background(), sc)
		if err != nil {
			t.Fatalf("loadAWSConfig: %v", err)
		}
		creds, err := loaded.Credentials.Retrieve(context.Background())
		if err != nil {
			t.Fatalf("retrieve: %v", err)
		}
		return creds
	}

	// The journalpump config pair is injected and wins over both the selected
	// profile and the environment pair.
	t.Setenv("AWS_PROFILE", "probe")
	cfg := map[string]any{"aws_access_key_id": "CFGKEY", "aws_secret_access_key": "CFGSECRET"}
	if creds := retrieveResolved(t, cfg); creds.AccessKeyID != "CFGKEY" {
		t.Errorf("access key = %q, want CFGKEY (config pair wins)", creds.AccessKeyID)
	}

	// Explicit profile + conflicting env pair: the SELECTED profile identity
	// wins (SDK skips the env pair). botocore's env provider would win here —
	// documented deviation (b).
	if creds := retrieveResolved(t, nil); creds.AccessKeyID != "PROFKEY" {
		t.Errorf("access key = %q, want PROFKEY (explicit profile beats env pair — deviation b)", creds.AccessKeyID)
	}

	// No explicit profile: the canonical environment pair wins over [default].
	t.Setenv("AWS_PROFILE", "")
	if creds := retrieveResolved(t, nil); creds.AccessKeyID != "ENVKEY" {
		t.Errorf("access key = %q, want ENVKEY (env pair beats default profile)", creds.AccessKeyID)
	}
}

// TestSDKCredentialEnvDeviations pins the two restored environment-credential
// differences as §2 deviations, driven through the real loader:
//   - AWS_SECURITY_TOKEN is IGNORED (botocore prefers it over AWS_SESSION_TOKEN);
//   - an AWS_ACCESS_KEY_ID with no secret falls through to another identity
//     (botocore raises PartialCredentialsError).
func TestSDKCredentialEnvDeviations(t *testing.T) {
	reset := func(t *testing.T) {
		t.Helper()
		for _, k := range []string{
			"AWS_ACCESS_KEY_ID", "AWS_SECRET_ACCESS_KEY", "AWS_SESSION_TOKEN",
			"AWS_SECURITY_TOKEN", "AWS_ACCESS_KEY", "AWS_SECRET_KEY",
			"AWS_WEB_IDENTITY_TOKEN_FILE", "AWS_ROLE_ARN",
			"AWS_CONTAINER_CREDENTIALS_FULL_URI", "AWS_CONTAINER_CREDENTIALS_RELATIVE_URI",
			"AWS_EC2_METADATA_SERVICE_ENDPOINT", "AWS_EC2_METADATA_DISABLED",
			"AWS_DEFAULT_PROFILE", "AWS_PROFILE", "AWS_ENDPOINT_URL", "AWS_ENDPOINT_URL_STS",
		} {
			t.Setenv(k, "")
		}
		t.Setenv("AWS_CONFIG_FILE", writeConfig(t, "[default]\nregion = us-east-1\n"))
		t.Setenv("AWS_SHARED_CREDENTIALS_FILE", writeConfig(t, ""))
	}

	retrieve := func(t *testing.T) aws.Credentials {
		t.Helper()
		sc, err := resolveConfig(nil, os.Environ())
		if err != nil {
			t.Fatalf("resolveConfig: %v", err)
		}
		loaded, err := loadAWSConfig(context.Background(), sc)
		if err != nil {
			t.Fatalf("loadAWSConfig: %v", err)
		}
		creds, err := loaded.Credentials.Retrieve(context.Background())
		if err != nil {
			t.Fatalf("retrieve: %v", err)
		}
		return creds
	}

	// AWS_SECURITY_TOKEN alone yields no session token (botocore would use it).
	t.Run("security_token_ignored", func(t *testing.T) {
		reset(t)
		t.Setenv("AWS_EC2_METADATA_DISABLED", "true")
		t.Setenv("AWS_ACCESS_KEY_ID", "K")
		t.Setenv("AWS_SECRET_ACCESS_KEY", "S")
		t.Setenv("AWS_SECURITY_TOKEN", "SEC")
		creds := retrieve(t)
		if creds.AccessKeyID != "K" {
			t.Fatalf("access key = %q, want K", creds.AccessKeyID)
		}
		if creds.SessionToken != "" {
			t.Errorf("session token = %q, want empty (AWS_SECURITY_TOKEN ignored — deviation)", creds.SessionToken)
		}
	})

	// An access key with no secret falls through to the container identity
	// rather than raising botocore's PartialCredentialsError.
	t.Run("access_key_without_secret_falls_through", func(t *testing.T) {
		reset(t)
		srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
			_, _ = io.WriteString(w, `{"AccessKeyId":"CONTKEY","SecretAccessKey":"CONTSECRET","Token":"CONTTOKEN","Expiration":"2099-01-01T00:00:00Z"}`)
		}))
		defer srv.Close()
		t.Setenv("AWS_EC2_METADATA_DISABLED", "true")
		t.Setenv("AWS_ACCESS_KEY_ID", "ACCESSONLY")
		t.Setenv("AWS_CONTAINER_CREDENTIALS_FULL_URI", srv.URL)
		if creds := retrieve(t); creds.AccessKeyID != "CONTKEY" {
			t.Errorf("access key = %q, want CONTKEY (access-only falls through — deviation)", creds.AccessKeyID)
		}
	})
}

// TestSDKCredentialExplicitProfileSkipsWebIdentity pins the extended §2
// deviation (b): selecting a (region-only) profile via AWS_PROFILE makes the
// SDK skip the environment web-identity branch and fall through to container
// credentials, unlike Python's credential chain. The non-vacuity check removes
// the profile and shows the same setup then resolves via web identity.
func TestSDKCredentialExplicitProfileSkipsWebIdentity(t *testing.T) {
	sts := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		w.Header().Set("Content-Type", "text/xml")
		_, _ = io.WriteString(w, assumeRoleWithWebIdentityResponse)
	}))
	defer sts.Close()
	cont := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		_, _ = io.WriteString(w, `{"AccessKeyId":"CONTKEY","SecretAccessKey":"CONTSECRET","Token":"CONTTOKEN","Expiration":"2099-01-01T00:00:00Z"}`)
	}))
	defer cont.Close()

	cfgPath := filepath.Join(t.TempDir(), "config")
	if err := os.WriteFile(cfgPath, []byte("[profile probe]\nregion = us-east-2\n"), 0o600); err != nil {
		t.Fatalf("write config: %v", err)
	}
	tokenFile := filepath.Join(t.TempDir(), "token")
	if err := os.WriteFile(tokenFile, []byte("web-identity-token"), 0o600); err != nil {
		t.Fatalf("write token: %v", err)
	}
	for _, k := range []string{
		"AWS_ACCESS_KEY_ID", "AWS_SECRET_ACCESS_KEY", "AWS_SESSION_TOKEN",
		"AWS_SECURITY_TOKEN", "AWS_ACCESS_KEY", "AWS_SECRET_KEY",
		"AWS_DEFAULT_PROFILE", "AWS_PROFILE",
	} {
		t.Setenv(k, "")
	}
	t.Setenv("AWS_CONFIG_FILE", cfgPath)
	t.Setenv("AWS_SHARED_CREDENTIALS_FILE", writeConfig(t, ""))
	t.Setenv("AWS_EC2_METADATA_DISABLED", "true")
	t.Setenv("AWS_WEB_IDENTITY_TOKEN_FILE", tokenFile)
	t.Setenv("AWS_ROLE_ARN", "arn:aws:iam::123456789012:role/web")
	t.Setenv("AWS_ENDPOINT_URL_STS", sts.URL)
	t.Setenv("AWS_CONTAINER_CREDENTIALS_FULL_URI", cont.URL)

	retrieve := func(t *testing.T) aws.Credentials {
		t.Helper()
		sc, err := resolveConfig(nil, os.Environ())
		if err != nil {
			t.Fatalf("resolveConfig: %v", err)
		}
		loaded, err := loadAWSConfig(context.Background(), sc)
		if err != nil {
			t.Fatalf("loadAWSConfig: %v", err)
		}
		creds, err := loaded.Credentials.Retrieve(context.Background())
		if err != nil {
			t.Fatalf("retrieve: %v", err)
		}
		return creds
	}

	// Non-vacuity: with no explicit profile, web identity is used.
	if creds := retrieve(t); creds.AccessKeyID != "WEBKEY" {
		t.Fatalf("access key = %q, want WEBKEY (web identity without an explicit profile)", creds.AccessKeyID)
	}

	// Explicit profile: web identity is skipped, container wins (deviation).
	t.Setenv("AWS_PROFILE", "probe")
	if creds := retrieve(t); creds.AccessKeyID != "CONTKEY" {
		t.Errorf("access key = %q, want CONTKEY (explicit profile skips web identity — deviation)", creds.AccessKeyID)
	}
}

// TestSDKCredentialLegacyAliasDeviation pins the documented §2 deviation (a):
// the SDK honours the ancient AWS_ACCESS_KEY/AWS_SECRET_KEY aliases, which
// botocore's EnvProvider ignores, so with those aliases set and no canonical
// env pair the SDK selects them instead of the [default] profile identity.
// This is loud and rare; it is recorded rather than papered over.
func TestSDKCredentialLegacyAliasDeviation(t *testing.T) {
	dir := t.TempDir()
	cfgPath := filepath.Join(dir, "config")
	credPath := filepath.Join(dir, "credentials")
	if err := os.WriteFile(cfgPath, []byte("[default]\nregion = us-east-1\n"), 0o600); err != nil {
		t.Fatalf("write config: %v", err)
	}
	if err := os.WriteFile(credPath, []byte("[default]\naws_access_key_id = PROFKEY\naws_secret_access_key = PROFSECRET\n"), 0o600); err != nil {
		t.Fatalf("write credentials: %v", err)
	}
	t.Setenv("AWS_CONFIG_FILE", cfgPath)
	t.Setenv("AWS_SHARED_CREDENTIALS_FILE", credPath)
	t.Setenv("AWS_DEFAULT_PROFILE", "")
	t.Setenv("AWS_PROFILE", "")
	t.Setenv("AWS_ACCESS_KEY_ID", "")
	t.Setenv("AWS_SECRET_ACCESS_KEY", "")
	t.Setenv("AWS_WEB_IDENTITY_TOKEN_FILE", "")
	t.Setenv("AWS_CONTAINER_CREDENTIALS_FULL_URI", "")
	t.Setenv("AWS_EC2_METADATA_DISABLED", "true")
	t.Setenv("AWS_ACCESS_KEY", "ALIASKEY")
	t.Setenv("AWS_SECRET_KEY", "ALIASSECRET")

	sc, err := resolveConfig(nil, os.Environ())
	if err != nil {
		t.Fatalf("resolveConfig: %v", err)
	}
	loaded, err := loadAWSConfig(context.Background(), sc)
	if err != nil {
		t.Fatalf("loadAWSConfig: %v", err)
	}
	creds, err := loaded.Credentials.Retrieve(context.Background())
	if err != nil {
		t.Fatalf("retrieve: %v", err)
	}
	if creds.AccessKeyID != "ALIASKEY" {
		t.Errorf("access key = %q, want ALIASKEY (documented deviation: SDK honours legacy aliases, botocore ignores them)", creds.AccessKeyID)
	}
}

// TestHTTPClientTLSHandshakeTimeout pins that the transport's TLS handshake
// timeout is botocore's connect timeout (not the BuildableClient's 10s default)
// and that a stalled handshake is bounded by it.
func TestHTTPClientTLSHandshakeTimeout(t *testing.T) {
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("listen: %v", err)
	}
	defer func() { _ = ln.Close() }()
	go func() {
		for {
			conn, err := ln.Accept()
			if err != nil {
				return
			}
			// Accept but never speak TLS, stalling the handshake.
			_ = conn
		}
	}()

	const handshakeBound = 300 * time.Millisecond
	sc := sdkConfig{proxy: botocoreProxyFunc(nil), connectTimeout: handshakeBound}
	hc, err := sc.newHTTPClient()
	if err != nil {
		t.Fatalf("newHTTPClient: %v", err)
	}
	b, ok := hc.(*awshttp.BuildableClient)
	if !ok {
		t.Fatalf("client = %T, want *awshttp.BuildableClient", hc)
	}
	if got := b.GetTransport().TLSHandshakeTimeout; got != handshakeBound {
		t.Errorf("TLSHandshakeTimeout = %v, want %v (botocore connect timeout)", got, handshakeBound)
	}

	req, _ := http.NewRequest(http.MethodGet, "https://"+ln.Addr().String()+"/", nil)
	start := time.Now()
	resp, err := hc.Do(req)
	if err == nil {
		_ = resp.Body.Close()
		t.Fatal("stalled TLS handshake did not time out")
	}
	if d := time.Since(start); d > 3*time.Second {
		t.Errorf("stalled handshake returned after %v, want a bounded timeout", d)
	}
}

// TestBuildSDKClientDiscoveryBounded exercises construction with an ACTIVE
// discovery path: no static credentials/region and a stalled local IMDS
// endpoint with auto defaults discovery. It asserts discovery was actually
// reached (so the test is not vacuous) and that the ctx bounds completion.
func TestBuildSDKClientDiscoveryBounded(t *testing.T) {
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("listen: %v", err)
	}
	defer func() { _ = ln.Close() }()
	var hits atomic.Int32
	go func() {
		for {
			conn, err := ln.Accept()
			if err != nil {
				return
			}
			hits.Add(1)
			// Accept but never respond, stalling the IMDS request.
			_ = conn
		}
	}()

	t.Setenv("AWS_EC2_METADATA_SERVICE_ENDPOINT", "http://"+ln.Addr().String())
	t.Setenv("AWS_EC2_METADATA_DISABLED", "")
	t.Setenv("AWS_DEFAULTS_MODE", "auto")
	t.Setenv("AWS_REGION", "")
	t.Setenv("AWS_DEFAULT_REGION", "")
	t.Setenv("AWS_PROFILE", "")
	t.Setenv("AWS_DEFAULT_PROFILE", "")
	t.Setenv("AWS_ACCESS_KEY_ID", "")
	t.Setenv("AWS_SECRET_ACCESS_KEY", "")
	t.Setenv("AWS_CONFIG_FILE", writeConfig(t, "[default]\n"))
	t.Setenv("AWS_SHARED_CREDENTIALS_FILE", writeConfig(t, ""))

	// No static credentials or region → the discovery path is active.
	sc := sdkConfig{proxy: botocoreProxyFunc(nil)}
	ctx, cancel := context.WithTimeout(context.Background(), 400*time.Millisecond)
	defer cancel()

	start := time.Now()
	done := make(chan struct{})
	go func() {
		defer close(done)
		_, _ = buildSDKClient(ctx, sc)
	}()
	select {
	case <-done:
		if d := time.Since(start); d > 4*time.Second {
			t.Errorf("construction took %v, want a ctx-bounded completion", d)
		}
	case <-time.After(6 * time.Second):
		t.Fatal("construction was not bounded by the ctx deadline")
	}
	if hits.Load() == 0 {
		t.Error("discovery never reached the stalled IMDS endpoint (test is vacuous)")
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
