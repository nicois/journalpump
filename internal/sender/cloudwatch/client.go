// Copyright 2026, Aiven, https://aiven.io/
//
// This file is under the Apache License, Version 2.0.
// See the file `LICENSE` for details.

package cloudwatch

import (
	"bufio"
	"bytes"
	"context"
	"errors"
	"fmt"
	"net"
	"net/http"
	"net/url"
	"os"
	"path/filepath"
	"strings"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	awshttp "github.com/aws/aws-sdk-go-v2/aws/transport/http"
	"github.com/aws/aws-sdk-go-v2/config"
	"github.com/aws/aws-sdk-go-v2/credentials"
	"github.com/aws/aws-sdk-go-v2/service/cloudwatchlogs"
	cwltypes "github.com/aws/aws-sdk-go-v2/service/cloudwatchlogs/types"
	"github.com/aws/smithy-go"
)

// sdkInitTimeout bounds the construction-time SDK config load (profile
// files, and any container/IMDS metadata the credential chain probes).
// botocore's construction is likewise bounded by its own metadata timeouts;
// this keeps a CloudWatch sender construction (startup/reload) from blocking
// without a bound. See the parity doc (Task 25) for the credentials-cache
// caveat: aws.CredentialsCache performs its own background retrieval and can
// suppress a caller deadline, so the caller may return before credentials
// resolve. One SDK-internal step remains outside this bound: the
// container-credentials provider's host validation performs its own
// hostname resolution that does not observe ctx (documented residual).
const sdkInitTimeout = 5 * time.Second

// botocore's default HTTP timeouts (Config: connect_timeout/read_timeout are
// both 60s). They are applied through the SDK's BuildableClient so the SDK
// keeps ownership of the transport.
const (
	connectTimeout = 60 * time.Second
	readTimeout    = 60 * time.Second
)

// sdkConfig is the botocore-compatible settings resolved from the sender
// config and the environment before the SDK client is built.
type sdkConfig struct {
	region  string
	profile string

	// Explicit pair from the journalpump config (aws_access_key_id/secret);
	// wins over the environment chain, as in botocore.
	accessKey string
	secretKey string
	hasCreds  bool

	// Env pair (AWS_ACCESS_KEY_ID/AWS_SECRET_ACCESS_KEY, or the legacy
	// AWS_ACCESS_KEY/AWS_SECRET_KEY) with AWS_SESSION_TOKEN. Passed as an
	// explicit provider when config credentials are absent so the env pair
	// keeps botocore's precedence over the selected profile.
	envAccessKey string
	envSecretKey string
	envSession   string
	hasEnvCreds  bool

	// caBundle is the CA bundle path (AWS_CA_BUNDLE then REQUESTS_CA_BUNDLE);
	// empty means the SDK/system default trust set.
	caBundle string

	// proxy is the botocore-compatible transport proxy selector.
	proxy func(*http.Request) (*url.URL, error)

	// baseEndpoint overrides the service endpoint (tests).
	baseEndpoint string
	// httpClient overrides the transport (tests).
	httpClient *http.Client

	// connectTimeout/readTimeout bound the transport; zero means the
	// botocore defaults (tests use smaller values).
	connectTimeout time.Duration
	readTimeout    time.Duration
}

// sdkClient wraps the aws-sdk-go-v2 cloudwatchlogs client behind logsClient.
// All calls take ctx, so cancellation and deadlines propagate (the SDK adds
// no sarama-style non-interruptible-write limitation). See the parity doc for
// the credentials-cache caveat.
type sdkClient struct {
	client *cloudwatchlogs.Client
}

// newSDKClient builds the production client for the sender config, resolving
// region/profile/credentials/CA/proxy/retry the botocore way and bounding the
// config load with sdkInitTimeout.
func newSDKClient(cfg map[string]any) (logsClient, error) {
	sc, err := resolveConfig(cfg, os.Environ())
	if err != nil {
		return nil, err
	}
	ctx, cancel := context.WithTimeout(context.Background(), sdkInitTimeout)
	defer cancel()
	return buildSDKClient(ctx, sc)
}

// resolveConfig resolves the botocore-compatible settings. It is pure over
// (cfg, environ) so the precedence rules are unit-testable. environ is a
// "KEY=VALUE" list (os.Environ()).
func resolveConfig(cfg map[string]any, environ []string) (sdkConfig, error) {
	sc := sdkConfig{}
	sc.proxy = botocoreProxyFunc(environ)

	// Region: an explicit aws_region config wins (Python passes region_name).
	// Otherwise botocore reads AWS_DEFAULT_REGION only — NOT AWS_REGION.
	if explicit, err := optionalPresentString(cfg, "aws_region"); err != nil {
		return sdkConfig{}, err
	} else if explicit != nil {
		sc.region = *explicit
	} else {
		sc.region = envLookup(environ, "AWS_DEFAULT_REGION")
	}

	// Profile: botocore prefers AWS_DEFAULT_PROFILE over AWS_PROFILE.
	sc.profile = envLookup(environ, "AWS_DEFAULT_PROFILE")
	if sc.profile == "" {
		sc.profile = envLookup(environ, "AWS_PROFILE")
	}

	// Credentials: botocore rejects a PARTIAL explicit pair at construction
	// (PartialCredentialsError); a present-but-empty value counts as supplied.
	access, err := optionalPresentString(cfg, "aws_access_key_id")
	if err != nil {
		return sdkConfig{}, err
	}
	secret, err := optionalPresentString(cfg, "aws_secret_access_key")
	if err != nil {
		return sdkConfig{}, err
	}
	switch {
	case access != nil && secret != nil:
		sc.accessKey, sc.secretKey, sc.hasCreds = *access, *secret, true
	case access != nil:
		return sdkConfig{}, errors.New("cloudwatch: partial credentials: aws_access_key_id supplied without aws_secret_access_key")
	case secret != nil:
		return sdkConfig{}, errors.New("cloudwatch: partial credentials: aws_secret_access_key supplied without aws_access_key_id")
	}

	// Environment credentials, mirroring botocore's EnvProvider exactly: the
	// access key is AWS_ACCESS_KEY_ID only (no legacy alias); when it is
	// absent the provider yields nothing and falls through to the profile,
	// even if a secret key is present; when it is present the secret key is
	// required (else PartialCredentialsError). The token is the first present
	// of AWS_SECURITY_TOKEN then AWS_SESSION_TOKEN. Env credentials take
	// precedence over the selected profile, so they are resolved here (only
	// when the config pair did not supply credentials) and passed as an
	// explicit provider.
	if !sc.hasCreds {
		if envAccess := envLookup(environ, "AWS_ACCESS_KEY_ID"); envAccess != "" {
			envSecret := envLookup(environ, "AWS_SECRET_ACCESS_KEY")
			if envSecret == "" {
				return sdkConfig{}, errors.New("cloudwatch: partial credentials: AWS_ACCESS_KEY_ID supplied without AWS_SECRET_ACCESS_KEY")
			}
			sc.envAccessKey, sc.envSecretKey, sc.hasEnvCreds = envAccess, envSecret, true
			sc.envSession = envLookup(environ, "AWS_SECURITY_TOKEN")
			if sc.envSession == "" {
				sc.envSession = envLookup(environ, "AWS_SESSION_TOKEN")
			}
		}
	}

	// CA bundle: botocore resolves AWS_CA_BUNDLE, then the profile's
	// ca_bundle, then REQUESTS_CA_BUNDLE (endpoint._get_verify_value). The
	// profile bundle is read from the shared config file for the selected
	// profile; the SDK has no native ca_bundle key, so it is parsed here.
	sc.caBundle = envLookup(environ, "AWS_CA_BUNDLE")
	if sc.caBundle == "" {
		sc.caBundle = profileCABundle(sc.profile, environ)
	}
	if sc.caBundle == "" {
		sc.caBundle = envLookup(environ, "REQUESTS_CA_BUNDLE")
	}

	// Retry is left to the SDK's own env/profile resolution. botocore and the
	// SDK agree on standard/adaptive defaults (3 attempts) and on
	// AWS_MAX_ATTEMPTS / profile max_attempts. The SDK has no "legacy" mode,
	// so AWS_RETRY_MODE=legacy cannot be honored and surfaces the SDK's own
	// rejection (documented deviation; see the parity doc).
	return sc, nil
}

// buildSDKClient loads the SDK config and constructs the client, applying any
// extra client options (tests inject a base endpoint / HTTP client).
func buildSDKClient(ctx context.Context, sc sdkConfig, clientOpts ...func(*cloudwatchlogs.Options)) (logsClient, error) {
	loaded, err := loadAWSConfig(ctx, sc)
	if err != nil {
		return nil, fmt.Errorf("cloudwatch: load AWS config: %w", err)
	}
	return &sdkClient{client: cloudwatchlogs.NewFromConfig(loaded, clientOpts...)}, nil
}

// loadAWSConfig renders the resolved settings as SDK config options and loads
// them. Exposed separately so tests can assert the SDK-resolved region,
// profile, retry, and credentials.
func loadAWSConfig(ctx context.Context, sc sdkConfig) (aws.Config, error) {
	loadOpts, err := sc.loadOptions()
	if err != nil {
		return aws.Config{}, err
	}
	return config.LoadDefaultConfig(ctx, loadOpts...)
}

// loadOptions renders the resolved settings as SDK config options. The HTTP
// client is a BuildableClient so the SDK itself injects the custom CA bundle
// (WithCustomCABundle) without hand-rolling TLS roots.
func (sc sdkConfig) loadOptions() ([]func(*config.LoadOptions) error, error) {
	var opts []func(*config.LoadOptions) error
	if sc.region != "" {
		opts = append(opts, config.WithRegion(sc.region))
	}
	if sc.profile != "" {
		opts = append(opts, config.WithSharedConfigProfile(sc.profile))
	}
	switch {
	case sc.hasCreds:
		opts = append(opts, config.WithCredentialsProvider(
			credentials.NewStaticCredentialsProvider(sc.accessKey, sc.secretKey, ""),
		))
	case sc.hasEnvCreds:
		opts = append(opts, config.WithCredentialsProvider(
			credentials.NewStaticCredentialsProvider(sc.envAccessKey, sc.envSecretKey, sc.envSession),
		))
	}
	if sc.caBundle != "" {
		pem, err := os.ReadFile(sc.caBundle)
		if err != nil {
			return nil, fmt.Errorf("cloudwatch: read CA bundle %s: %w", sc.caBundle, err)
		}
		opts = append(opts, config.WithCustomCABundle(bytes.NewReader(pem)))
	}
	if sc.baseEndpoint != "" {
		opts = append(opts, config.WithBaseEndpoint(sc.baseEndpoint))
	}
	httpClient, err := sc.newHTTPClient()
	if err != nil {
		return nil, err
	}
	opts = append(opts, config.WithHTTPClient(httpClient))
	return opts, nil
}

// newHTTPClient builds the HTTP client through the SDK's BuildableClient so the
// SDK owns the transport: it keeps its TLS/CA injection (which requires a
// BuildableClient), applies the botocore-compatible proxy selector, and bounds
// the connection with botocore's connect/read (inactivity) timeouts.
func (sc sdkConfig) newHTTPClient() (aws.HTTPClient, error) {
	if sc.httpClient != nil {
		return sc.httpClient, nil
	}
	connect, read := sc.connectTimeout, sc.readTimeout
	if connect <= 0 {
		connect = connectTimeout
	}
	if read <= 0 {
		read = readTimeout
	}
	b := awshttp.NewBuildableClient().
		WithTransportOptions(func(tr *http.Transport) {
			tr.Proxy = sc.proxy
			// botocore applies its connect timeout to the TLS handshake too;
			// the BuildableClient default is 10s.
			tr.TLSHandshakeTimeout = connect
		}).
		WithDialerOptions(func(d *net.Dialer) { d.Timeout = connect })
	return b.WithReadTimeout(read), nil
}

// envLookup returns the value of key in an os.Environ()-style list ("" when
// absent).
func envLookup(environ []string, key string) string {
	for _, kv := range environ {
		if strings.HasPrefix(kv, key+"=") {
			return kv[len(key)+1:]
		}
	}
	return ""
}

// profileCABundle reads the ca_bundle setting for the selected profile from the
// shared config file (AWS_CONFIG_FILE, else ~/.aws/config), mirroring
// botocore's config-file lookup: the default profile uses [default], others use
// [profile NAME]. An unreadable or absent file yields "". This exists because
// the SDK has no native ca_bundle config key.
func profileCABundle(profile string, environ []string) string {
	if profile == "" {
		profile = "default"
	}
	section := "profile " + profile
	if profile == "default" {
		section = "default"
	}
	path := envLookup(environ, "AWS_CONFIG_FILE")
	if path == "" {
		home, err := os.UserHomeDir()
		if err != nil {
			return ""
		}
		path = filepath.Join(home, ".aws", "config")
	}
	f, err := os.Open(path)
	if err != nil {
		return ""
	}
	defer func() { _ = f.Close() }()

	inSection := false
	scanner := bufio.NewScanner(f)
	for scanner.Scan() {
		line := strings.TrimSpace(scanner.Text())
		if line == "" || strings.HasPrefix(line, "#") || strings.HasPrefix(line, ";") {
			continue
		}
		if strings.HasPrefix(line, "[") && strings.HasSuffix(line, "]") {
			inSection = strings.EqualFold(strings.TrimSpace(line[1:len(line)-1]), section)
			continue
		}
		if !inSection {
			continue
		}
		key, value, ok := strings.Cut(line, "=")
		if !ok {
			continue
		}
		if strings.EqualFold(strings.TrimSpace(key), "ca_bundle") {
			return strings.TrimSpace(value)
		}
	}
	return ""
}

// botocoreProxies reproduces urllib.request.getproxies_environment: scan for
// <scheme>_proxy vars, then a lowercase-only second pass that overrides (or,
// when empty, removes) the entry. The value for "no" is the no_proxy list.
func botocoreProxies(environ []string) map[string]string {
	proxies := map[string]string{}
	type entry struct{ name, value, lower string }
	var entries []entry
	for _, kv := range environ {
		i := strings.IndexByte(kv, '=')
		if i < 0 {
			continue
		}
		name, value := kv[:i], kv[i+1:]
		// name must be longer than "proxy" and end with '_proxy'
		// (case-insensitive on the suffix).
		if len(name) <= 5 || name[len(name)-6] != '_' || !strings.EqualFold(name[len(name)-5:], "proxy") {
			continue
		}
		lower := strings.ToLower(name[:len(name)-6])
		entries = append(entries, entry{name: name, value: value, lower: lower})
		if value != "" {
			proxies[lower] = value
		}
	}
	// CVE-2016-1000110: a CGI environment drops the (non-lowercase) HTTP_PROXY.
	if envLookup(environ, "REQUEST_METHOD") != "" {
		delete(proxies, "http")
	}
	for _, e := range entries {
		if e.name[len(e.name)-6:] == "_proxy" {
			if e.value != "" {
				proxies[e.lower] = e.value
			} else {
				delete(proxies, e.lower)
			}
		}
	}
	return proxies
}

// proxyBypass ports urllib.request.proxy_bypass_environment for a host
// (host[:port]): no_proxy "*" bypasses everything, otherwise a comma list of
// DNS suffixes (leading dots ignored, label-boundary via a prepended dot).
func proxyBypass(host string, proxies map[string]string) bool {
	noProxy, ok := proxies["no"]
	if !ok {
		return false
	}
	if noProxy == "*" {
		return true
	}
	host = strings.ToLower(host)
	hostonly := host
	if h, _, err := net.SplitHostPort(host); err == nil {
		hostonly = h
	}
	for _, name := range strings.Split(noProxy, ",") {
		name = strings.TrimSpace(name)
		if name == "" {
			continue
		}
		name = strings.TrimLeft(name, ".")
		name = strings.ToLower(name)
		if hostonly == name || host == name {
			return true
		}
		name = "." + name
		if strings.HasSuffix(hostonly, name) || strings.HasSuffix(host, name) {
			return true
		}
	}
	return false
}

// botocoreProxyFunc builds the transport proxy selector from the environment
// snapshot.
func botocoreProxyFunc(environ []string) func(*http.Request) (*url.URL, error) {
	proxies := botocoreProxies(environ)
	return func(req *http.Request) (*url.URL, error) {
		if req.URL == nil {
			return nil, nil
		}
		if proxyBypass(req.URL.Host, proxies) {
			return nil, nil
		}
		raw := proxies[strings.ToLower(req.URL.Scheme)]
		if raw == "" {
			return nil, nil
		}
		// A scheme-relative value ("//host:port") is completed with http, the
		// same as a bare authority, rather than producing "http:////host".
		if strings.HasPrefix(raw, "//") {
			raw = "http:" + raw
		} else if !strings.Contains(raw, "://") {
			raw = "http://" + raw
		}
		return url.Parse(raw)
	}
}

// optionalPresentString reads an optional config key, distinguishing ABSENT
// (nil) from present (including an empty string), and rejecting a present
// non-string value.
func optionalPresentString(cfg map[string]any, key string) (*string, error) {
	raw, present := cfg[key]
	if !present || raw == nil {
		return nil, nil
	}
	s, ok := raw.(string)
	if !ok {
		return nil, fmt.Errorf("%s: %v is not a string", key, raw)
	}
	return &s, nil
}

// CreateLogGroup implements logsClient.
func (c *sdkClient) CreateLogGroup(ctx context.Context, group string) error {
	_, err := c.client.CreateLogGroup(ctx, &cloudwatchlogs.CreateLogGroupInput{LogGroupName: aws.String(group)})
	return wrapAPIError(err)
}

// CreateLogStream implements logsClient.
func (c *sdkClient) CreateLogStream(ctx context.Context, group, stream string) error {
	_, err := c.client.CreateLogStream(ctx, &cloudwatchlogs.CreateLogStreamInput{
		LogGroupName:  aws.String(group),
		LogStreamName: aws.String(stream),
	})
	return wrapAPIError(err)
}

// UploadSequenceToken implements logsClient, paging describe_log_streams until
// the named stream is found (aws_cloudwatch.py:82-90's paginator).
func (c *sdkClient) UploadSequenceToken(ctx context.Context, group, stream string) (string, bool, error) {
	pager := cloudwatchlogs.NewDescribeLogStreamsPaginator(c.client, &cloudwatchlogs.DescribeLogStreamsInput{
		LogGroupName: aws.String(group),
	})
	for pager.HasMorePages() {
		page, err := pager.NextPage(ctx)
		if err != nil {
			return "", false, wrapAPIError(err)
		}
		for _, s := range page.LogStreams {
			if aws.ToString(s.LogStreamName) == stream {
				return aws.ToString(s.UploadSequenceToken), true, nil
			}
		}
	}
	return "", false, nil
}

// PutLogEvents implements logsClient. The SDK returns an error for non-2xx
// responses, so a nil error is reported as HTTP 200 (Python's
// ResponseMetadata.HTTPStatusCode check).
func (c *sdkClient) PutLogEvents(ctx context.Context, group, stream, sequenceToken string, events []logEvent) (string, int, error) {
	in := &cloudwatchlogs.PutLogEventsInput{
		LogGroupName:  aws.String(group),
		LogStreamName: aws.String(stream),
		LogEvents:     make([]cwltypes.InputLogEvent, 0, len(events)),
	}
	if sequenceToken != "" {
		in.SequenceToken = aws.String(sequenceToken)
	}
	for _, ev := range events {
		in.LogEvents = append(in.LogEvents, cwltypes.InputLogEvent{
			Timestamp: aws.Int64(ev.Timestamp),
			Message:   aws.String(ev.Message),
		})
	}
	out, err := c.client.PutLogEvents(ctx, in)
	if err != nil {
		return "", 0, wrapAPIError(err)
	}
	return aws.ToString(out.NextSequenceToken), 200, nil
}

// wrapAPIError maps a smithy API error onto apiError so the sender can read
// the botocore Error.Code the same way Python does. Non-API errors pass
// through unchanged.
func wrapAPIError(err error) error {
	if err == nil {
		return nil
	}
	var api smithy.APIError
	if errors.As(err, &api) {
		return &apiError{Code: api.ErrorCode(), Message: api.ErrorMessage()}
	}
	return err
}
