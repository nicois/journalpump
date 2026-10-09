// Copyright 2026, Aiven, https://aiven.io/
//
// This file is under the Apache License, Version 2.0.
// See the file `LICENSE` for details.

// Package statsd is a port of journalpump/statsd.py: a raw-UDP statsd
// client speaking telegraf's "metric,tag=val:value|type" protocol
// extension (https://github.com/influxdata/telegraf/tree/master/plugins/inputs/statsd).
//
// The client deliberately performs NO tag sanitization: Python sanitizes
// at the Tagged.make_tags call sites (journalpump/senders/base.py), which
// is ported in internal/tagged — the wire layer receives already
// sanitized tags exactly like statsd.py does.
package statsd

import (
	"net"
	"reflect"
	"sort"
	"strconv"
	"sync"

	"github.com/Aiven-Open/journalpump/internal/journal"
)

// Logger is the logging subset used for send failures; the standard
// library *log.Logger satisfies it. Python logs at INFO through the
// "StatsClient" logger (statsd.py:60).
type Logger interface {
	Printf(format string, args ...any)
}

type discardLogger struct{}

func (discardLogger) Printf(_ string, _ ...any) {}

// Client mirrors statsd.StatsClient. Sending is disabled when the host
// or the port is MISSING (Python: `None in self._dest_addr`,
// statsd.py:46-48); journalpump.py passes the config values straight
// through via stats.get("host")/stats.get("port"), so an absent host or
// port means "no stats" and every send is a no-op. An explicitly EMPTY
// host string is enabled: CPython's setipaddr special-cases the empty
// host to INADDR_ANY (0.0.0.0) without calling getaddrinfo, so
// sendto(("", port)) sends to 0.0.0.0 and the kernel delivers locally.
type Client struct {
	disabled bool
	host     string
	port     int

	// tags holds the global tags from the config ("statsd"."tags");
	// per-call tags are merged over them on every send.
	tags map[string]string
	// globalOrder holds the global tag keys in config document order
	// (captured from the raw JSON by the config layer, mirroring
	// Python's dict order); nil falls back to sorted keys.
	globalOrder []string
	// prefix is prepended to every metric name (default "").
	prefix string
	log    Logger

	mu   sync.Mutex
	conn *net.UDPConn
	addr *net.UDPAddr
}

// New mirrors StatsClient.__init__(host="127.0.0.1", port=8125,
// tags=None, prefix=""). A nil host or port pointer means the config
// had no such key (Python None) and disables the client; an explicit
// empty host string sends to loopback like Python. tagOrder carries the
// statsd.tags document order captured by the config layer (nil falls
// back to sorted keys). A nil logger discards send-failure logs
// (callers pass the pump logger).
func New(host *string, port *int64, tags map[string]string, tagOrder []string, prefix string, logger Logger) *Client {
	if logger == nil {
		logger = discardLogger{}
	}
	c := &Client{
		tags:   tags,
		prefix: prefix,
		log:    logger,
	}
	if host != nil {
		c.host = *host
	} else {
		c.disabled = true
	}
	if port != nil {
		c.port = int(*port)
	} else {
		c.disabled = true
	}
	c.globalOrder = globalTagOrder(tags, tagOrder)
	return c
}

// globalTagOrder resolves the wire order of the global tags: the
// config-captured document order when available, otherwise sorted keys.
// Keys missing from the captured order, or duplicated in it (defensive:
// the capture and the map decode see the same object, so neither should
// happen — Python's dict holds each key once), are appended/deduped
// accordingly.
func globalTagOrder(tags map[string]string, tagOrder []string) []string {
	if tagOrder == nil {
		order := make([]string, 0, len(tags))
		for key := range tags {
			order = append(order, key)
		}
		sort.Strings(order)
		return order
	}
	seen := make(map[string]bool, len(tags))
	order := make([]string, 0, len(tags))
	for _, key := range tagOrder {
		if _, ok := tags[key]; ok && !seen[key] {
			seen[key] = true
			order = append(order, key)
		}
	}
	var missing []string
	for key := range tags {
		if !seen[key] {
			missing = append(missing, key)
		}
	}
	sort.Strings(missing)
	return append(order, missing...)
}

// Gauge sends a float gauge (Python gauge -> b"g", str(value) with
// Python float repr semantics: 60.0 stays "60.0", 1e-05 stays "1e-05").
// Call sites that pass Python ints (journal.read_lines,
// journal.read_bytes, journal.sent_*, journal.status, the persistent
// gauges) must use GaugeInt instead — Python's str(int) renders "123",
// not "123.0".
func (c *Client) Gauge(metric string, value float64, tags map[string]string) {
	c.send(metric, "g", journal.PythonFloatRepr(value), tags)
}

// GaugeInt sends an integer-valued gauge (Python gauge called with an
// int; str(int) is plain decimal).
func (c *Client) GaugeInt(metric string, value int64, tags map[string]string) {
	c.send(metric, "g", strconv.FormatInt(value, 10), tags)
}

// Increase sends a counter (Python increase -> b"c"; inc_value is an
// int, most call sites use the default 1).
func (c *Client) Increase(metric string, incValue int64, tags map[string]string) {
	c.send(metric, "c", strconv.FormatInt(incValue, 10), tags)
}

// Timing sends a timing (Python timing -> b"ms", float value).
func (c *Client) Timing(metric string, value float64, tags map[string]string) {
	c.send(metric, "ms", journal.PythonFloatRepr(value), tags)
}

// UnexpectedException mirrors statsd.unexpected_exception: it increases
// the "exception" counter with tags exception=<name>, where=<where>
// plus the caller's tags (which override the defaults). Python uses
// ex.__class__.__name__; Go has no exception classes, so the tag
// carries the error's dynamic type name via reflect.TypeOf (e.g.
// "fs.PathError") — a recorded deviation, the metric name and "where"
// tag are identical. A nil error (impossible in Python, where
// ex.__class__ would raise) renders the type name "error" so the
// counter still fires.
func (c *Client) UnexpectedException(ex error, where string, tags map[string]string) {
	exceptionName := "error"
	if ex != nil {
		exceptionName = reflect.TypeOf(ex).String()
	}
	exceptionTags := map[string]string{
		"exception": exceptionName,
		"where":     where,
	}
	for key, value := range tags {
		exceptionTags[key] = value
	}
	// Python's all_tags is a fresh dict in a known order — exception,
	// where, then the caller's keys (statsd.py:38-42) — so unlike the
	// arbitrary per-call maps the insertion order is recoverable and
	// reproduced (extras sorted for determinism).
	ordered := []string{"exception", "where"}
	for key := range tags {
		if key != "exception" && key != "where" {
			ordered = append(ordered, key)
		}
	}
	sort.Strings(ordered[2:])
	c.sendKeys("exception", "c", "1", mergeTagOrder(c.globalOrder, c.tags, ordered), exceptionTags)
}

// Close closes the lazily created UDP socket. Python never closes its
// socket (the process owns it); this exists for the Go lifecycle and
// tests.
func (c *Client) Close() error {
	c.mu.Lock()
	defer c.mu.Unlock()
	if c.conn == nil {
		return nil
	}
	err := c.conn.Close()
	c.conn, c.addr = nil, nil
	return err
}

// send assembles and transmits one datagram. Python builds
// parts = [prefix+metric, ":", str(value), "|", type] and then inserts
// every tag at index 1 (statsd.py:51-55), which REVERSES the merged tag
// order on the wire: with merged tags a,b,c the datagram reads
// "metric,c=..,b=..,a=..:value|type". mergedTagOrder reproduces that
// merged order; the loop below walks it backwards.
func (c *Client) send(metric, metricType, value string, callTags map[string]string) {
	c.sendKeys(metric, metricType, value, mergedTagOrder(c.globalOrder, c.tags, callTags), callTags)
}

// sendKeys assembles and transmits one datagram for the given merged
// (already wire-ordered by the caller's backwards iteration) tag keys.
func (c *Client) sendKeys(metric, metricType, value string, merged []string, callTags map[string]string) {
	if c.disabled {
		return
	}
	tagValue := func(key string) string {
		if v, ok := callTags[key]; ok {
			return v // Python: send_tags.update(tags) — per-call wins
		}
		return c.tags[key]
	}
	b := make([]byte, 0, 64+len(c.prefix)+len(metric))
	b = append(b, c.prefix+metric...)
	for i := len(merged) - 1; i >= 0; i-- {
		key := merged[i]
		b = append(b, ',')
		b = append(b, key...)
		b = append(b, '=')
		b = append(b, tagValue(key)...)
	}
	b = append(b, ':')
	b = append(b, value...)
	b = append(b, '|')
	b = append(b, metricType...)
	c.write(b)
}

// mergedTagOrder reproduces statsd.py's
// `send_tags = self._tags.copy(); send_tags.update(tags or {})`:
// global tags keep their positions and new per-call keys are appended
// (a per-call key already in the global set keeps the global position,
// with the per-call value). The global positions come from the
// config-captured document order (Python's dict order); the appended
// per-call keys are sorted — Go's map[string]string loses the per-call
// insertion order and it is unrecoverable, which is the recorded
// sanctioned deviation (metric values identical; telegraf treats the
// tag list as a set; ordering unobservable in stored data).
func mergedTagOrder(globalOrder []string, global, call map[string]string) []string {
	appended := make([]string, 0, len(call))
	for key := range call {
		appended = append(appended, key)
	}
	sort.Strings(appended)
	return mergeTagOrder(globalOrder, global, appended)
}

// mergeTagOrder merges the global tag positions with an explicitly
// ordered list of per-call keys (a per-call key already in the global
// set keeps the global position, with the per-call value).
func mergeTagOrder(globalOrder []string, global map[string]string, callKeys []string) []string {
	keys := make([]string, 0, len(globalOrder)+len(callKeys))
	keys = append(keys, globalOrder...)
	for _, key := range callKeys {
		if _, ok := global[key]; !ok {
			keys = append(keys, key)
		}
	}
	return keys
}

// write sends the datagram over a single lazily created unconnected
// UDP4 socket — the exact equivalent of Python's AF_INET SOCK_DGRAM
// socket with one sendto per metric (statsd.py:24, 57-58). The udp4
// family makes an IPv6-literal host fail resolution exactly like
// Python's AF_INET getaddrinfo (an OSError subclass, caught below). An
// unconnected socket never surfaces ICMP "connection refused" errors
// the way a connected one would, so sends into a dead collector stay
// silent just like Python's. Any resolution or send error is logged at
// info level with the same message Python uses (statsd.py:59-60).
func (c *Client) write(b []byte) {
	c.mu.Lock()
	defer c.mu.Unlock()
	if c.conn == nil {
		addr, err := net.ResolveUDPAddr("udp4", net.JoinHostPort(c.host, strconv.Itoa(c.port)))
		if err != nil {
			c.log.Printf("Stats UDP send failed: %s", err)
			return
		}
		// Python's sendto(("", port)) targets INADDR_ANY: CPython's
		// setipaddr special-cases the empty host to 0.0.0.0 without
		// calling getaddrinfo, and the kernel delivers locally. Go
		// resolves the empty host to a nil IP, so substitute 0.0.0.0
		// explicitly.
		if addr.IP == nil {
			addr.IP = net.IPv4zero
		}
		conn, err := net.ListenUDP("udp4", nil)
		if err != nil {
			c.log.Printf("Stats UDP send failed: %s", err)
			return
		}
		c.conn, c.addr = conn, addr
	}
	if _, err := c.conn.WriteToUDP(b, c.addr); err != nil {
		c.log.Printf("Stats UDP send failed: %s", err)
	}
}

// GaugeSender is the consumer-side seam the persistent-gauge bookkeeping
// needs; *Client satisfies it via GaugeInt.
type GaugeSender interface {
	GaugeInt(metric string, value int64, tags map[string]string)
}

// GaugeState is one recorded persistent gauge value.
type GaugeState struct {
	Value int64
	Tags  map[string]string
}

// PersistentGauges ports JournalReader.set_persistent_gauge
// (journalpump.py:313-324) and the pump-level refresh_gauges resend
// loop (journalpump.py:1104-1109). One reader owns one of these; the
// pump drains it.
type PersistentGauges struct {
	stats GaugeSender // nil = Python self.stats is None: track, never emit

	mu     sync.Mutex
	gauges map[string]GaugeState
}

// NewPersistentGauges tracks gauges for one consumer; a nil stats
// behaves like Python's stats=None (values are still recorded so the
// transition logic and the pump refresh see them).
func NewPersistentGauges(stats GaugeSender) *PersistentGauges {
	return &PersistentGauges{
		stats:  stats,
		gauges: map[string]GaugeState{},
	}
}

// Set mirrors set_persistent_gauge: the (value, tags) pair is always
// recorded, but a zero is emitted only on the transition from an
// existing non-zero value — "zeros won't be resent by the loop so we
// send them only once" (journalpump.py:318-322). Emissions go out as
// int gauges (Python str(int) wire form).
func (g *PersistentGauges) Set(metric string, value int64, tags map[string]string) {
	g.mu.Lock()
	defer g.mu.Unlock()
	old, existed := g.gauges[metric] // Python: get(metric, (None, None))
	g.gauges[metric] = GaugeState{Value: value, Tags: tags}
	if value == 0 && existed && old.Value != 0 && g.stats != nil {
		g.stats.GaugeInt(metric, value, tags)
	}
}

// Snapshot returns the current (metric -> value, tags) state.
func (g *PersistentGauges) Snapshot() map[string]GaugeState {
	g.mu.Lock()
	defer g.mu.Unlock()
	out := make(map[string]GaugeState, len(g.gauges))
	for metric, state := range g.gauges {
		out[metric] = state
	}
	return out
}

// RefreshNonZero mirrors the pump's refresh_gauges: re-emit every
// persistent gauge whose value is non-zero ("we don't resend zeros as
// 'no errors' is not interesting", journalpump.py:1106-1108). Python
// iterates the dict in insertion order; the datagrams are independent,
// so Go's map order is not observable per metric.
func (g *PersistentGauges) RefreshNonZero() {
	if g.stats == nil {
		return
	}
	for metric, state := range g.Snapshot() {
		if state.Value != 0 {
			g.stats.GaugeInt(metric, state.Value, state.Tags)
		}
	}
}
