// Copyright 2026, Aiven, https://aiven.io/
//
// This file is under the Apache License, Version 2.0.
// See the file `LICENSE` for details.

//go:build systest

package systestgo

import (
	"encoding/base64"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"os"
	"os/exec"
	"path/filepath"
	"strconv"
	"strings"
	"syscall"
	"testing"
	"time"

	"github.com/IBM/sarama"
)

// kafkaFixture mirrors the committed capture of
// systest/test_groundtruth_kafka.py (Task 12 ground truth): the delivered
// payload bytes, key and partition placement the Go sender must
// reproduce on a real broker.
type kafkaFixture struct {
	Compression string `json:"compression"`
	Capture     struct {
		Runs []struct {
			Mode             string         `json:"mode"`
			KafkaMsgKey      *string        `json:"kafka_msg_key"`
			KafkaTopicConfig map[string]any `json:"kafka_topic_config"`
		} `json:"runs"`
	} `json:"capture"`
	Expected struct {
		AutoCreate struct {
			Key          *string  `json:"key"`
			NumPartition int      `json:"num_partitions"`
			Partition    int32    `json:"partition"`
			Payloads     []string `json:"payloads"`
			SentCount    int64    `json:"sent_count"`
			SentCursor   string   `json:"sent_cursor"`
		} `json:"auto_create"`
		ManualCreateKeyed struct {
			Key          *string  `json:"key"`
			NumPartition int      `json:"num_partitions"`
			Partition    int32    `json:"partition"`
			Payloads     []string `json:"payloads"`
			SentCount    int64    `json:"sent_count"`
			SentCursor   string   `json:"sent_cursor"`
		} `json:"manual_create_keyed"`
	} `json:"expected"`
}

// TestKafkaSenderEndToEnd runs the REAL Go binary against a real Kafka
// broker and asserts the Task 12 ground truth: the delivered payload
// bytes, the message key and the partition placement for both the
// auto-create and the admin-create/keyed scenarios.
//
// Opt-in: a real broker is required, so the test SKIPS unless
// JOURNALPUMP_KAFKA_BROKER points at one (the Go analogue of the Python
// harness's JOURNALPUMP_KAFKA_BROKER override). Provision one
// reproducibly with scripts/kafka-e2e-go (make systest-go-kafka), which
// starts a throwaway docker.io/apache/kafka:4.0.0 KRaft broker on podman.
// CI does NOT run this yet (the .github/workflows/build-go.yml job is
// lint + unit only); a mandatory broker gate is plan Task 30. The default
// systest gate therefore stays broker-free.
func TestKafkaSenderEndToEnd(t *testing.T) {
	broker := os.Getenv("JOURNALPUMP_KAFKA_BROKER")
	if broker == "" {
		t.Skip("JOURNALPUMP_KAFKA_BROKER not set; skipping real-broker kafka e2e")
	}
	repoRoot, err := filepath.Abs("..")
	if err != nil {
		t.Fatalf("repo root: %v", err)
	}
	rawFixture, err := os.ReadFile(filepath.Join(repoRoot, "systest-go", "fixtures", "kafka.json"))
	if err != nil {
		t.Fatalf("read fixture: %v", err)
	}
	var fixture kafkaFixture
	if err := json.Unmarshal(rawFixture, &fixture); err != nil {
		t.Fatalf("parse fixture: %v", err)
	}
	if fixture.Compression != "snappy" {
		t.Fatalf("fixture compression = %q, want snappy", fixture.Compression)
	}

	logsDir := stageFixtureJournals(t, repoRoot)

	for _, tc := range []struct {
		name         string
		key          *string
		topicConfig  map[string]any
		partition    int32
		numPartition int
		payloads     []string
		sentCount    int64
		sentCursor   string
	}{
		{
			name:         "auto-create",
			key:          fixture.Expected.AutoCreate.Key,
			topicConfig:  nil,
			partition:    fixture.Expected.AutoCreate.Partition,
			numPartition: fixture.Expected.AutoCreate.NumPartition,
			payloads:     fixture.Expected.AutoCreate.Payloads,
			sentCount:    fixture.Expected.AutoCreate.SentCount,
			sentCursor:   fixture.Expected.AutoCreate.SentCursor,
		},
		{
			name:         "manual-create-keyed",
			key:          fixture.Expected.ManualCreateKeyed.Key,
			topicConfig:  map[string]any{"num_partitions": 2, "replication_factor": 1},
			partition:    fixture.Expected.ManualCreateKeyed.Partition,
			numPartition: fixture.Expected.ManualCreateKeyed.NumPartition,
			payloads:     fixture.Expected.ManualCreateKeyed.Payloads,
			sentCount:    fixture.Expected.ManualCreateKeyed.SentCount,
			sentCursor:   fixture.Expected.ManualCreateKeyed.SentCursor,
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			topic := fmt.Sprintf("journalpump-go-e2e-%s-%d", strings.ReplaceAll(tc.name, "-", ""), time.Now().UnixNano())
			runKafkaScenario(t, broker, repoRoot, logsDir, topic, tc.key, tc.topicConfig, tc.partition, tc.numPartition, tc.payloads, tc.sentCount, tc.sentCursor)
		})
	}
}

// TestKafkaPythonConsumerInterop is the committed snappy interop regression
// (spec risk 10 / charter D5): the real Go sender produces snappy records to
// a fresh 2-partition topic, and the DEPLOYED kafka-python client — the same
// consumer library Python journalpump used — reads them back with intact
// payload bytes and key, matching the Task 12 fixture ground truth.
//
// Opt-in like TestKafkaSenderEndToEnd (skips without JOURNALPUMP_KAFKA_BROKER)
// and additionally skips when python3 with the kafka module is unavailable.
// The child is built with -race; a race report or a non-zero exit fails the
// test, and shutdown is bounded.
func TestKafkaPythonConsumerInterop(t *testing.T) {
	broker := os.Getenv("JOURNALPUMP_KAFKA_BROKER")
	if broker == "" {
		t.Skip("JOURNALPUMP_KAFKA_BROKER not set; skipping real-broker kafka python interop")
	}
	python := pythonWithKafka(t)
	repoRoot, err := filepath.Abs("..")
	if err != nil {
		t.Fatalf("repo root: %v", err)
	}
	rawFixture, err := os.ReadFile(filepath.Join(repoRoot, "systest-go", "fixtures", "kafka.json"))
	if err != nil {
		t.Fatalf("read fixture: %v", err)
	}
	var fixture kafkaFixture
	if err := json.Unmarshal(rawFixture, &fixture); err != nil {
		t.Fatalf("parse fixture: %v", err)
	}
	want := fixture.Expected.ManualCreateKeyed
	if want.Key == nil {
		t.Fatal("fixture manual_create_keyed.key is nil")
	}

	logsDir := stageFixtureJournals(t, repoRoot)
	topic := fmt.Sprintf("journalpump-go-interop-%d", time.Now().UnixNano())
	key := *want.Key
	configPath, _ := writeSenderConfig(t, broker, logsDir, topic, &key,
		map[string]any{"num_partitions": 2, "replication_factor": 1})

	cmd, stderr := startBinary(t, configPath)
	records, err := consumeWithKafkaPython(t, python, repoRoot, broker, topic, want.NumPartition, len(want.Payloads))
	if err != nil {
		t.Fatalf("kafka-python consume: %v; stderr:\n%s", err, stderr.String())
	}
	terminateAndCheck(t, cmd, stderr)

	if got := len(records); got != len(want.Payloads) {
		t.Fatalf("records = %d, want %d", got, len(want.Payloads))
	}
	for i, rec := range records {
		if rec.Partition != want.Partition {
			t.Errorf("record %d partition = %d, want %d", i, rec.Partition, want.Partition)
		}
		gotKey, err := hex.DecodeString(rec.KeyHex)
		if err != nil {
			t.Fatalf("record %d key hex: %v", i, err)
		}
		if string(gotKey) != key {
			t.Errorf("record %d key = %q, want %q", i, gotKey, key)
		}
		gotValue, err := base64.StdEncoding.DecodeString(rec.ValueB64)
		if err != nil {
			t.Fatalf("record %d value base64: %v", i, err)
		}
		if string(gotValue) != want.Payloads[i] {
			t.Errorf("payload %d mismatch:\n got %s\nwant %s", i, gotValue, want.Payloads[i])
		}
	}
}

// pythonWithKafka returns the python3 interpreter when it can import
// kafka, else skips the test. JOURNALPUMP_PYTHON overrides the interpreter.
func pythonWithKafka(t *testing.T) string {
	t.Helper()
	python := os.Getenv("JOURNALPUMP_PYTHON")
	if python == "" {
		python = "python3"
	}
	if err := exec.Command(python, "-c", "import kafka").Run(); err != nil {
		t.Skipf("%s cannot import kafka; skipping kafka-python interop", python)
	}
	return python
}

// kafkaPythonRecord mirrors one record of the consumer script's JSON output.
type kafkaPythonRecord struct {
	Partition int32  `json:"partition"`
	Offset    int64  `json:"offset"`
	KeyHex    string `json:"key_hex"`
	ValueB64  string `json:"value_b64"`
}

// consumeWithKafkaPython runs the committed kafka-python consumer script
// against the topic and returns the records it read, requiring the broker's
// topic to have exactly wantPartitions partitions.
func consumeWithKafkaPython(t *testing.T, python, repoRoot, broker, topic string, wantPartitions, count int) ([]kafkaPythonRecord, error) {
	t.Helper()
	script := filepath.Join(repoRoot, "systest-go", "kafka_python_consumer.py")
	out, err := exec.Command(python, script, broker, topic,
		strconv.Itoa(wantPartitions), strconv.Itoa(count), "60").Output()
	if err != nil {
		if ee, ok := err.(*exec.ExitError); ok {
			return nil, fmt.Errorf("consumer script: %w: %s", err, ee.Stderr)
		}
		return nil, err
	}
	var result struct {
		Partitions         int                 `json:"partitions"`
		ExpectedPartitions int                 `json:"expected_partitions"`
		Records            []kafkaPythonRecord `json:"records"`
	}
	if err := json.Unmarshal(out, &result); err != nil {
		return nil, fmt.Errorf("parse consumer output %q: %w", out, err)
	}
	if result.Partitions != wantPartitions {
		return nil, fmt.Errorf("broker topic has %d partitions, want exactly %d", result.Partitions, wantPartitions)
	}
	return result.Records, nil
}

// stageFixtureJournals copies the fixture journals into a temp dir,
// shared by every kafka scenario.
func stageFixtureJournals(t *testing.T, repoRoot string) string {
	t.Helper()
	logsDir := filepath.Join(t.TempDir(), "logs")
	if err := os.MkdirAll(logsDir, 0o755); err != nil {
		t.Fatalf("mkdir logs: %v", err)
	}
	journals, err := filepath.Glob(filepath.Join(repoRoot, "systest", "data", "rotated_logs", "*.journal"))
	if err != nil || len(journals) == 0 {
		t.Fatalf("glob journals: %v (%d)", err, len(journals))
	}
	for _, journal := range journals {
		data, err := os.ReadFile(journal)
		if err != nil {
			t.Fatalf("read journal: %v", err)
		}
		if err := os.WriteFile(filepath.Join(logsDir, filepath.Base(journal)), data, 0o644); err != nil {
			t.Fatalf("stage journal: %v", err)
		}
	}
	return logsDir
}

// writeSenderConfig writes the journalpump config for one kafka scenario and
// returns its path plus the state-file path the run writes.
func writeSenderConfig(t *testing.T, broker, logsDir, topic string, key *string, topicConfig map[string]any) (configPath, statePath string) {
	t.Helper()
	statePath = filepath.Join(t.TempDir(), "state.json")
	sender := map[string]any{
		"output_type":   "kafka",
		"kafka_address": broker,
		"kafka_topic":   topic,
	}
	if key != nil {
		sender["kafka_msg_key"] = *key
	}
	if topicConfig != nil {
		sender["kafka_topic_config"] = topicConfig
	}
	config := map[string]any{
		"readers": map[string]any{
			"my-reader": map[string]any{
				"journal_path":     logsDir,
				"initial_position": "head",
				"senders":          map[string]any{"out": sender},
			},
		},
		"json_state_file_path": statePath,
	}
	configPath = filepath.Join(t.TempDir(), "journalpump.json")
	configBytes, err := json.Marshal(config)
	if err != nil {
		t.Fatalf("marshal config: %v", err)
	}
	if err := os.WriteFile(configPath, configBytes, 0o644); err != nil {
		t.Fatalf("write config: %v", err)
	}
	return configPath, statePath
}

// startBinary starts the -race journalpump binary for a config, returning the
// command and its captured stderr.
func startBinary(t *testing.T, configPath string) (*exec.Cmd, *strings.Builder) {
	t.Helper()
	cmd := exec.Command(goBinaryPath, configPath)
	cmd.Env = append(os.Environ(), "JOURNALPUMP_FILES_AVAILABLE=1")
	stderr := &strings.Builder{}
	cmd.Stderr = stderr
	if err := cmd.Start(); err != nil {
		t.Fatalf("start binary: %v", err)
	}
	t.Cleanup(func() {
		if cmd.ProcessState == nil {
			_ = cmd.Process.Kill()
			_, _ = cmd.Process.Wait()
		}
	})
	return cmd, stderr
}

// terminateAndCheck sends SIGTERM and requires a bounded, successful exit
// with no race-detector report (the binary is built with -race).
func terminateAndCheck(t *testing.T, cmd *exec.Cmd, stderr *strings.Builder) {
	t.Helper()
	if err := cmd.Process.Signal(syscall.SIGTERM); err != nil {
		t.Fatalf("SIGTERM: %v", err)
	}
	done := make(chan error, 1)
	go func() { done <- cmd.Wait() }()
	select {
	case err := <-done:
		if err != nil {
			t.Fatalf("binary exited with error: %v; stderr:\n%s", err, stderr.String())
		}
	case <-time.After(30 * time.Second):
		_ = cmd.Process.Kill()
		<-done
		t.Fatalf("binary did not exit within 30s of SIGTERM; stderr:\n%s", stderr.String())
	}
	out := stderr.String()
	if strings.Contains(out, "WARNING: DATA RACE") || strings.Contains(out, "DATA RACE") {
		t.Fatalf("child reported a data race; stderr:\n%s", out)
	}
}

// runKafkaScenario drives one sender configuration against the broker
// and asserts the fixture's payload/key/partition/sent ground truth.
func runKafkaScenario(
	t *testing.T,
	broker, _ string,
	logsDir, topic string,
	key *string,
	topicConfig map[string]any,
	wantPartition int32,
	wantPartitions int,
	wantPayloads []string,
	wantSentCount int64,
	wantSentCursor string,
) {
	t.Helper()
	configPath, statePath := writeSenderConfig(t, broker, logsDir, topic, key, topicConfig)

	cmd, stderr := startBinary(t, configPath)

	records, err := consumeKafka(t, broker, topic, wantPartitions, len(wantPayloads))
	if err != nil {
		t.Fatalf("consume: %v; stderr:\n%s", err, stderr.String())
	}
	terminateAndCheck(t, cmd, stderr)

	// Every record lands on the single deterministic partition.
	for _, rec := range records {
		if rec.partition != wantPartition {
			t.Errorf("record partition = %d, want %d", rec.partition, wantPartition)
		}
	}
	gotPayloads := make([]string, len(records))
	for i, rec := range records {
		if key == nil {
			if rec.key != nil {
				t.Errorf("record %d key = %q, want none", i, rec.key)
			}
		} else if string(rec.key) != *key {
			t.Errorf("record %d key = %q, want %q", i, rec.key, *key)
		}
		gotPayloads[i] = string(rec.value)
	}
	if len(gotPayloads) != len(wantPayloads) {
		t.Fatalf("records = %d, want %d", len(gotPayloads), len(wantPayloads))
	}
	for i, want := range wantPayloads {
		if gotPayloads[i] != want {
			t.Errorf("payload %d mismatch:\n got %s\nwant %s", i, gotPayloads[i], want)
		}
	}

	stateBytes, err := os.ReadFile(statePath)
	if err != nil {
		t.Fatalf("read state: %v", err)
	}
	var st stateFile
	if err := json.Unmarshal(stateBytes, &st); err != nil {
		t.Fatalf("parse state: %v", err)
	}
	sent := st.Readers["my-reader"].Senders["out"].Sent
	if sent.Count != wantSentCount {
		t.Errorf("state sent.count = %d, want %d", sent.Count, wantSentCount)
	}
	if sent.Cursor != wantSentCursor {
		t.Errorf("state sent.cursor = %q, want %q", sent.Cursor, wantSentCursor)
	}
}

// kafkaRecord is one consumed record in (partition, offset) order.
type kafkaRecord struct {
	partition int32
	offset    int64
	key       []byte
	value     []byte
}

// consumeKafka reads the topic from the earliest offsets until the
// expected record count arrives or the deadline passes.
func consumeKafka(t *testing.T, broker, topic string, partitions, want int) ([]kafkaRecord, error) {
	t.Helper()
	cfg := sarama.NewConfig()
	cfg.Version = sarama.V2_5_0_0
	cfg.Consumer.Offsets.Initial = sarama.OffsetOldest
	// CRITICAL: the consumer's metadata requests must NOT auto-create the
	// topic. On an auto-create broker, a subscribe/metadata request for a
	// not-yet-existing topic creates it with the DEFAULT partition count
	// and would race (and beat) the sender's own admin create_topics — the
	// exact race the Task 12 harness documents. With this disabled the
	// consumer waits for the sender's topic (and its partition count).
	cfg.Metadata.AllowAutoTopicCreation = false
	consumer, err := sarama.NewConsumer([]string{broker}, cfg)
	if err != nil {
		return nil, fmt.Errorf("new consumer: %w", err)
	}
	defer func() { _ = consumer.Close() }()

	var records []kafkaRecord
	deadline := time.Now().Add(60 * time.Second)
	// Wait for the producer to create the topic (the admin scenario
	// creates it), then read every partition from the earliest offsets.
	// Each outer pass re-reads from the beginning and replaces the
	// accumulated set, so a partially-populated topic does not duplicate
	// records across passes.
	for len(records) < want && time.Now().Before(deadline) {
		partitionsList, perr := consumer.Partitions(topic)
		// Require EXACTLY the fixture's partition count: a larger count
		// means the broker auto-created the topic (or the sender's admin
		// create raced the consumer), which the fixture forbids.
		if perr != nil || len(partitionsList) != partitions {
			time.Sleep(500 * time.Millisecond)
			continue
		}
		records = records[:0]
		for _, p := range partitionsList {
			pc, err := consumer.ConsumePartition(topic, p, sarama.OffsetOldest)
			if err != nil {
				continue
			}
			drain := time.NewTimer(2 * time.Second)
			collecting := true
			for collecting {
				select {
				case msg, ok := <-pc.Messages():
					if !ok {
						collecting = false
						continue
					}
					records = append(records, kafkaRecord{partition: msg.Partition, offset: msg.Offset, key: msg.Key, value: msg.Value})
				case <-drain.C:
					collecting = false
				}
			}
			drain.Stop()
			_ = pc.Close()
		}
		if len(records) < want {
			time.Sleep(500 * time.Millisecond)
		}
	}
	// sort by (partition, offset)
	for i := 1; i < len(records); i++ {
		for j := i; j > 0 && less(records[j], records[j-1]); j-- {
			records[j], records[j-1] = records[j-1], records[j]
		}
	}
	if len(records) != want {
		return records, fmt.Errorf("consumed %d records, want %d", len(records), want)
	}
	return records, nil
}

func less(a, b kafkaRecord) bool {
	if a.partition != b.partition {
		return a.partition < b.partition
	}
	return a.offset < b.offset
}
