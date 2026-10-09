// Copyright 2026, Aiven, https://aiven.io
//
// This file is under the Apache License, Version 2.0.
// See the file `LICENSE` for details.

package kafka

import (
	"testing"

	"github.com/IBM/sarama"
)

// TestMurmur2MatchesKafkaPython pins murmur2 against values computed by
// kafka-python 2.3.2's kafka/partitioner/default.py:murmur2 (review item
// A1). The expected hashes were produced by running the reference
// implementation; a regression here means keyed records would route
// differently than the Python sender.
func TestMurmur2MatchesKafkaPython(t *testing.T) {
	cases := []struct {
		key  string
		want uint32
	}{
		{"journalpump", 156424754},
		{"", 275646681},
		{"a", 2731586172},
		{"abc", 479470107},
		{"hostname.example.org", 2776664639},
		{"topic-key-123", 590263568},
		{"\x00\x01\x02\x03\xff\xfe", 734238927},
		{"café", 2936959922},
		{"kkkkkkkkkkkkkkkkk", 842010292},
	}
	for _, tc := range cases {
		if got := murmur2([]byte(tc.key)); got != tc.want {
			t.Errorf("murmur2(%q) = %d, want %d", tc.key, got, tc.want)
		}
	}
}

// TestPartitionerMatchesKafkaPythonDefaultPartitioner verifies the
// partition index for multiple keys across several partition counts
// against kafka-python's DefaultPartitioner (also item A1). The expected
// indices are kafka-python's output for partitions 0..n-1.
func TestPartitionerMatchesKafkaPythonDefaultPartitioner(t *testing.T) {
	counts := []int32{1, 2, 3, 4, 8, 16}
	keys := []struct {
		key    string
		wantBy map[int32]int32
	}{
		{"journalpump", map[int32]int32{1: 0, 2: 0, 3: 2, 4: 2, 8: 2, 16: 2}},
		{"", map[int32]int32{1: 0, 2: 1, 3: 0, 4: 1, 8: 1, 16: 9}},
		{"a", map[int32]int32{1: 0, 2: 0, 3: 1, 4: 0, 8: 4, 16: 12}},
		{"abc", map[int32]int32{1: 0, 2: 1, 3: 0, 4: 3, 8: 3, 16: 11}},
		{"hostname.example.org", map[int32]int32{1: 0, 2: 1, 3: 0, 4: 3, 8: 7, 16: 15}},
		{"topic-key-123", map[int32]int32{1: 0, 2: 0, 3: 2, 4: 0, 8: 0, 16: 0}},
		{"\x00\x01\x02\x03\xff\xfe", map[int32]int32{1: 0, 2: 1, 3: 0, 4: 3, 8: 7, 16: 15}},
		{"café", map[int32]int32{1: 0, 2: 0, 3: 0, 4: 2, 8: 2, 16: 2}},
		{"kkkkkkkkkkkkkkkkk", map[int32]int32{1: 0, 2: 0, 3: 1, 4: 0, 8: 4, 16: 4}},
	}
	for _, tc := range keys {
		for _, n := range counts {
			p := &murmur2Partitioner{topic: "t", consistency: true}
			msg := &sarama.ProducerMessage{Topic: "t", Key: sarama.ByteEncoder(tc.key)}
			got, err := p.Partition(msg, n)
			if err != nil {
				t.Fatalf("Partition(%q, %d): %v", tc.key, n, err)
			}
			if want := tc.wantBy[n]; got != want {
				t.Errorf("key %q, %d partitions: got %d, want %d", tc.key, n, got, want)
			}
		}
	}
}

// TestPartitionerUnkeyedPicksValidPartition covers the key=None branch:
// kafka-python draws a random partition in [0, numPartitions).
func TestPartitionerUnkeyedPicksValidPartition(t *testing.T) {
	p := &murmur2Partitioner{topic: "t"}
	msg := &sarama.ProducerMessage{Topic: "t"}
	for i := 0; i < 200; i++ {
		got, err := p.Partition(msg, 5)
		if err != nil {
			t.Fatalf("Partition: %v", err)
		}
		if got < 0 || got >= 5 {
			t.Fatalf("unkeyed partition %d out of range [0,5)", got)
		}
	}
}

// TestPartitionerConsistencyReflectsKeying: a keyed sender needs stable
// routing (RequiresConsistency true); an unkeyed one does not.
func TestPartitionerConsistencyReflectsKeying(t *testing.T) {
	if !(&murmur2Partitioner{consistency: true}).RequiresConsistency() {
		t.Error("keyed partitioner must require consistency")
	}
	if (&murmur2Partitioner{consistency: false}).RequiresConsistency() {
		t.Error("unkeyed partitioner must not require consistency")
	}
}
