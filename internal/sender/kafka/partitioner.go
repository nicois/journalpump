// Copyright 2026, Aiven, https://aiven.io
//
// This file is under the Apache License, Version 2.0.
// See the file `LICENSE` for details.

package kafka

import (
	"math/rand"

	"github.com/twmb/franz-go/pkg/kgo"
)

// murmur2Partitioner reproduces kafka-python's DefaultPartitioner
// (kafka/partitioner/default.py) exactly: a keyed message is routed with
// the Java-client murmur2 hash, and an unkeyed message picks a random
// partition. franz-go's default UniformBytesPartitioner uses the same
// Java-compatible murmur2 for keyed records, but for unkeyed records it is
// sticky-per-batch (Java semantics), NOT the Python sender's
// random.choice. Keeping our partitioner preserves both behaviours
// byte-for-byte (review item A1).
//
// kafka-python:
//
//	idx = murmur2(key)
//	idx &= 0x7fffffff
//	idx %= len(all_partitions)
//	return all_partitions[idx]
//
// all_partitions is sorted by id, so the index is the partition id.
//
// The keyed/random decision is a client-wide config (whether a fixed
// kafka_msg_key is set), so the top-level Partitioner returns a
// per-topic partitioner carrying that one bit.
type murmur2Partitioner struct {
	// keyed is true when the sender configures a fixed kafka_msg_key
	// (stable keyed routing): it becomes RequiresConsistency, so franz-go
	// waits for a metadata view covering every partition. An unkeyed
	// sender reports false and franz-go only requires the partitions it
	// would pick.
	keyed bool
}

func (p murmur2Partitioner) ForTopic(string) kgo.TopicPartitioner {
	return murmur2TopicPartitioner{consistency: p.keyed}
}

type murmur2TopicPartitioner struct {
	consistency bool
}

func (p murmur2TopicPartitioner) RequiresConsistency(*kgo.Record) bool { return p.consistency }

func (p murmur2TopicPartitioner) Partition(rec *kgo.Record, numPartitions int) int {
	if numPartitions <= 0 {
		return 0
	}
	if rec.Key == nil {
		// kafka-python with key None: random.choice(available or all).
		return int(rand.Int31n(int32(numPartitions)))
	}
	idx := murmur2(rec.Key) & 0x7fffffff
	return int(idx % uint32(numPartitions))
}

// murmur2 is the Java client's MurmurHash2 (org.apache.kafka.common.utils.
// Utils.murmur2) as reimplemented by kafka-python's partitioner.py: seed
// 0x9747b28c, m 0x5bd1e995, r 24, little-endian 4-byte blocks, a
// cumulative tail, and the three final mixing rounds. All arithmetic is
// modulo 2**32; Go's uint32 overflow reproduces the Python masking.
func murmur2(data []byte) uint32 {
	const (
		seed uint32 = 0x9747b28c
		m    uint32 = 0x5bd1e995
		r           = 24
	)
	length := len(data)
	h := seed ^ uint32(length)
	length4 := length / 4
	for i := 0; i < length4; i++ {
		i4 := i * 4
		k := uint32(data[i4]) |
			uint32(data[i4+1])<<8 |
			uint32(data[i4+2])<<16 |
			uint32(data[i4+3])<<24
		k *= m
		k ^= k >> r
		k *= m
		h *= m
		h ^= k
	}
	// Tail: Python applies the >=3, >=2, >=1 branches cumulatively.
	base := length4 * 4
	switch length % 4 {
	case 3:
		h ^= uint32(data[base+2]) << 16
		fallthrough
	case 2:
		h ^= uint32(data[base+1]) << 8
		fallthrough
	case 1:
		h ^= uint32(data[base])
		h *= m
	}
	h ^= h >> 13
	h *= m
	h ^= h >> 15
	return h
}
