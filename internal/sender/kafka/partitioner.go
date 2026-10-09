// Copyright 2026, Aiven, https://aiven.io
//
// This file is under the Apache License, Version 2.0.
// See the file `LICENSE` for details.

package kafka

import (
	"math/rand"

	"github.com/IBM/sarama"
)

// murmur2Partitioner reproduces kafka-python's DefaultPartitioner
// (kafka/partitioner/default.py) exactly: a keyed message is routed with
// the Java-client murmur2 hash, and an unkeyed message picks a random
// partition. sarama's own NewReferenceHashPartitioner is NOT equivalent
// — it hashes with FNV-1a, so keyed records land on different partitions
// than the Python sender and cross-cutover data routing would diverge
// (review item A1). The sender therefore installs this partitioner.
//
// kafka-python:
//
//	idx = murmur2(key)
//	idx &= 0x7fffffff
//	idx %= len(all_partitions)
//	return all_partitions[idx]
//
// all_partitions is sorted by id, so the index is the partition id.
type murmur2Partitioner struct {
	topic string
	// consistency is RequiresConsistency: true when the sender configures
	// a fixed kafka_msg_key (stable keyed routing), false otherwise so
	// sarama does not gate on every partition having a leader when the
	// Python sender would simply pick a random available one.
	consistency bool
}

func (p *murmur2Partitioner) Partition(msg *sarama.ProducerMessage, numPartitions int32) (int32, error) {
	if numPartitions <= 0 {
		return 0, nil
	}
	if msg.Key == nil {
		// kafka-python with key None: random.choice(available or all).
		return rand.Int31n(numPartitions), nil
	}
	key, err := msg.Key.Encode()
	if err != nil {
		return 0, err
	}
	idx := murmur2(key) & 0x7fffffff
	return int32(idx % uint32(numPartitions)), nil
}

func (p *murmur2Partitioner) RequiresConsistency() bool { return p.consistency }

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
