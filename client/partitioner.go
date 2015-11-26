package client

import (
	"hash"
	"hash/fnv"
	"math/rand"
	"time"
)

// Partitioner interface with one func Partition which calculates the partition for specified topic.
type Partitioner interface {
	Partition(*QMsg, int) int
}

// PartitionerConstructor is the type for a function capable of constructing new Partitioners.
type PartitionerConstructor func(topic string) Partitioner

// NewRoundRobinPartitioner returns a round robin Partitioner.
func NewRoundRobinPartitioner(topic string) Partitioner {
	return &roundRobinPartitioner{}
}

// roundRobinPartitioner round robin implementation of Partitioner.
type roundRobinPartitioner struct {
	last int
}

// Partition returns next partitionId when last element is reached it starts again from 1.
func (p *roundRobinPartitioner) Partition(msg *QMsg, noOfPartitions int) int {
	if p.last >= noOfPartitions {
		p.last = 0
	}

	val := p.last
	p.last++
	return val
}

// NewRoundRobinPartitioner returns a hash Partitioner.
func NewHashPartitioner(topic string) Partitioner {
	return &hashPartitioner{
		topic:  topic,
		hasher: fnv.New32a(),
		random: rand.New(rand.NewSource(time.Now().UTC().UnixNano())),
	}
}

type hashPartitioner struct {
	topic  string
	hasher hash.Hash32
	random *rand.Rand
}

// Partition returns next partitionId based on hash.
func (p *hashPartitioner) Partition(msg *QMsg, noOfPartitions int) int {
	if msg.Key == nil {
		return p.random.Intn(noOfPartitions)
	}

	p.hasher.Write(msg.Key)
	sum := p.hasher.Sum32()
	p.hasher.Reset()

	return int(sum) % noOfPartitions
}
