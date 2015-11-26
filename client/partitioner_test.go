package client

import (
	"testing"
	"fmt"
)

func Test_Partition_roundRobinPartitioner(t *testing.T) {
	// given
	partitions := uint(3)
	res := make([]uint, 0)
	partitioner := NewRoundRobinPartitioner("")

	// when
	for i := 0; i < 4; i++ {
		res = append(res, partitioner.Partition(&QMsg{Key: []byte("test-topic")}, partitions))
	}

	// then
	if res[0] != 0 { t.Errorf("Wrong partition! Got: %d, Expected: %d", res[0], 0)}
	if res[1] != 1 { t.Errorf("Wrong partition! Got: %d, Expected: %d", res[1], 1)}
	if res[2] != 2 { t.Errorf("Wrong partition! Got: %d, Expected: %d", res[2], 2)}
	if res[3] != 0 { t.Errorf("Wrong partition! Got: %d, Expected: %d", res[3], 0)}
}

func Test_Partition_hashPartitioner(t *testing.T) {
	// given
	partitions := uint(3)
	partitioner := NewHashPartitioner("topic")

	// when
	val1 := partitioner.Partition(&QMsg{Key: []byte(fmt.Sprintf("Key-%d", 1))}, partitions)
	val2 := partitioner.Partition(&QMsg{Key: []byte(fmt.Sprintf("Key-%d", 1))}, partitions)
	val3 := partitioner.Partition(&QMsg{Key: []byte(fmt.Sprintf("Key-%d", 2))}, partitions)
	val4 := partitioner.Partition(&QMsg{Key: nil}, partitions)

	// then
	if val1 != val2 {
		t.Errorf("Partitions should be the same!")
	}

	if val1 == val3 {
		t.Errorf("Partitions should not be the same!")
	}

	if val4 < 0 && val4 > 3 - 1 {
		t.Errorf("Value should be between 0 and 2!")
	}
}
