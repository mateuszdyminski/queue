package client

import "sync"

type metadata struct {
	topics map[string]topicInfo

	m *sync.Mutex
}

type topicInfo struct {
	name string
	partitions []int
}