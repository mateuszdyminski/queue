package client

// Producer interface of the producer.
type Producer interface {
	// Send sends message to the topic and returns: partition id, offset and error if appears.
	Send(QMsg) (int, int64, error)

	// Close shuts down the producer and flushes any messages it may have buffered.
	Close() error
}

// QMsg holds info about message.
type QMsg struct {
	// Topic the QServer topic. Required.
	Topic string

	// Key message key. Required.
	Key []byte

	// Value. Required.
	Value []byte

	// Partition. Optional. If not set Partitioner is used.
	Partition int32
}


