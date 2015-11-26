package util

import (
	"encoding/binary"
	"fmt"
	"bytes"
)

type ProduceRequest struct {
	// Topic the queue topic. Required.
	Topic *qStr

	// Partition partition of the topic. Required.
	Partition uint32

	// Key message key. Required.
	Key *qBytes

	// Value. Required.
	Value *qBytes
}

func NewProduceRequest(topic string, partition int, key, value []byte) *ProduceRequest {
	return &ProduceRequest{
		Topic: NewQStr(topic),
		Partition: uint32(partition),
		Key: NewQBytes(key),
		Value: NewQBytes(value),
	}
}

func (r *ProduceRequest) Length() uint32 {
	return uint32(8 + r.Topic.Length() + r.Value.Length() + r.Key.Length())
}

func (r *ProduceRequest) Marshall(buf *bytes.Buffer) (error) {
	length := uint32(4 + r.Topic.Length() + r.Value.Length() + r.Key.Length())

	// encode message length
	if err := binary.Write(buf, binary.LittleEndian, &length); err != nil {
		return fmt.Errorf("Can't enconde msg length. Err: %v", err)
	}

	// encode message topic
	if err := r.Topic.Marshall(buf); err != nil {
		return fmt.Errorf("Can't enconde msg topic. Err: %v", err)
	}

	// encode topic partition
	if err := binary.Write(buf, binary.LittleEndian, r.Partition); err != nil {
		return fmt.Errorf("Can't enconde msg partition. Err: %v", err)
	}

	// encode message key
	if err := r.Key.Marshall(buf); err != nil {
		return fmt.Errorf("Can't enconde msg key. Err: %v", err)
	}

	// encode message value
	if err := r.Value.Marshall(buf); err != nil {
		return fmt.Errorf("Can't enconde msg value. Err: %v", err)
	}

	return nil
}

func (p *ProduceRequest) Unmarshall(buf *bytes.Buffer) error {
	var length int32
	if err := binary.Read(buf, binary.LittleEndian, &length); err != nil {
		return fmt.Errorf("Can't decode ProduceRequest length. Err: %v", err)
	}

	// read topic
	p.Topic = new(qStr)
	if err := UnmarshallQStr(buf, p.Topic); err != nil {
		return fmt.Errorf("Can't decode msg topic. Err: %v", err)
	}

	// read partition
	if err := binary.Read(buf, binary.LittleEndian, &p.Partition); err != nil {
		return fmt.Errorf("Can't decode msg partition. Err: %v", err)
	}

	// read key
	p.Key = new(qBytes)
	if err := UnmarshallQBytes(buf, p.Key); err != nil {
		return fmt.Errorf("Can't decode msg key. Err: %v", err)
	}

	// read value
	p.Value = new(qBytes)
	if err := UnmarshallQBytes(buf, p.Value); err != nil {
		return fmt.Errorf("Can't decode msg value. Err: %v", err)
	}

	return nil
}

func (p *ProduceRequest) Type() MsgType {
	return ProduceRequestType
}
