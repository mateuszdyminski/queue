package util

import (
	"bytes"
	"encoding/binary"
	"fmt"
)

//MetadataResponse => [TopicName []Partition]
//TopicName => string
//Partitions => []int32

type MetadataResponse struct {
	Topic      *qStr
	Partitions *qIntArr
}

func NewMetadataResponse(topic string, partitions []int) *MetadataResponse {
	return &MetadataResponse{
		Topic:      NewQStr(topic),
		Partitions: NewQIntArr(partitions),
	}
}

func (r *MetadataResponse) Length() uint32 {
	return uint32(4 + r.Topic.Length() + r.Partitions.Length())
}

func (r *MetadataResponse) Marshall(buf *bytes.Buffer) error {
	length := uint32(r.Topic.Length() + r.Partitions.Length())

	// encode message length
	if err := binary.Write(buf, binary.LittleEndian, &length); err != nil {
		return fmt.Errorf("Can't enconde msg length. Err: %v", err)
	}

	// encode message topic
	if err := r.Topic.Marshall(buf); err != nil {
		return fmt.Errorf("Can't enconde msg topic. Err: %v", err)
	}

	// encode partitions
	if err := r.Partitions.Marshall(buf); err != nil {
		return fmt.Errorf("Can't enconde partitions. Err: %v", err)
	}

	return nil
}

func (p *MetadataResponse) Unmarshall(buf *bytes.Buffer) error {
	var length int32
	if err := binary.Read(buf, binary.LittleEndian, &length); err != nil {
		return fmt.Errorf("Can't decode ProduceResponse length. Err: %v", err)
	}

	// read topic
	p.Topic = new(qStr)
	if err := UnmarshallQStr(buf, p.Topic); err != nil {
		return fmt.Errorf("Can't decode msg topic. Err: %v", err)
	}

	// read topic
	p.Partitions = new(qIntArr)
	if err := UnmarshallQIntArr(buf, p.Partitions); err != nil {
		return fmt.Errorf("Can't decode partitions. Err: %v", err)
	}

	return nil
}

func (p *MetadataResponse) Type() MsgType {
	return MetadataResponseType
}