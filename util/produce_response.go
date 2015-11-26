package util

import (
	"bytes"
	"encoding/binary"
	"fmt"
)

//
//ProduceResponse => [TopicName [Partition ErrorCode Offset]]
//TopicName => string
//Partition => int32
//ErrorCode => int16
//Offset => int64

type ProduceResponse struct {
	Topic     *qStr
	Partition uint32
	ErrorCode uint16
	Offset    uint64
}

func NewProduceResponse(topic string, partition uint32, errorCode uint16, offset uint64) *ProduceResponse {
	return &ProduceResponse{
		Topic:     NewQStr(topic),
		Partition: partition,
		ErrorCode: errorCode,
		Offset:    offset,
	}
}

func (r *ProduceResponse) Length() uint32 {
	return uint32(18 + r.Topic.Length())
}

func (r *ProduceResponse) Marshall(buf *bytes.Buffer) error {
	length := uint32(14 + r.Topic.Length())

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

	// encode topic error code
	if err := binary.Write(buf, binary.LittleEndian, r.ErrorCode); err != nil {
		return fmt.Errorf("Can't enconde msg error code. Err: %v", err)
	}

	// encode topic offset
	if err := binary.Write(buf, binary.LittleEndian, r.Offset); err != nil {
		return fmt.Errorf("Can't enconde msg offset. Err: %v", err)
	}

	return nil
}

func (p *ProduceResponse) Unmarshall(buf *bytes.Buffer) error {
	var length int32
	if err := binary.Read(buf, binary.LittleEndian, &length); err != nil {
		return fmt.Errorf("Can't decode ProduceResponse length. Err: %v", err)
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

	// read error code
	if err := binary.Read(buf, binary.LittleEndian, &p.ErrorCode); err != nil {
		return fmt.Errorf("Can't decode msg error code. Err: %v", err)
	}

	// read offset
	if err := binary.Read(buf, binary.LittleEndian, &p.Offset); err != nil {
		return fmt.Errorf("Can't decode msg offset. Err: %v", err)
	}

	return nil
}

func (p *ProduceResponse) Type() MsgType {
	return ProduceResponseType
}