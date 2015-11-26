package util

import (
	"bytes"
	"encoding/binary"
	"fmt"
)

type MetadataRequest struct {
	Topic *qStr
	typ MsgType
}

func NewMetadataRequest(topic string) *MetadataRequest {
	return &MetadataRequest{Topic: NewQStr(topic), typ: MetadataRequestType}
}

func (r *MetadataRequest) Length() uint32 {
	return uint32(2 + r.Topic.Length())
}

func (r *MetadataRequest) Marshall(buf *bytes.Buffer) (error) {
	length := uint32(r.Topic.Length())

	// encode message length
	if err := binary.Write(buf, binary.LittleEndian, &length); err != nil {
		return fmt.Errorf("Can't enconde msg length. Err: %v", err)
	}

	// encode message topic
	if err := r.Topic.Marshall(buf); err != nil {
		return fmt.Errorf("Can't enconde msg topic. Err: %v", err)
	}

	return nil
}

func (p *MetadataRequest) Unmarshall(buf *bytes.Buffer) error {
	var length int32
	if err := binary.Read(buf, binary.LittleEndian, &length); err != nil {
		return fmt.Errorf("Can't decode MetadataRequest length. Err: %v", err)
	}

	// read topic
	p.Topic = new(qStr)
	if err := UnmarshallQStr(buf, p.Topic); err != nil {
		return fmt.Errorf("Can't decode msg topic. Err: %v", err)
	}

	return nil
}

func (p *MetadataRequest) Type() MsgType {
	return MetadataRequestType
}