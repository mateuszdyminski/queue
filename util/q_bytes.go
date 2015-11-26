package util

import (
	"bytes"
	"encoding/binary"
	"fmt"
)

type qBytes []byte

func NewQBytes(data []byte) *qBytes {
	q := qBytes(data)
	return &q
}

func (b *qBytes) Length() int {
	return 4 + len(*b)
}

func (b *qBytes) Marshall(buf *bytes.Buffer) error {
	length := int32(len(*b))
	if length == 0 {
		length = -1
	}

	// encode message length
	if err := binary.Write(buf, binary.LittleEndian, &length); err != nil {
		return fmt.Errorf("Can't enconde msg length. Err: %v", err)
	}

	// encode bytes
	if err := binary.Write(buf, binary.LittleEndian, b); err != nil {
		return fmt.Errorf("Can't enconde bytes. Err: %v", err)
	}

	return nil
}

func UnmarshallQBytes(buf *bytes.Buffer, q *qBytes) error {
	var length int32
	if err := binary.Read(buf, binary.LittleEndian, &length); err != nil {
		return fmt.Errorf("Can't decode qBytes length. Err: %v", err)
	}

	// byte array is nil
	if length == -1 {
		return nil
	}

	*q = make(qBytes, length)
	if err := binary.Read(buf, binary.LittleEndian, q); err != nil {
		return fmt.Errorf("Can't decode qBytes payload. Err: %v", err)
	}

	return nil
}
