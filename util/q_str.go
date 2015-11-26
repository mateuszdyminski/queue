package util

import (
	"bytes"
	"encoding/binary"
	"fmt"
)

type qStr string

func NewQStr(str string) *qStr {
	q := qStr(str)
	return &q
}

func (b *qStr) Length() int {
	return 2 + len(*b)
}

func (b *qStr) Marshall(buf *bytes.Buffer) error {
	length := int16(len(*b))
	if length == 0 {
		length = -1
	}

	// encode message length
	if err := binary.Write(buf, binary.LittleEndian, &length); err != nil {
		return fmt.Errorf("Can't enconde msg length. Err: %v", err)
	}

	// encode bytes
	if err := binary.Write(buf, binary.LittleEndian, []byte(*b)); err != nil {
		return fmt.Errorf("Can't enconde bytes. Err: %v", err)
	}

	return nil
}

func UnmarshallQStr(buf *bytes.Buffer, q *qStr) error {
	var length int16
	if err := binary.Read(buf, binary.LittleEndian, &length); err != nil {
		return fmt.Errorf("Can't decode qStr length. Err: %v", err)
	}

	// byte array is nil
	if length == -1 {
		return nil
	}

	d := make([]byte, length)
	if err := binary.Read(buf, binary.LittleEndian, d); err != nil {
		return fmt.Errorf("Can't decode qStr payload. Err: %v", err)
	}

	*q = qStr(d)

	return nil
}
