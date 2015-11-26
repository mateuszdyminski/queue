package util

import (
	"bytes"
	"encoding/binary"
	"fmt"
)

type qIntArr []int

func NewQIntArr(ints []int) *qIntArr {
	q := qIntArr(ints)
	return &q
}

func (b *qIntArr) Length() int {
	return 4 + (len(*b) * 4)
}

func (b *qIntArr) Marshall(buf *bytes.Buffer) error {
	length := int32(len(*b) * 4)
	if length == 0 {
		length = -1
	}

	// encode message length
	if err := binary.Write(buf, binary.LittleEndian, &length); err != nil {
		return fmt.Errorf("Can't enconde msg length. Err: %v", err)
	}

	// encode bytes
	for _, val := range *b {
		if err := binary.Write(buf, binary.LittleEndian, int32(val)); err != nil {
			return fmt.Errorf("Can't enconde qintArr. Err: %v", err)
		}
	}

	return nil
}

func UnmarshallQIntArr(buf *bytes.Buffer, q *qIntArr) error {
	var length int32
	if err := binary.Read(buf, binary.LittleEndian, &length); err != nil {
		return fmt.Errorf("Can't decode qintArr length. Err: %v", err)
	}

	// int array is nil
	if length == -1 {
		return nil
	}

	*q = make(qIntArr, length / 4)
	for i := range *q {
		var d int32
		if err := binary.Read(buf, binary.LittleEndian, &d); err != nil {
			return fmt.Errorf("Can't decode qintArr payload. Err: %v", err)
		}
		(*q)[i] = int(d)
	}

	return nil
}
