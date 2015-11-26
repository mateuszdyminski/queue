package util

import (
	"bytes"
	"testing"
)

func Test_ProduceResponse_Length(t *testing.T) {
	// given
	r := NewProduceResponse("test topic", 1, 2, 3)

	// when
	totalLenght := r.Length()

	// then
	if totalLenght != 30 {
		t.Errorf("Wrong Lenght. Expected: %v, Got: %v", 30, totalLenght)
	}
}

func Test_ProduceResponse_Marshall_Unmarshall(t *testing.T) {
	// given
	r := NewProduceResponse("test topic", 1, 2, 3)
	var bufBytes []byte
	buf := bytes.NewBuffer(bufBytes)
	final := new(ProduceResponse)

	// when
	err := r.Marshall(buf)
	err2 := final.Unmarshall(buf)

	// then
	if err != nil {
		t.Errorf("Marshall should be successful. Expected no nil, Got: %v", err)
	}

	if final.Length() != 30 {
		t.Errorf("Wrong Lenght. Expected: %v, Got: %v", 30, final.Length())
	}

	if err2 != nil {
		t.Errorf("Unmarshall should be successful. Expected no nil, Got: %v", err2)
	}

	compareProduceResponses(r, final, t)
}

func compareProduceResponses(expected, got *ProduceResponse, t *testing.T) {
	if *expected.Topic != *got.Topic {
		t.Errorf("Wrong topic. Expected: %v, Got: %v", *expected.Topic, *got.Topic)
	}

	if expected.Partition != got.Partition {
		t.Errorf("Wrong partition. Expected: %v, Got: %v", expected.Partition, got.Partition)
	}

	if expected.ErrorCode != got.ErrorCode {
		t.Errorf("Wrong error code. Expected: %v, Got: %v", expected.ErrorCode, got.ErrorCode)
	}

	if expected.Offset != got.Offset {
		t.Errorf("Wrong offset. Expected: %v, Got: %v", expected.Offset, got.Offset)
	}
}