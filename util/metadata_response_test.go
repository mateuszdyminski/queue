package util

import (
	"bytes"
	"testing"
)

func Test_MetadataResponse_Length(t *testing.T) {
	// given
	r := NewMetadataResponse("test topic", []int{2, 4, 6})

	// when
	totalLength := r.Length()

	// then
	if totalLength != 32 {
		t.Errorf("Wrong Length. Expected: %v, Got: %v", 32, totalLength)
	}
}

func Test_MetadataResponse_Marshall_Unmarshall(t *testing.T) {
	// given
	r := NewMetadataResponse("test topic", []int{2, 4, 6})
	var bufBytes []byte
	buf := bytes.NewBuffer(bufBytes)
	final := new(MetadataResponse)

	// when
	err := r.Marshall(buf)
	err2 := final.Unmarshall(buf)

	// then
	if err != nil {
		t.Errorf("Marshall should be successful. Expected no nil, Got: %v", err)
	}

	if final.Length() != 32 {
		t.Errorf("Wrong Lenght. Expected: %v, Got: %v", 32, final.Length())
	}

	if err2 != nil {
		t.Errorf("Unmarshall should be successful. Expected no nil, Got: %v", err2)
	}

	if *r.Topic != *final.Topic {
		t.Errorf("Wrong topic. Expected: %v, Got: %v", *r.Topic, *final.Topic)
	}

	if len(*r.Partitions) != len(*final.Partitions) {
		t.Errorf("Wrong length of partitions. Expected: %v, Got: %v", len(*r.Partitions), len(*final.Partitions))
	}
}
