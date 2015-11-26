package util

import (
	"bytes"
	"testing"
)

func Test_ProduceRequest_Length(t *testing.T) {
	// given
	r := NewProduceRequest("test topic", 0, []byte("test key"), []byte("test value"))

	// when
	totalLenght := r.Length()

	// then
	if totalLenght != 46 {
		t.Errorf("Wrong Lenght. Expected: %v, Got: %v", 46, totalLenght)
	}
}

func Test_ProduceRequest_Marshall_Unmarshall(t *testing.T) {
	// given
	r := NewProduceRequest("test topic", 2, []byte("test key"), []byte("test value"))
	var bufBytes []byte
	buf := bytes.NewBuffer(bufBytes)
	final := new(ProduceRequest)

	// when
	err := r.Marshall(buf)
	err2 := final.Unmarshall(buf)

	// then
	if err != nil {
		t.Errorf("Marshall should be successful. Expected no nil, Got: %v", err)
	}

	if final.Length() != 46 {
		t.Errorf("Wrong Lenght. Expected: %v, Got: %v", 46, final.Length())
	}

	if err2 != nil {
		t.Errorf("Unmarshall should be successful. Expected no nil, Got: %v", err2)
	}

	compareProduceRequests(r, final, t)
}

func Test_ProduceRequest_Marshall_Unmarshall_Nil(t *testing.T) {
	// given
	r := NewProduceRequest("test topic", 2, nil, []byte("test value"))
	var bufBytes []byte
	buf := bytes.NewBuffer(bufBytes)
	final := new(ProduceRequest)

	// when
	err := r.Marshall(buf)
	err2 := final.Unmarshall(buf)

	// then
	if err != nil {
		t.Errorf("Marshall should be successful. Expected no nil, Got: %v", err)
	}

	if final.Length() != 38 {
		t.Errorf("Wrong Lenght. Expected: %v, Got: %v", 38, final.Length())
	}

	if err2 != nil {
		t.Errorf("Unmarshall should be successful. Expected no nil, Got: %v", err)
	}

	compareProduceRequests(r, final, t)
}

func compareProduceRequests(expected, got *ProduceRequest, t *testing.T) {
	if bytes.Compare(*expected.Key, *got.Key) != 0 {
		t.Errorf("Wrong key. Expected: %v, Got: %v", *expected.Key, *got.Key)
	}

	if bytes.Compare(*expected.Value, *got.Value) != 0 {
		t.Errorf("Wrong value. Expected: %v, Got: %v", *expected.Value, *got.Value)
	}

	if *expected.Topic != *got.Topic {
		t.Errorf("Wrong topic. Expected: %v, Got: %v", *expected.Topic, *got.Topic)
	}

	if expected.Partition != got.Partition {
		t.Errorf("Wrong partition. Expected: %v, Got: %v", expected.Partition, got.Partition)
	}
}