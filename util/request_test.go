package util

import (
	"bytes"
	"testing"
)

func Test_RequestMsg_Length(t *testing.T) {
	// given
	m := NewProduceRequest("test topic", 2, nil, []byte("test value"))
	r := NewRequestMsg(4, ProduceRequestType, "client test id", m)

	// when
	totalLenght := r.Length()

	// then
	if totalLenght != 64 {
		t.Errorf("Wrong Lenght. Expected: %v, Got: %v", 64, totalLenght)
	}
}

func Test_RequestMsg_Marshall_Unmarshall(t *testing.T) {
	m := NewProduceRequest("test topic", 2, nil, []byte("test value"))
	r := NewRequestMsg(4, ProduceRequestType, "client test id", m)
	var bufBytes []byte
	buf := bytes.NewBuffer(bufBytes)
	final := new(RequestMsg)

	// when
	buf, err := r.Encode()
	buf.Read(make([]byte, 4)) // read 4 bytes to simulate that length will be read
	err2 := final.Decode(buf)

	// then
	if err != nil {
		t.Errorf("Marshall should be successful. Expected no nil, Got: %v", err)
	}

	if final.Length() != 64 {
		t.Errorf("Wrong Lenght. Expected: %v, Got: %v", 64, final.Length())
	}

	if err2 != nil {
		t.Errorf("Unmarshall should be successful. Expected no nil, Got: %v", err)
	}

	compareRequestMsg(r, final, t)
}

func compareRequestMsg(expected, got *RequestMsg, t *testing.T) {
	if *expected.ClientId != *got.ClientId {
		t.Errorf("Wrong clientId. Expected: %v, Got: %v", *expected.ClientId, *got.ClientId)
	}

	if expected.ApiKey != got.ApiKey {
		t.Errorf("Wrong apiKey. Expected: %v, Got: %v", expected.ApiKey, got.ApiKey)
	}

	if expected.CorrelationId != got.CorrelationId {
		t.Errorf("Wrong correlationId. Expected: %v, Got: %v", expected.CorrelationId, got.CorrelationId)
	}

	expectedBody := expected.Message.(*ProduceRequest)
	gotBody := got.Message.(*ProduceRequest)

	compareProduceRequests(expectedBody, gotBody, t)
}