package util

import (
	"bytes"
	"testing"
)

func Test_ResponseMsg_Length(t *testing.T) {
	// given
	m := NewProduceResponse("test topic", 2, 3, 4)
	r := NewResponseMsg(5, ProduceResponseType, "client test id", m)

	// when
	totalLenght := r.Length()

	// then
	if totalLenght != 38 {
		t.Errorf("Wrong Lenght. Expected: %v, Got: %v", 38, totalLenght)
	}
}

func Test_ResponseMsg_Marshall_Unmarshall(t *testing.T) {
	m := NewProduceResponse("test topic", 2, 3, 4)
	r := NewResponseMsg(5, ProduceResponseType, "client test id", m)
	var bufBytes []byte
	buf := bytes.NewBuffer(bufBytes)
	final := new(ProduceResponse)


	// when
	buf, err := r.Encode()
	buf.Read(make([]byte, 8)) // read 8 bytes to simulate that length and correlationId is read outside the decoder
	err2 := Decode(buf, final)

	// then
	if err != nil {
		t.Errorf("Marshall should be successful. Expected no nil, Got: %v", err)
	}

	if final.Length() != 30 {
		t.Errorf("Wrong Lenght. Expected: %v, Got: %v", 30, final.Length())
	}

	if err2 != nil {
		t.Errorf("Unmarshall should be successful. Expected no nil, Got: %v", err)
	}

	compareResponseMsg(m, final, t)
}

func compareResponseMsg(expected, got Msg, t *testing.T) {
	expectedBody := expected.(*ProduceResponse)
	gotBody := got.(*ProduceResponse)

	compareProduceResponses(expectedBody, gotBody, t)
}