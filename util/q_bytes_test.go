package util

import (
	"bytes"
	"testing"
)

func Test_QBytes_Length(t *testing.T) {
	// given
	b := NewQBytes([]byte("testtest"))

	// when
	totalLenght := b.Length()

	// then
	if totalLenght != 12 {
		t.Errorf("Wrong Lenght. Expected: %v, Got: %v", 12, totalLenght)
	}
}

func Test_QBytes_Marshall_Unmarshall(t *testing.T) {
	// given
	b := qBytes{0x18, 0x2d, 0x44, 0x54, 0xfb, 0x21, 0x09, 0x40}
	var bufBytes []byte
	buf := bytes.NewBuffer(bufBytes)
	var final qBytes

	// when
	err := b.Marshall(buf)
	err2 := UnmarshallQBytes(buf, &final)

	// then
	if err != nil {
		t.Errorf("Marshall should be successful. Expected no nil, Got: %v", err)
	}

	if final.Length() != 12 {
		t.Errorf("Wrong Lenght. Expected: %v, Got: %v", 12, final.Length())
	}

	if err2 != nil {
		t.Errorf("Unmarshall should be successful. Expected no nil, Got: %v", err2)
	}

	if bytes.Compare(b, final) != 0 {
		t.Errorf("Data after marshalling and unmarshalling should be the same!")
	}
}

func Test_QBytes_Marshall_Unmarshall_Nil(t *testing.T) {
	// given
	var b qBytes
	var final qBytes
	buf := bytes.NewBuffer(b)

	// when
	err := b.Marshall(buf)
	err2 := UnmarshallQBytes(buf, &final)

	// then
	if err != nil {
		t.Errorf("Marshall should be successful. Expected no nil, Got: %v", err)
	}

	if final.Length() != 4 {
		t.Errorf("Wrong Lenght. Expected: %v, Got: %v", 4, final.Length())
	}

	if err2 != nil {
		t.Errorf("Unmarshall should be successful. Expected no nil, Got: %v", err)
	}

	if bytes.Compare(b, final) != 0 {
		t.Errorf("Data after marshalling and unmarshalling should be the same!")
	}
}
