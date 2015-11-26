package util

import (
	"testing"
	"bytes"
)

func Test_QStr_Length(t *testing.T) {
	// given
	b := NewQStr("test string")

	// when
	totalLenght := b.Length()

	// then
	if totalLenght != 13 {
		t.Errorf("Wrong Lenght. Expected: %v, Got: %v", 13, totalLenght)
	}
}

func Test_QStr_Marshall_Unmarshall(t *testing.T) {
	// given
	var bufArr []byte
	b := qStr("test string")
	buf := bytes.NewBuffer(bufArr)
	var final qStr

	// when
	err := b.Marshall(buf)
	err2 := UnmarshallQStr(buf, &final)

	// then
	if err != nil {
		t.Errorf("Marshall should be successful. Expected no nil, Got: %v", err)
	}

	if final.Length() != 13 {
		t.Errorf("Wrong Lenght. Expected: %v, Got: %v", 13, final.Length())
	}

	if err2 != nil {
		t.Errorf("Unmarshall should be successful. Expected no nil, Got: %v", err)
	}

	if b != final {
		t.Errorf("Data after marshalling and unmarshalling should be the same!")
	}
}

func Test_QStr_Marshall_Unmarshall_Nil(t *testing.T) {
	// given
	var bufArr []byte
	buf := bytes.NewBuffer(bufArr)
	var b qStr
	var final qStr

	// when
	err := b.Marshall(buf)
	err2 := UnmarshallQStr(buf, &final)

	// then
	if err != nil {
		t.Errorf("Marshall should be successful. Expected no nil, Got: %v", err)
	}

	if final.Length() != 2 {
		t.Errorf("Wrong Lenght. Expected: %v, Got: %v", 2, final.Length())
	}

	if err2 != nil {
		t.Errorf("Unmarshall should be successful. Expected no nil, Got: %v", err2)
	}

	if b != final {
		t.Errorf("Data after marshalling and unmarshalling should be the same!")
	}
}
