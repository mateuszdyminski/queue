package util

import (
	"bytes"
	"testing"
)

func Test_QIntArr_Length(t *testing.T) {
	// given
	b := NewQIntArr([]int{2, 4, 6})

	// when
	totalLenght := b.Length()

	// then
	if totalLenght != 16 {
		t.Errorf("Wrong Lenght. Expected: %v, Got: %v", 16, totalLenght)
	}
}

func Test_QIntArr_Marshall_Unmarshall(t *testing.T) {
	// given
	elems := []int{2, 4, 6}
	b := NewQIntArr(elems)
	var bufBytes []byte
	buf := bytes.NewBuffer(bufBytes)
	var final qIntArr

	// when
	err := b.Marshall(buf)
	err2 := UnmarshallQIntArr(buf, &final)

	// then
	if err != nil {
		t.Errorf("Marshall should be successful. Expected no nil, Got: %v", err)
	}

	if final.Length() != 16 {
		t.Errorf("Wrong Lenght. Expected: %v, Got: %v", 16, final.Length())
	}

	if err2 != nil {
		t.Errorf("Unmarshall should be successful. Expected no nil, Got: %v", err2)
	}

	if len(final) != len(elems) {
		t.Errorf("Wrong Value. Expected: %v, Got: %v", len(elems), len(final))
	}

	for i := range elems {
		if elems[i] != final[i] {
			t.Errorf("Wrong Value. Expected: %v, Got: %v", elems[i], final[i])
		}
	}
}

func Test_QIntArr_Marshall_Unmarshall_Nil(t *testing.T) {
	// given
	var data []byte
	var b qIntArr
	var final qIntArr
	buf := bytes.NewBuffer(data)

	// when
	err := b.Marshall(buf)
	err2 := UnmarshallQIntArr(buf, &final)

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

	if len(final) != 0 {
		t.Errorf("Slice should be empty!")
	}
}
