package util
import (
	"testing"
	"bytes"
)

func Test_MetadataRequest_Length(t *testing.T) {
	// given
	r := NewMetadataRequest("test topic")

	// when
	totalLenght := r.Length()

	// then
	if totalLenght != 14 {
		t.Errorf("Wrong Lenght. Expected: %v, Got: %v", 14, totalLenght)
	}
}

func Test_MetadataRequest_Marshall_Unmarshall(t *testing.T) {
	// given
	r := NewMetadataRequest("test topic")
	var bufBytes []byte
	buf := bytes.NewBuffer(bufBytes)
	final := new(MetadataRequest)

	// when
	err := r.Marshall(buf)
	err2 := final.Unmarshall(buf)

	// then
	if err != nil {
		t.Errorf("Marshall should be successful. Expected no nil, Got: %v", err)
	}

	if final.Length() != 14 {
		t.Errorf("Wrong Lenght. Expected: %v, Got: %v", 14, final.Length())
	}

	if err2 != nil {
		t.Errorf("Unmarshall should be successful. Expected no nil, Got: %v", err2)
	}

	if *r.Topic != *final.Topic {
		t.Errorf("Wrong topic. Expected: %v, Got: %v", *r.Topic, *final.Topic)
	}
}
