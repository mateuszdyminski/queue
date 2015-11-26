package client

import (
	"testing"
)

func Test_NewSyncProducer(t *testing.T) {
	cfg, _ := NewConfig()
	c := NewSyncProducer(cfg)

	if c == nil {
		t.Error("Producer should be created!")
	}
}
