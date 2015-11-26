package client

import "testing"

func Test_NewProducerConfig_SetServerAddress(t *testing.T) {
	// given
	address := "foo"

	// when
	cfg, err := NewConfig(SetServerAddress(address))

	// then
	if err != nil {
		t.Error("Address in counfiguration should be set without errors")
	}

	if cfg.address != address {
		t.Errorf("Address in counfiguration should be set. Got: %s, expected: %s", cfg.address, address)
	}
}

func Test_NewProducerConfig_SetTimeout(t *testing.T) {
	// given
	timeout := 20

	// when
	cfg, err := NewConfig(SetTimeout(timeout))

	// then
	if err != nil {
		t.Error("Timeout in counfiguration should be set without errors")
	}

	if cfg.timeout != timeout {
		t.Errorf("Timeout in counfiguration should be set. Got: %d, expected: %d", cfg.timeout, timeout)
	}
}

func Test_NewProducerConfig_SetTimeout_Fail(t *testing.T) {
	// given
	timeout := -1

	// when
	_, err := NewConfig(SetTimeout(timeout))

	// then
	if err == nil {
		t.Error("Timeout in counfiguration should return error")
	}
}

func Test_NewProducerConfig_ChainAllFuncs(t *testing.T) {
	// given
	timeout := 20
	address := "foo"
	client := "bar"

	// when
	cfg, err := NewConfig(SetTimeout(timeout), SetServerAddress(address), SetClientID(client))

	// then
	if err != nil {
		t.Error("Timeout in configuration should be set without errors")
	}
	if cfg.timeout != timeout {
		t.Errorf("Timeout in configuration should be set. Got: %d, expected: %d", cfg.timeout, timeout)
	}
	if cfg.address != address {
		t.Errorf("Address in configuration should be set. Got: %s, expected: %s", cfg.address, address)
	}
	if cfg.clientID != client {
		t.Errorf("Client id in configuration should be set. Got: %s, expected: %s", cfg.clientID, client)
	}
}
