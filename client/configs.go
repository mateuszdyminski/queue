package client

import (
	"fmt"
)

// Config holds info about client configuration.
type Config struct {
	address string
	timeout int
	clientID    string
	partitioner PartitionerConstructor
}

// NewConfig func returns new configuration
func NewConfig(options ...ConfigFunc) (*Config, error) {
	c := &Config{
		address: DefaultServerAddress,
		timeout: DefaultTimeout,
		clientID: DefaultClientID,
		partitioner: NewHashPartitioner,
	}

	// Run the options on it
	for _, option := range options {
		if err := option(c); err != nil {
			return nil, err
		}
	}

	return c, nil
}

// SetServerAddress sets server address in configuration and returns ProducerConfigFunc.
func SetServerAddress(address string) ConfigFunc {
	return func(c *Config) error {
		c.address = address
		return nil
	}
}

// SetServerAddress sets server address in configuration and returns ProducerConfigFunc.
func SetTimeout(timeout int) ConfigFunc {
	return func(c *Config) error {
		if timeout < 1 {
			return fmt.Errorf("Wrong value of timeout! Val: %d", timeout)
		}

		c.timeout = timeout
		return nil
	}
}

// SetClientID sets the id of the client and returns ProducerConfigFunc.
func SetClientID(clientID string) ConfigFunc {
	return func(c *Config) error {
		if clientID == "" {
			return fmt.Errorf("Client ID of the producer can't be empty!")
		}

		c.clientID = clientID
		return nil
	}
}

// ConfigFunc helper func to chain the setters of configuration.
type ConfigFunc func(*Config) error
