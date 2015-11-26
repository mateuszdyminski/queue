package server

import (
	"net"
	log "github.com/Sirupsen/logrus"
)


type client struct {
	conf *Config

	id int32
	conn net.Conn
}


func (c *client) readLoop() {
	b := make([]byte, c.conf.BufferSize)

	for {
		n, err := c.conn.Read(b)
		if err != nil {
			log.Errorf("Read error: %v", err)
			c.close()
			return
		}
		if err := c.parse(b[:n]); err != nil {
			log.Printf("Parse Error: %v\n", err)
			c.close()
			return
		}
	}
}

func (c *client) close() {
	if c.conn == nil {
		return
	}

	// close connection
	if err := c.conn.Close(); err != nil {
		log.Errorf("Can't close client connection. Err: %v", err)
	}

	// remove subscriptions from server
}

func (c *client) parse(bytes []byte) error {

}