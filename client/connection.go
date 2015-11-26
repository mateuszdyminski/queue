package client

import (
	"fmt"
	"io"
	"net"
	"sync"
	"sync/atomic"
	"time"
	"github.com/Sirupsen/logrus"
	"encoding/binary"
	"github.com/mateuszdyminski/queue/util"
	"bytes"
)

var ErrConnEstablished = fmt.Errorf("Connection already established!")
var ErrConnNon = fmt.Errorf("Connection not defined! Nothing to be closed.")
var ErrNotConnected = fmt.Errorf("Not connected!")

// Server represents a single server connection.
type Server struct {
	config        *Config

	correlationID uint32
	conn          net.Conn
	connErr       error
	lock          sync.Mutex
	opened        int32

	responses     chan responsePromise
	done          chan bool
}

type responsePromise struct {
	correlationID uint32
	payload 	  chan []byte
	errors        chan error
}

func NewServer(conf *Config) *Server {
	return &Server{config: conf}
}

func (b *Server) Open() error {
	if !atomic.CompareAndSwapInt32(&b.opened, 0, 1) {
		return ErrConnEstablished
	}

	b.lock.Lock()

	if b.conn != nil {
		b.lock.Unlock()
		return ErrConnEstablished
	}

	go func() {
		defer b.lock.Unlock()

		dialer := net.Dialer{
			Timeout:   time.Duration(10 * time.Second), // FIXME: put it in config
			KeepAlive: time.Duration(0), // FIXME: put it in config
		}

		b.conn, b.connErr = dialer.Dial("tcp", b.config.address)

		if b.connErr != nil {
			b.conn = nil
			atomic.StoreInt32(&b.opened, 0)
			logrus.Errorf("Failed to connect to broker %s: %s", b.config.address, b.connErr)
			return
		}

		b.done = make(chan bool)
		b.responses = make(chan responsePromise, 1024) // FIXME: Put no of open request in configuration

		logrus.Infof("Connected to broker at %s", b.config.address)

		go b.responseReceiver()
	}()

	return nil
}

// Connected returns true if the broker is connected and false otherwise. If the broker is not
// connected but it had tried to connect, the error from that connection attempt is also returned.
func (b *Server) Connected() (bool, error) {
	b.lock.Lock()
	defer b.lock.Unlock()

	return b.conn != nil, b.connErr
}

func (b *Server) Close() error {
	b.lock.Lock()
	defer b.lock.Unlock()

	if b.conn == nil {
		return ErrConnNon
	}

	close(b.responses)
	<-b.done

	err := b.conn.Close()
	if err == nil {
		logrus.Printf("Closed connection to broker %s", b.config.address)
	} else {
		logrus.Printf("Error while closing connection to broker %s: %s", b.config.address, err)
	}

	b.conn = nil
	b.connErr = nil
	b.done = nil
	b.responses = nil

	atomic.StoreInt32(&b.opened, 0)

	return err
}

func (b *Server) GetMetadata(request *util.MetadataRequest) (*util.MetadataResponse, error) {
	res := new(util.MetadataResponse)

	if err := b.sendAndReceive(request, res); err != nil {
		return nil, err
	}

	return res, nil
}

func (b *Server) Produce(request *util.ProduceRequest) (*util.ProduceResponse, error) {
	res := new(util.ProduceResponse)

	if err := b.sendAndReceive(request, res); err != nil {
		return nil, err
	}

	return res, nil
}

func (b *Server) send(msg util.Msg, promiseResponse bool) (*responsePromise, error) {
	b.lock.Lock()
	defer b.lock.Unlock()

	if b.conn == nil {
		if b.connErr != nil {
			return nil, b.connErr
		}
		return nil, ErrNotConnected
	}

	req := util.NewRequestMsg(b.correlationID, b.config.clientID, msg)
	buf, err := req.Encode()
	if err != nil {
		return nil, err
	}

	err = b.conn.SetWriteDeadline(time.Now().Add(time.Duration(10 * time.Second))) // FIXME: Add new entry for write timeout
	if err != nil {
		return nil, err
	}

	_, err = b.conn.Write(buf.Bytes())
	if err != nil {
		return nil, err
	}
	b.correlationID++

	if !promiseResponse {
		return nil, nil
	}

	promise := responsePromise{req.CorrelationId, make(chan []byte), make(chan error)}
	b.responses <- promise

	return &promise, nil
}

func (b *Server) sendAndReceive(req util.Msg, res util.Msg) error {
	promise, err := b.send(req, res != nil)

	if err != nil {
		return err
	}

	if promise == nil {
		return nil
	}

	select {
	case buf := <-promise.payload:
		return util.Decode(bytes.NewBuffer(buf), res)
	case err = <-promise.errors:
		return err
	}
}

func (b *Server) responseReceiver() {
	length := make([]byte, 8)
	for response := range b.responses {

		// should we set it every time we encountered response ???
		err := b.conn.SetReadDeadline(time.Now().Add(time.Duration(10 * time.Second))) // FIXME: put it config as read timeout
		if err != nil {
			response.errors <- err
			continue
		}

		_, err = io.ReadFull(b.conn, length)
		if err != nil {
			response.errors <- err
			continue
		}

		// read slice as int
		l := binary.LittleEndian.Uint32(length[:3])
		cid := binary.LittleEndian.Uint32(length[4:])

		// read response body
		buf := make([]byte, l - 4) // we have already read the correlationID
		_, err = io.ReadFull(b.conn, buf)
		if err != nil {
			response.errors <- err
			continue
		}

		if cid != response.correlationID {
			// FIXME: implement logic when those IDs are not the same - order is a bit different
			response.errors <- fmt.Errorf("correlation ID didn't match, wanted %d, got %d", response.correlationID, cid)
			continue
		}

		// decode msg
		response.payload <- buf
	}

	close(b.done)
}
