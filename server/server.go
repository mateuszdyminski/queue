package server

import (
	"net"
	log "github.com/Sirupsen/logrus"
	"github.com/BurntSushi/toml"
	"sync/atomic"
)

type QServer struct {
	clients map[int32]*client
	conf *Config

	idSeq int32
}

func Start(configPath string) *QServer {
	// load config
	var conf Config
	_, err := toml.DecodeFile(configPath, &conf)
	if err != nil {
		log.Fatalf("Can't decode config file!")
	}

	// create server
	s := &QServer{conf: conf, clients: make(map[int32]*client)}

	return s
}

func (q *QServer) ListenForClients() {
	l, err := net.Listen("tcp", q.conf.Address)
	if err != nil {
		log.Println("Error while start listening", l.Addr())
		return
	}

	log.Println("Listening on ", l.Addr())
	for {
		conn, err := l.Accept()
		if err != nil {
			if ne, ok := err.(net.Error); ok && ne.Temporary() {
				log.Printf("Accept error: %v", err)
			}
			continue
		}
		q.addClient(conn)
	}
}

func (q *QServer) addClient(conn *net.Conn) {
	cli := &client{conf: q.conf, conn: conn}
	cli.id = atomic.AddInt32(&q.idSeq, 1)
	q.clients[cli.id] = cli
}