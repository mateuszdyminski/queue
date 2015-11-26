package main

import (
	"github.com/mateuszdyminski/queue/client"
	"fmt"
)

func main() {
	cfg, err := client.NewConfig(client.SetTimeout(10), client.SetServerAddress(":1337"), client.SetClientID("first client ever"))
	if err != nil {
		panic(err)
	}

	producer := client.NewSyncProducer(cfg)

	msg := client.QMsg{Topic: "topic", Key: []byte("test key"), Value: []byte("test value")}
	pid, offset, err := producer.Send(&msg)
	if err != nil {
		panic(err)
	}

	fmt.Errorf("Message send to partition %d. Got offset: %d", pid, offset)
}
