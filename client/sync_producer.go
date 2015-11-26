package client

import (
	"fmt"
	log "github.com/Sirupsen/logrus"
	"github.com/mateuszdyminski/queue/util"
	"sync"
)

var ErrNotPartitionAvailable = fmt.Errorf("No partition is available for specified topic!")

// NewSyncProducer returns new instance of synchronized producer.
func NewSyncProducer(cfg *Config) *SyncProducer {
	p := &SyncProducer{
		cfg:         cfg,
		partitioner: NewHashPartitioner(""),
		input:       make(chan *QMsg), // FIXME: put this size in config
	}

	go p.dispatcher()
	log.Infof("Producer dispatcher started!")

	return p
}

// SyncProducer.
type SyncProducer struct {
	cfg    *Config
	closed bool

	producers    map[*Server]chan<- *QMsg
	producerLock sync.Mutex

	input, successes chan *QMsg

	// it should be set per topic not per producer
	partitioner Partitioner
}

// Send sends message to the topic and returns: partition id, offset and error if appears.
func (p *SyncProducer) Send(m *QMsg) (int, int64, error) {
	p.input <- m

	// wait for result here

	return 0, int64(0), nil
}

func (p *SyncProducer) Close() error {
	p.closed = true
	// FIXME: impl rest

	return nil
}

func (p *SyncProducer) fetchMetadata() {
	// fetch metadata per topic
}

// dispatches messages by topic
func (p *SyncProducer) dispatcher() {
	handlers := make(map[string]chan<- *QMsg)
	//	shuttingDown := false

	for msg := range p.input {
		log.Infof("Got message! Dispatching per topic.")

		if msg == nil {
			log.Warnf("Someone wants to send nil as message. Omitting.")
			continue
		}

		//		if msg.flags&shutdown != 0 {
		//			shuttingDown = true
		//			p.inFlight.Done()
		//			continue
		//		} else if msg.retries == 0 {
		//			if shuttingDown {
		//				// we can't just call returnError here because that decrements the wait group,
		//				// which hasn't been incremented yet for this message, and shouldn't be
		//				pErr := &ProducerError{Msg: msg, Err: ErrShuttingDown}
		//				if p.conf.Producer.Return.Errors {
		//					p.errors <- pErr
		//				} else {
		//					Logger.Println(pErr)
		//				}
		//				continue
		//			}
		//			p.inFlight.Add(1)
		//		}
		//
		//		if msg.byteSize() > p.conf.Producer.MaxMessageBytes {
		//			p.returnError(msg, ErrMessageSizeTooLarge)
		//			continue
		//		}

		handler := handlers[msg.Topic]
		if handler == nil {
			handler = p.newTopicProducer(msg.Topic)
			handlers[msg.Topic] = handler
		}

		handler <- msg
	}

	for _, handler := range handlers {
		close(handler)
	}
}

// newTopicProducer creates new producer for specified topic
func (p *SyncProducer) newTopicProducer(topic string) chan<- *QMsg {
	log.Infof("Creating new topic producer!")

	input := make(chan *QMsg, 1024) // FIXME: put it in config
	tp := &topicProducer{
		parent: p,
		topic:  topic,
		input:  input,

		handlers:    make(map[int32]chan<- *QMsg),
		partitioner: p.partitioner, // FIXME: create partitioner per topic
	}

	// fetch metadata about this topic here
	srv := NewServer(p.cfg)
	srv.Open()

	req := new(util.MetadataRequest)
	res, err := srv.GetMetadata(req)
	if err != nil {
		log.Errorf("Can't find topic's metadata! Err: ", err)
		return nil
	}

	tp.partitions = []int(*res.Partitions)
	srv.Close()

	go tp.dispatch()
	log.Infof("Topic dispatcher started!")

	return input
}

// one per topic
// partitions messages, then dispatches them by partition
type topicProducer struct {
	parent     *SyncProducer
	topic      string
	input      <-chan *QMsg
	partitions []int

	handlers    map[int32]chan<- *QMsg
	partitioner Partitioner
}

func (tp *topicProducer) dispatch() {
	for msg := range tp.input {
		log.Infof("Got message in topic producer!")

		if err := tp.partitionMessage(msg); err != nil {
			fmt.Errorf("Can't partition: %v", err)
			continue
		}

		log.Infof("Got message partition!")

		handler := tp.handlers[msg.Partition]
		if handler == nil {
			handler = tp.parent.newPartitionProducer(msg.Topic, msg.Partition)
			tp.handlers[msg.Partition] = handler
		}

		handler <- msg
	}

	for _, handler := range tp.handlers {
		close(handler)
	}
}

func (tp *topicProducer) partitionMessage(msg *QMsg) error {
	numPartitions := len(tp.partitions)

	if numPartitions == 0 {
		return ErrNotPartitionAvailable
	}

	choice := tp.partitioner.Partition(msg, numPartitions)
	msg.Partition = int32(tp.partitions[choice])

	return nil
}

// one per partition per topic
// dispatches messages to the appropriate broker
// also responsible for maintaining message order during retries
type partitionProducer struct {
	parent    *SyncProducer
	topic     string
	partition int32
	input     <-chan *QMsg

	leader *Server
	output chan<- *QMsg
}

func (p *SyncProducer) newPartitionProducer(topic string, partition int32) chan<- *QMsg {
	log.Infof("Creating new partition producer")

	input := make(chan *QMsg, 1024) // FIXME: put this value in config
	pp := &partitionProducer{
		parent:    p,
		topic:     topic,
		partition: partition,
		input:     input,
		output:    make(chan<- *QMsg),
	}
	go pp.dispatch()
	log.Infof("Partition dispatcher started!")

	return input
}

func (pp *partitionProducer) dispatch() {
	for msg := range pp.input {
		log.Infof("Got message in partition dispatcher!")
		pp.output <- msg
	}

	log.Infof("Partition dispatcher finished its job!")
}
