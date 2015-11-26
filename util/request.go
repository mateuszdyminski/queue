package util

import (
	"bytes"
	"encoding/binary"
	"fmt"
)

//RequestMessage => CorrelationId ApiKey ApiVersion ClientId RequestMessage
//  CorrelationId => int32
//  ApiKey => int16
//  ClientId => string
//  RequestMessage => MetadataRequest | ProduceRequest | FetchRequest | OffsetRequest | OffsetCommitRequest | OffsetFetchRequest

type RequestMsg struct {
	CorrelationId uint32
	ApiKey        MsgType
	ClientId      *qStr
	Message       Msg
}

func NewRequestMsg(correlationID uint32, clientId string, msg Msg) *RequestMsg {
	return &RequestMsg{
		CorrelationId: correlationID,
		ApiKey:        msg.Type(),
		ClientId:      NewQStr(clientId),
		Message:       msg,
	}
}

func (m *RequestMsg) Length() uint32 {
	return uint32(10) + uint32(m.ClientId.Length()) + m.Message.Length()
}

func (m *RequestMsg) Encode() (*bytes.Buffer, error) {
	length := uint32(6) + uint32(m.ClientId.Length()) + m.Message.Length()
	buf := new(bytes.Buffer)

	// encode message length
	if err := binary.Write(buf, binary.LittleEndian, length); err != nil {
		return nil, fmt.Errorf("Can't enconde msg length. Err: %v", err)
	}

	// encode correlationId
	if err := binary.Write(buf, binary.LittleEndian, m.CorrelationId); err != nil {
		return nil, fmt.Errorf("Can't enconde correlationId. Err: %v", err)
	}

	// encode apiKey
	if err := binary.Write(buf, binary.LittleEndian, m.ApiKey); err != nil {
		return nil, fmt.Errorf("Can't enconde api key. Err: %v", err)
	}


	// encode clientId
	if err := m.ClientId.Marshall(buf); err != nil {
		return nil, fmt.Errorf("Can't enconde clientId. Err: %v", err)
	}

	// encode message body
	if err := m.Message.Marshall(buf); err != nil {
		return nil, fmt.Errorf("Can't encode message. Err: %v", err)
	}

	return buf, nil
}

func (m *RequestMsg) Decode(buf *bytes.Buffer) error {
	// read correlationId
	if err := binary.Read(buf, binary.LittleEndian, &m.CorrelationId); err != nil {
		return fmt.Errorf("Can't decode msg correlationid. Err: %v", err)
	}

	// read apiKey
	if err := binary.Read(buf, binary.LittleEndian, &m.ApiKey); err != nil {
		return fmt.Errorf("Can't decode msg apiKey. Err: %v", err)
	}

	// read clientId
	m.ClientId = new(qStr)
	if err := UnmarshallQStr(buf, m.ClientId); err != nil {
		return fmt.Errorf("Can't decode msg clientid. Err: %v", err)
	}

	// read message body
	switch m.ApiKey {
	case ProduceRequestType:
		m.Message = new(ProduceRequest)
	case FetchRequestType, MetadataRequestType, OffsetRequestType, OffsetCommitRequestType, OffsetFetchRequestType:
		return fmt.Errorf("Can't decode msg. Message type not implemented!")
	default:
		return fmt.Errorf("Unknown API key: %v. Can't decode msg.", m.ApiKey)
	}
	if err := m.Message.Unmarshall(buf); err != nil {
		return fmt.Errorf("Can't decode msg body. Err: %v", err)
	}

	return nil
}
