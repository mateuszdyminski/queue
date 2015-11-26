package util

import (
	"bytes"
	"encoding/binary"
	"fmt"
)

//Response => CorrelationId ResponseMessage
//CorrelationId => int32
//ResponseMessage => MetadataResponse | ProduceResponse | FetchResponse | OffsetResponse | OffsetCommitResponse | OffsetFetchResponse

type ResponseMsg struct {
	CorrelationId uint32
	Message       Msg
}

func NewResponseMsg(correlationID uint32, typ MsgType, clientId string, msg Msg) *ResponseMsg {
	return &ResponseMsg{
		CorrelationId: correlationID,
		Message:       msg,
	}
}

func (m *ResponseMsg) Length() uint32 {
	return uint32(8) + m.Message.Length()
}

func (m *ResponseMsg) Encode() (*bytes.Buffer, error) {
	length := uint32(4) + m.Message.Length()
	buf := new(bytes.Buffer)

	// encode message length
	if err := binary.Write(buf, binary.LittleEndian, length); err != nil {
		return nil, fmt.Errorf("Can't enconde msg length. Err: %v", err)
	}

	// encode correlationId
	if err := binary.Write(buf, binary.LittleEndian, m.CorrelationId); err != nil {
		return nil, fmt.Errorf("Can't enconde correlationId. Err: %v", err)
	}

	// encode message body
	if err := m.Message.Marshall(buf); err != nil {
		return nil, fmt.Errorf("Can't encode message. Err: %v", err)
	}

	return buf, nil
}

func Decode(buf *bytes.Buffer, msg Msg) error {
	if err := msg.Unmarshall(buf); err != nil {
		return fmt.Errorf("Can't decode msg body. Err: %v", err)
	}

	return nil
}
