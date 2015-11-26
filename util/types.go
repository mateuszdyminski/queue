package util
import "bytes"

type Msg interface {
	Length() uint32
	Marshall(*bytes.Buffer) error
	Unmarshall(*bytes.Buffer) error
	Type() MsgType
}

type MsgType uint16

const (
	MetadataRequestType     = MsgType(1)
	ProduceRequestType      = MsgType(2)
	FetchRequestType        = MsgType(3)
	OffsetRequestType       = MsgType(4)
	OffsetCommitRequestType = MsgType(5)
	OffsetFetchRequestType  = MsgType(6)

	MetadataResponseType     = MsgType(101)
	ProduceResponseType      = MsgType(102)
	FetchResponseType        = MsgType(103)
	OffsetResponseType       = MsgType(104)
	OffsetCommitResponseType = MsgType(105)
	OffsetFetchResponseType  = MsgType(106)
)

