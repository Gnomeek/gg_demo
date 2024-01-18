package main

import (
	"github.com/stretchr/testify/assert"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/types/known/anypb"
	"testing"
	"time"
)

func newMsg(payload *anypb.Any) *Message {
	event := newEventMsg()
	event.Payload = payload
	bytePayload, _ := proto.Marshal(event)
	return &Message{
		uuid:    "1",
		payload: bytePayload,
	}
}

func newEventMsg() *Event {
	event := &Event{
		Metadata: newMetaDataMsg(),
	}
	return event
}

func newMetaDataMsg() *MetaData {
	meta := &MetaData{
		SpanId:    "1233",
		TraceId:   "2377",
		EventTime: time.Now().Unix(),
		EventId:   "a123",
	}
	return meta
}

func TestMessage_Unmarshal(t *testing.T) {
	p1, _ := anypb.New(newMetaDataMsg())
	p2, _ := anypb.New(newEventMsg())
	msg1 := newMsg(p1)
	msg2 := newMsg(p2)
	engine := newSubscribeEngine()
	h1, _ := NewHandler[*MetaData]("h1")
	h2, _ := NewHandler[*Event]("h2")
	tests := []struct {
		name      string
		h         *Handler[proto.Message]
		msg       *Message
		wantFalse bool
	}{
		{
			"MetaData handler, MetaData msg, success",
			h1,
			msg1,
			false,
		},
		{
			"Event handler, Event msg, success",
			h2,
			msg2,
			false,
		},
		{
			"MetaData handler, Event msg, success",
			h1,
			msg2,
			true,
		},
		{
			"Event handler, MetaData msg, success",
			h2,
			msg1,
			true,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			engine.AppendHandler(tt.h)
			b, _ := engine.Run(tt.msg)
			assert.Equal(t, b, tt.wantFalse)
		})
	}
}
