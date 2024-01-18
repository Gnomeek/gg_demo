package main

import (
	"google.golang.org/protobuf/proto"
)

type Message struct {
	uuid    string
	payload []byte
}

func (m Message) Unmarshal(dst proto.Message) error {
	return nil
}

type Filter func(*Message) (bool, error)

func FilterMessageBySchema[T proto.Message]() Filter {
	return func(m *Message) (bool, error) {
		var dst T
		if e := m.Unmarshal(dst); e != nil {
			return false, e
		}
		return true, nil
	}
}

type SubscribeEngine struct {
	handlers map[string]*Handler[proto.Message]
}

func newSubscribeEngine() *SubscribeEngine {
	return &SubscribeEngine{
		handlers: make(map[string]*Handler[proto.Message]),
	}
}

func (e *SubscribeEngine) AppendHandler(handlers ...*Handler[proto.Message]) error {
	for _, h := range handlers {
		e.handlers[h.HandlerName] = h
	}
	return nil
}

type Handler[T proto.Message] struct {
	HandlerName string
	Filters     []Filter
}

func NewHandler[T proto.Message](name string) (*Handler[proto.Message], error) {
	return &Handler[proto.Message]{
		HandlerName: name,
		Filters:     make([]Filter, 0),
	}, nil
}

func (h *Handler[T]) WithSchemaFilter() *Handler[T] {
	h.Filters = append(h.Filters, FilterMessageBySchema[T]())
	return h
}

func (e *SubscribeEngine) Run(msg *Message) (b bool, err error) {
	for _, v := range e.handlers {
		v = v.WithSchemaFilter()
		for _, f := range v.Filters {
			b, err = f(msg)
		}
	}
	return
}
