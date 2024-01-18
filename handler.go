package main

import (
	watermill "github.com/ThreeDotsLabs/watermill/message"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/types/known/anypb"
)

type Message struct {
	uuid    string
	payload []byte
}

func (m Message) Unmarshal(dst proto.Message) error {
	event := &Event{}
	if e := proto.Unmarshal(m.payload, event); e != nil {
		return e
	}
	if e := anypb.UnmarshalTo(event.Payload, dst, proto.UnmarshalOptions{}); e != nil {
		return e
	}
	return nil
}

type Filter func(*Message) (bool, error)

func (f Filter) Middleware(h watermill.HandlerFunc) watermill.HandlerFunc {
	return func(msg *watermill.Message) ([]*watermill.Message, error) {
		return h(msg)
	}
}

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
	handlers map[string]any
	topic    string
}

func newSubscribeEngine() *SubscribeEngine {
	return &SubscribeEngine{
		handlers: make(map[string]any),
	}
}

func ProvideHandlers[T proto.Message](e *SubscribeEngine, handlers ...*Handler[T]) error {
	for _, h := range handlers {
		e.handlers[h.HandlerName] = h
	}
	return nil
}

func InvokeHandler[T proto.Message](e *SubscribeEngine, name string) *Handler[T] {
	h := e.handlers[name]
	return h.(*Handler[T])
}

type Handler[T proto.Message] struct {
	HandlerName string
	Filters     []Filter
	HandlerFunc watermill.NoPublishHandlerFunc
	middlewares []watermill.HandlerMiddleware
}

func NewHandler[T proto.Message](name string) (*Handler[T], error) {
	return &Handler[T]{
		HandlerName: name,
		Filters:     make([]Filter, 0),
	}, nil
}

func (h *Handler[T]) WithSchemaFilter() *Handler[T] {
	h.Filters = append(h.Filters, FilterMessageBySchema[T]())
	return h
}

func (h *Handler[T]) beforeBindHook() *Handler[T] {
	for _, f := range h.Filters {
		h.middlewares = append(h.middlewares, f.Middleware)
	}
	return h
}

func bindWishSubscribeEngine[T proto.Message](
	router *watermill.Router,
	engine *SubscribeEngine,
	sub watermill.Subscriber,
	handlerName string
) {
	h := InvokeHandler[T](engine, handlerName)
	h.beforeBindHook()
	router.AddNoPublisherHandler(
		k,
		engine.topic,
		sub,
		h.HandlerFunc,
	).AddMiddleware(h.middlewares...)
}
