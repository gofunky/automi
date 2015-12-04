package api

import (
	"fmt"

	"golang.org/x/net/context"
)

type ProcessingElement interface {
	Apply(ctx context.Context, data interface{}) interface{}
}

type ProcFunc func(context.Context, interface{}) interface{}

func (f ProcFunc) Apply(ctx context.Context, data interface{}) interface{} {
	return f(ctx, data)
}

type Muxer interface {
	AddReadStream(ReadStream)
	SetWriteStream(WriteStream)
	Mux()
}

type Processor interface {
	// surface ports
	GetWriteStream() WriteStream
	GetReadStream() ReadStream

	// config params
	SetProcessingElement(ProcessingElement)
	SetConcurrency(int)

	// behavior
	Exec(context.Context) error
	Close(context.Context) error
}

type Source interface {
	Init(context.Context)
	Uninit(context.Context)
	Output() <-chan interface{}
}

type Sink interface {
	AddInput(<-chan interface{})
	Inputs() []<-chan interface{}
}

type Endpoint interface {
	Done() <-chan struct{}
}

type Collector interface {
	SetInputs([]<-chan interface{})
}

type Emitter interface {
	GetOutputs() []<-chan interface{}
}

type ProcError struct {
	Err      error
	ProcName string
}

func (e ProcError) Error() string {
	if e.ProcName != "" {
		return fmt.Sprintf("[%s] %v", e.ProcName, e.Err)
	}
	return e.Err.Error()
}
