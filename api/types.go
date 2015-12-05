package api

import (
	"fmt"

	"golang.org/x/net/context"
)

type Op interface {
	Apply(ctx context.Context, data interface{}) interface{}
}

type OpFunc func(context.Context, interface{}) interface{}

func (f OpFunc) Apply(ctx context.Context, data interface{}) interface{} {
	return f(ctx, data)
}

type Muxer interface {
	AddReadStream(ReadStream)
	SetWriteStream(WriteStream)
	Mux()
}

type Process interface {
	SetOp(Op)
	SetConcurrency(int)
	Exec(context.Context) error
	Close(context.Context) error	
}

type StreamProcessor interface {
	Process
	GetWriteStream() WriteStream
	GetReadStream() ReadStream
}

type StreamProducer interface {
	Process
	GetReadStream() ReadStream
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
