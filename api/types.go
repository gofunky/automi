package api

import (
	"fmt"

	"golang.org/x/net/context"
)

type StreamData struct {
	Tuple map[interface{}]interface{}
}

type ReadStream interface {
	Get() <-chan StreamData
	Close()
}

type WriteStream interface {
	Put() chan<- StreamData
	Close()
}

type ReadWriteStream interface {
	Get() <-chan StreamData
	Put() chan<- StreamData
	Close()
}

type ProcessingElement interface {
	Apply(ctx context.Context, data StreamData, output WriteStream) error
}

type Processor interface {
	SetProcessingElement(ProcessingElement)
	SetConcurrency(int)

	AddInputStream(ReadStream)
	GetOutputStream() ReadStream

	Exec(context.Context) error
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
