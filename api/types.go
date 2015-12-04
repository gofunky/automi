package api

import (
	"fmt"

	"golang.org/x/net/context"
)

type Tuple struct {
	Fields []string
	Values []interface{}
}

func NewTuple(vals ...interface{}) Tuple {
	return Tuple{
		Fields: make([]string, 0),
		Values: vals,
	}
}

func (t Tuple) WithValues(vals ...interface{}) Tuple {
	t.Values = append(t.Values, vals...)
	return t
}

func (t Tuple) WithFields(fds ...string) Tuple {
	t.Fields = append(t.Fields, fds...)
	return t
}

type StreamData struct {
	Tuple Tuple
}

func NewStreamData(t Tuple) StreamData {
	return StreamData{Tuple: t}
}

type ReadStream interface {
	Get() <-chan StreamData
}

type WriteStream interface {
	Put() chan<- StreamData
}

type ReadWriteStream interface {
	Get() <-chan StreamData
	Put() chan<- StreamData
	Close()
}

type ProcessingElement interface {
	Apply(ctx context.Context, data StreamData) interface{}
}

type ProcElemFunc func(context.Context, StreamData) interface{}

func (f ProcElemFunc) Apply(ctx context.Context, data StreamData) interface{} {
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
