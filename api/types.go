package api

import (
	"fmt"

	"golang.org/x/net/context"
)



type StreamData struct {
	Id string
	Tuple map[interface{}]interface{}
}


type ProcElement func (ctx context.Context, data StreamData) error


type Processor interface {
	SetProcess(ProcElement)
	SetConcurrency(int)
	AddInput()
	Exec(context.Context) error
}

type Source interface {
	Init(context.Context)
	Uninit(context.Context)
	Output() <-chan interface{}
}

type Sink interface {
	AddInput(<-chan interface{})
	Inputs()[]<-chan interface{}	
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
