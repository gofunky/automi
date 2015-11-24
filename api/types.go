package api

import (
	"fmt"

	"golang.org/x/net/context"
)



type ChannelData struct {
	Id string
	Source string
	Tuple map[interface{}]interface{}
}


type Function func (ctx context.Context, data ChannelData, out chan<- interface{}) error


type Process interface {
	GetId() string
	GetF() Function
	Init(context.Context) error
	Exec(context.Context) error
	Uninit(context.Context) error
}

type Source interface {
	Output() <-chan interface{}
}

type Sink interface {
	AddInput(<-chan interface{})
	Inputs()[]<-chan interface{}	
}

type Processor interface {
	Process
	Source
	Sink
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
