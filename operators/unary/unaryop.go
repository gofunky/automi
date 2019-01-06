package unary

import (
	"context"
	"errors"
	"fmt"
	"sync"

	"github.com/go-faces/logger"
	"github.com/gofunky/automi/api"
	autoctx "github.com/gofunky/automi/api/context"
	"github.com/gofunky/automi/util"
)

type packed struct {
	vals []interface{}
}

func pack(vals ...interface{}) packed {
	return packed{vals}
}

// UnaryOp is an executor node that can execute a unary operation (i.e. transformation, etc)
type UnaryOperator struct {
	ctx         context.Context
	op          api.UnOperation
	concurrency int
	input       <-chan interface{}
	output      chan interface{}
	log         logger.Interface
	cancelled   bool
	mutex       sync.RWMutex
}

// NewUnary creates *UnaryOperator value
func New(ctx context.Context) *UnaryOperator {
	// extract logger
	log := autoctx.GetLogger(ctx)

	o := new(UnaryOperator)
	o.ctx = ctx
	o.log = log

	o.concurrency = 1
	o.output = make(chan interface{}, 1024)

	util.Log(o.log, "unary operator initialized")
	return o
}

// SetOperation sets the executor operation
func (o *UnaryOperator) SetOperation(op api.UnOperation) {
	o.op = op
}

// SetConcurrency sets the concurrency level for the operation
func (o *UnaryOperator) SetConcurrency(concurr int) {
	o.concurrency = concurr
	if o.concurrency < 1 {
		o.concurrency = 1
	}
}

// SetInput sets the input channel for the executor node
func (o *UnaryOperator) SetInput(in <-chan interface{}) {
	o.input = in
}

// GetOutput returns the output channel for the executor node
func (o *UnaryOperator) GetOutput() <-chan interface{} {
	return o.output
}

// Exec is the entry point for the executor
func (o *UnaryOperator) Exec(drain chan<- error) {
	if o.input == nil {
		err := fmt.Errorf("no input channel found")
		drain <- err
		return
	}

	// validate p
	if o.concurrency < 1 {
		o.concurrency = 1
	}

	go func() {
		defer func() {
			util.Log(o.log, "unary operator closing")
			close(o.output)
		}()

		var barrier sync.WaitGroup
		wgDelta := o.concurrency
		barrier.Add(wgDelta)

		wait := make(chan struct{})
		go func(wg *sync.WaitGroup) {
			defer close(wait)
			wg.Wait()
		}(&barrier)

		for i := 0; i < o.concurrency; i++ {
			go func(wg *sync.WaitGroup) {
				defer wg.Done()
				if err := o.doProc(o.ctx); err != nil {
					drain <- err
				}
			}(&barrier)
		}

		select {
		case <-wait:
			if o.cancelled {
				util.Log(o.log, "unary operator cancelled")
				return
			}
		case <-o.ctx.Done():
			util.Log(o.log, "unary operator done")
			return
		}
	}()
}

func (o *UnaryOperator) doProc(ctx context.Context) error {
	if o.op == nil {
		err := errors.New("unary operator missing operation")
		util.Log(o.log, err)
		return err
	}
	exeCtx, cancel := context.WithCancel(ctx)

	for {
		select {
		// process incoming item
		case item, opened := <-o.input:
			if !opened {
				return nil
			}

			result, err := o.op.Apply(exeCtx, item)
			if err != nil {
				util.Log(o.log, err)
				return err
			}

			switch val := result.(type) {
			case nil:
				continue
			case error, api.ProcError:
				util.Log(o.log, val)
				continue
			default:
				o.output <- val
			}

		// is cancelling
		case <-ctx.Done():
			util.Log(o.log, "unary operator cancelling...")
			o.mutex.Lock()
			cancel()
			o.cancelled = true
			o.mutex.Unlock()
			return nil
		}
	}
}
