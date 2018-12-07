package emitters

import (
	"context"
	"errors"
	"reflect"

	"github.com/go-faces/logger"
	autoctx "github.com/gofunky/automi/api/context"
	"github.com/gofunky/automi/util"
)

// ChanEmitter is an emitter that takes in a channel and
// and sets it up as the source of the emitter .
type ChanEmitter struct {
	channel interface{}
	output  chan interface{}
	log     logger.Interface
}

// Chan creates new slice source
func Chan(channel interface{}) *ChanEmitter {
	return &ChanEmitter{
		channel: channel,
		output:  make(chan interface{}, 1024),
	}
}

//GetOutput returns the output channel of this source node
func (c *ChanEmitter) GetOutput() <-chan interface{} {
	return c.output
}

// Open opens the source node to start streaming data on its channel
func (c *ChanEmitter) Open(ctx context.Context) error {
	// ensure channel param is a chan type
	chanType := reflect.TypeOf(c.channel)
	if chanType.Kind() != reflect.Chan {
		return errors.New("ChanEmitter requires channel")
	}
	c.log = autoctx.GetLogger(ctx)
	util.Log(c.log, "opening channel emitter")
	chanVal := reflect.ValueOf(c.channel)

	if !chanVal.IsValid() {
		return errors.New("invalid channel for ChanEmitter")
	}

	go func() {
		defer func() {
			close(c.output)
			util.Log(c.log, "closing slice emitter")
		}()

		for {
			val, open := chanVal.Recv()
			if !open {
				return
			}
			c.output <- val.Interface()
		}
	}()
	return nil
}
