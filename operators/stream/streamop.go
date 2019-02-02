package stream

import (
	"context"
	"fmt"
	"github.com/emirpasic/gods/containers"
	"github.com/go-faces/logger"
	autoctx "github.com/gofunky/automi/api/context"
	"github.com/gofunky/automi/api/tuple"
	"github.com/gofunky/automi/util"
	"github.com/gofunky/pyraset/v2"
	"reflect"
)

// StreamOperator is an operator takes streamed items of type
// map, array, or slice and unpacks and emits each item individually
// downstream.
type StreamOperator struct {
	ctx    context.Context
	input  <-chan interface{}
	output chan interface{}
	log    logger.Interface
}

// New creates a *StreamOperator value
func New(ctx context.Context) *StreamOperator {
	log := autoctx.GetLogger(ctx)

	r := new(StreamOperator)
	r.ctx = ctx
	r.log = log
	r.output = make(chan interface{}, 1024)

	util.Log(r.log, "stream operator initialized")
	return r
}

// SetInput sets the input channel for the executor node
func (r *StreamOperator) SetInput(in <-chan interface{}) {
	r.input = in
}

// GetOutput returns the output channel of the executer node
func (r *StreamOperator) GetOutput() <-chan interface{} {
	return r.output
}

// Exec is the execution starting point for the executor node.
func (r *StreamOperator) Exec(drain chan<- error) {
	if r.input == nil {
		drain <- fmt.Errorf("no input channel found")
		return
	}

	go func() {
		defer func() {
			util.Log(r.log, "stream operator closing")
			close(r.output)
		}()
		for {
			select {
			case item, opened := <-r.input:
				if !opened {
					return
				}

				itemVal := reflect.ValueOf(item)

				switch item.(type) {
				case containers.Container:
					itemSet := item.(containers.Container)
					for _, subItem := range itemSet.Values() {
						r.output <- subItem
					}
				case mapset.Set:
					itemSet := item.(mapset.Set)
					for subItem := range itemSet.Iter() {
						r.output <- subItem
					}
				default:
					itemType := reflect.TypeOf(item)
					switch itemType.Kind() {
					case reflect.Array, reflect.Slice:
						for i := 0; i < itemVal.Len(); i++ {
							j := itemVal.Index(i)
							r.output <- j.Interface()
						}
					case reflect.Map:
						for _, key := range itemVal.MapKeys() {
							val := itemVal.MapIndex(key)
							r.output <- tuple.KV{key.Interface(), val.Interface()}
						}
					default:
						r.output <- item
					}
				}
			}
		}
	}()
}
