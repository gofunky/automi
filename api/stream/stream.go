package stream

import (
	"fmt"

	"github.com/Sirupsen/logrus"
	"github.com/vladimirvivien/automi/api"
	"golang.org/x/net/context"
)

type Stream struct {
	source api.StreamSource
	sink   api.StreamSink
	drain  <-chan interface{}
	ops    []api.Operator
	ctx    context.Context
	log    *logrus.Entry
}

func New() *Stream {
	s := &Stream{
		ops: make([]api.Operator, 0),
		log: logrus.WithField("Stream", "Default"),
		ctx: context.Background(),
	}
	return s
}

func (s *Stream) WithContext(ctx context.Context) *Stream {
	s.ctx = ctx
	return s
}

func (s *Stream) From(src api.StreamSource) *Stream {
	s.source = src
	return s
}

func (s *Stream) To(sink api.StreamSink) *Stream {
	s.sink = sink
	return s
}

// Transform is a general method used to apply transfomrmative
// operations to stream elements (i.e. filter, map, etc)
func (s *Stream) Transform(op api.UnOperation) *Stream {
	operator := api.NewUnaryOp(s.ctx)
	operator.SetOperation(op)
	s.ops = append(s.ops, operator)
	return s
}

type FilterFunc func(interface{}) bool

func (s *Stream) Filter(f FilterFunc) *Stream {
	op := api.UnFunc(func(ctx context.Context, data interface{}) interface{} {
		predicate := f(data)
		if !predicate {
			return nil
		}
		return data
	})
	return s.Transform(op)
}

// MapFunc type represents the Map operation
// A map operation takes one value and maps to another value
type MapFunc func(interface{}) interface{}

// Map takes one value and maps it to another value.
func (s *Stream) Map(f MapFunc) *Stream {
	op := api.UnFunc(func(ctx context.Context, data interface{}) interface{} {
		result := f(data)
		return result
	})
	return s.Transform(op)
}

// FlatMapFunc type represents the FlatMap operation.
// A flat map takes one value and should return a slice of values.
type FlatMapFunc func(interface{}) interface{}

// FlatMap similar to Map, however, expected to return a slice of values
// to downstream operator
func (s *Stream) FlatMap(f FlatMapFunc) *Stream {
	op := api.UnFunc(func(ctx context.Context, data interface{}) interface{} {
		result := f(data)
		return result
	})
	return s.Transform(op)
}

// Accumulate is a general method used to apply transfornative reduction
// operations to stream elements (i.e. reduce, collect, etc)
func (s *Stream) Accumulate(op api.BinOperation) *Stream {
	operator := api.NewBinaryOp(s.ctx)
	operator.SetOperation(op)
	s.ops = append(s.ops, operator)
	return s
}

func (s *Stream) SetInitialState(val interface{}) *Stream {
	lastOp := s.ops[len(s.ops)-1]
	binOp, ok := lastOp.(*api.BinaryOp)
	if !ok {
		panic("Unable to SetInitialState on last operator, wrong type")
	}
	binOp.SetInitialState(val)
	return s
}

// ReduceFunc represents a reduce operation where values are folded into a value
type ReduceFunc func(interface{}, interface{}) interface{}

// Reduce accumulates and reduce a stream of elements into a single value
func (s *Stream) Reduce(f ReduceFunc) *Stream {
	op := api.BinFunc(func(ctx context.Context, op1, op2 interface{}) interface{} {
		return f(op1, op2)
	})
	return s.Accumulate(op)
}

func (s *Stream) Open() <-chan error {
	result := make(chan error, 1)
	if err := s.initGraph(); err != nil {
		result <- err
		return result
	}

	// open stream
	go func() {
		// open source, if err bail
		if err := s.source.Open(s.ctx); err != nil {
			result <- err
			return
		}
		//apply operators, if err bail
		for _, op := range s.ops {
			if err := op.Exec(); err != nil {
				result <- err
				return
			}
		}
		// open sink, pipe result out
		err := <-s.sink.Open(s.ctx)
		result <- err
	}()

	return result
}

// bindOps binds operator channels
func (s *Stream) bindOps() {
	s.log.Debug("Binding operators")
	if s.ops == nil {
		return
	}
	for i, op := range s.ops {
		if i == 0 { // link 1st to source
			op.SetInput(s.source.GetOutput())
		} else {
			op.SetInput(s.ops[i-1].GetOutput())
		}
	}
}

// initGraph initialize stream graph source + ops +
func (s *Stream) initGraph() error {
	s.log.Infoln("Preparing stream operator graph")
	if s.source == nil {
		return fmt.Errorf("Operator graph failed, missing source")
	}

	// if there are no ops, link source to sink
	if len(s.ops) == 0 && s.sink != nil {
		s.log.Warnln("No operator nodes found, binding source to sink directly")
		s.sink.SetInput(s.source.GetOutput())
		return nil
	}

	// link ops
	s.bindOps()

	// link last op to sink
	if s.sink != nil {
		s.sink.SetInput(s.ops[len(s.ops)-1].GetOutput())
	}

	return nil
}