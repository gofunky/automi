package api

import (
	"fmt"
	"sync"

	"golang.org/x/net/context"

	"github.com/Sirupsen/logrus"
	autoctx "github.com/vladimirvivien/automi/context"
)

type defaultProcessor struct {
	op    Op
	concurrency int

	inCh chan interface{}
	input  WriteStream

	outCh chan interface{}
	output ReadStream

	log       *logrus.Entry
	cancelled bool
	mutex     sync.RWMutex
}

func NewProcessor(ctx context.Context) StreamProcessor {
	return newDefaultProcessor(ctx)
}

func newDefaultProcessor(ctx context.Context) *defaultProcessor {
	inCh := make(chan interface{}, 1024)
	outCh := make(chan interface{}, 1024)

	p := &defaultProcessor{
		concurrency: 1,
		inCh:       inCh,
		input:      NewWriteStream(inCh),
		outCh: outCh,
		output: NewReadStream(outCh),
	}

	log, ok := autoctx.GetLogEntry(ctx)
	if !ok {
		log = logrus.WithField("Component", "DefaultProcessor")
		log.Error("Logger not found in context")
	}
	p.log = log.WithFields(logrus.Fields{
		"Component": "DefaultProcessor",
		"Type":      fmt.Sprintf("%T", p),
	})

	return p
}

func (p *defaultProcessor) SetOp(op Op) {
	p.op = op
}

func (p *defaultProcessor) SetConcurrency(concurr int) {
	p.concurrency = concurr
}

func (p *defaultProcessor) GetWriteStream() WriteStream{
	return p.input
}

func (p *defaultProcessor) GetReadStream() ReadStream {
	return p.output
}

func (p *defaultProcessor) Close(ctx context.Context) error {
	close(p.inCh)
	p.inCh = nil
	return nil
}

func (p *defaultProcessor) Exec(ctx context.Context) error {
	// validate p
	if p.concurrency < 1 {
		p.concurrency = 1
	}

	p.log.Info("Execution started for component")

	go func() {
		defer func() {
			close(p.outCh)
			p.log.Info("Shuttingdown component")
		}()

		var barrier sync.WaitGroup
		wgDelta := p.concurrency
		barrier.Add(wgDelta)

		for i := 0; i < p.concurrency; i++ { // workers
			go func(wg *sync.WaitGroup) {
				defer wg.Done()
				p.doProc(ctx, p.inCh)
			}(&barrier)
		}

		wait := make(chan struct{})
		go func() {
			defer close(wait)
			barrier.Wait()
		}()

		select {
		case <-wait:
			if p.cancelled {
				p.log.Infof("Component [%s] cancelling...")
				return
			}
		case <-ctx.Done():
			p.log.Info("Cancelled!")
			return
		}
	}()
	return nil
}

func (p *defaultProcessor) doProc(ctx context.Context, input <-chan interface{}) {
	if p.op == nil {
		p.log.Error("No processing function installed, exiting.")
		return
	}

	exeCtx, cancel := context.WithCancel(ctx)

	for  {
		select {
		// process incoming item
		case item, opened := <- input:
			if !opened {
				return
			}
			result := p.op.Apply(exeCtx, item)

			switch val := result.(type) {
			case nil:
				continue
			case error, ProcError:
				p.log.Error(val)
				continue
			default:
				p.outCh <- val
			}

		// is cancelling	
		case <- ctx.Done():
			p.log.Infoln("Cancelling....")
			p.mutex.Lock()
			cancel()
			p.cancelled = true
			p.mutex.Unlock()
			return
		}
	}
}