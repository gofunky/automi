package api

import (
	"fmt"
	"sync"

	"golang.org/x/net/context"

	"github.com/Sirupsen/logrus"
	autoctx "github.com/vladimirvivien/automi/context"
)

type defaultProcessor struct {
	procElem    ProcessingElement
	concurrency int

	inCh chan StreamData
	input  WriteStream

	outCh chan StreamData
	output ReadStream

	log       *logrus.Entry
	cancelled bool
	mutex     sync.RWMutex
}

func NewProcessor(ctx context.Context) Processor {
	return newDefaultProcessor(ctx)
}

func newDefaultProcessor(ctx context.Context) *defaultProcessor {
	inCh := make(chan StreamData, 1024)
	outCh := make(chan StreamData, 1024)

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

func (p *defaultProcessor) SetProcessingElement(elem ProcessingElement) {
	p.procElem = elem
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

func (p *defaultProcessor) doProc(ctx context.Context, input <-chan StreamData) {
	if p.procElem == nil {
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
			result := p.procElem.Apply(exeCtx, item)

			switch val := result.(type) {
			case nil:
				continue
			case ProcError:
				p.log.Error(val)
				continue
			case StreamData:
				p.outCh <- val
			default:
				p.outCh <- StreamData{Tuple: NewTuple(val)}
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


func (p *defaultProcessor) Close(ctx context.Context) error {
	_, cancel := context.WithCancel(ctx)
	cancel()
	close(p.inCh)
	return nil
}