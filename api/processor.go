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

	input  ReadStream
	outCh  chan StreamData
	output ReadStream

	log       *logrus.Entry
	cancelled bool
	mutex     sync.RWMutex
}

func NewProcessor(ctx context.Context) Processor {
	return newDefaultProcessor(ctx)
}

func newDefaultProcessor(ctx context.Context) *defaultProcessor {
	outCh := make(chan StreamData, 1024)
	p := &defaultProcessor{
		concurrency: 1,
		outCh:       outCh,
		output:      NewReadStream(outCh),
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

func (p *defaultProcessor) SetInputStream(s ReadStream) {
	p.input = s
}

func (p *defaultProcessor) GetOutputStream() ReadStream {
	return p.output
}

func (p *defaultProcessor) Exec(ctx context.Context) error {
	// validate p
	if p.concurrency < 1 {
		p.concurrency = 1
	}

	p.log.Info("Execution started for component")

	exeCtx, cancel := context.WithCancel(ctx)

	go func() {
		defer func() {
			p.output.Close()
			p.log.Info("Shuttingdown component")
		}()

		var barrier sync.WaitGroup
		wgDelta := p.concurrency
		barrier.Add(wgDelta)

		for i := 0; i < p.concurrency; i++ { // workers
			go func(wg *sync.WaitGroup) {
				defer wg.Done()
				p.doProc(exeCtx, p.input.Get())
			}(&barrier)
		}

		wait := make(chan struct{})
		go func() {
			defer close(wait)
			barrier.Wait()
		}()

		select {
		case <-wait:
		case <-ctx.Done():
			p.log.Infof("Component [%s] cancelling...")
			cancel()
			p.mutex.Lock()
			p.cancelled = true
			p.mutex.Unlock()
			return
		}
	}()
	return nil
}

func (p *defaultProcessor) doProc(exeCtx context.Context, input <-chan StreamData) {
	if p.procElem == nil {
		return
	}

	for item := range input {
		err := p.procElem.Apply(exeCtx, item, NewWriteStream(p.outCh))
		if err != nil {
			p.log.Error(err)
		}

		p.mutex.Lock()
		if !p.cancelled {
			p.outCh <- item
		} else {
			return
		}
		p.mutex.Unlock()
	}
}
