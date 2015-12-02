package api

import (
	"fmt"
	"sync"

	"golang.org/x/net/context"

	"github.com/Sirupsen/logrus"
	_ "github.com/vladimirvivien/automi/context"
)

type defaultProcessor struct {
	procElem    ProcessingElement
	concurrency int

	input  []ReadStream
	outCh  chan StreamData
	output ReadStream

	log       *logrus.Entry
	cancelled bool
	mutex     sync.RWMutex
}

func NewProcessor() Processor {
	return newDefaultProcessor()
}

func newDefaultProcessor() *defaultProcessor {
	ch := make(chan StreamData, 1024)
	return &defaultProcessor{
		input:  make([]ReadStream, 0),
		outCh:  ch,
		output: NewReadStream(ch),
	}
}

func (p *defaultProcessor) SetProcessingElement(elem ProcessingElement) {
	p.procElem = elem
}

func (p *defaultProcessor) SetConcurrency(concurr int) {
	p.concurrency = concurr
}

func (p *defaultProcessor) AddInputStream(s ReadStream) {
	p.input = append(p.input, s)
}

func (p *defaultProcessor) GetOutputStream() ReadStream {
	return p.output
}

func (p *defaultProcessor) Exec(ctx context.Context) error {
	// validate p
	inputCount := len(p.input)
	if len(p.input) == 0 {
		return fmt.Errorf("No input stream found")
	}

	if p.concurrency < 1 {
		p.concurrency = 1
	}

	p.log.Info("Execution started for process")

	exeCtx, cancel := context.WithCancel(ctx)

	go func() {
		defer func() {
			p.output.Close()
			p.log.Info("Shuttingdown default processor")
		}()

		var barrier sync.WaitGroup
		wgDelta := p.concurrency * inputCount
		barrier.Add(wgDelta)

		for j := 0; j < len(p.input); j++ { // input
			inCh := p.input[j].Get()
			for i := 0; i < p.concurrency; i++ { // workers
				go func(wg *sync.WaitGroup) {
					defer wg.Done()
					p.doProc(exeCtx, inCh)
				}(&barrier)
			}
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
