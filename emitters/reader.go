package emitters

import (
	"context"
	"errors"
	"io"

	"github.com/go-faces/logger"
	autoctx "github.com/gofunky/automi/api/context"
	"github.com/gofunky/automi/util"
)

// ReaderEmitter takes an io.Reader as its source and emits a slice of
// bytes, N length, with each iteration.
type ReaderEmitter struct {
	reader io.Reader
	size   int
	output chan interface{}
	log    logger.Interface
}

// Reader returns a *ReaderEmitter which can be used to emit bytes
func Reader(reader io.Reader) *ReaderEmitter {
	return &ReaderEmitter{
		reader: reader,
		output: make(chan interface{}, 1024),
	}
}

// BufferSize sets the size of the transfer buffer used to
// read from the source io.Reader.
func (e *ReaderEmitter) BufferSize(s int) *ReaderEmitter {
	e.size = s
	return e
}

// GetOutput returns the output channel of this source node
func (e *ReaderEmitter) GetOutput() <-chan interface{} {
	return e.output
}

// Open opens the emitter to start emitting data
func (e *ReaderEmitter) Open(ctx context.Context) error {
	if err := e.setupReader(); err != nil {
		return err
	}

	// grab logger
	e.log = autoctx.GetLogger(ctx)
	util.Log(e.log, "opening io.Reader emitter")

	go func() {
		defer close(e.output)

		for {
			buf := make([]byte, e.size)
			bytesRead, err := e.reader.Read(buf)

			if bytesRead > 0 {
				select {
				case e.output <- buf[0:bytesRead]:
				case <-ctx.Done():
					return
				}
			}
			if err != nil {
				if err != io.EOF {
					// TODO handle error
					util.Log(e.log, err)
				}
				return
			}
		}
	}()
	return nil
}

func (e *ReaderEmitter) setupReader() error {
	if e.reader == nil {
		return errors.New("emitter missing io.Reader source")
	}
	if e.size <= 0 {
		e.size = 10 * 1024 // default 10k buffer
	}
	return nil
}
