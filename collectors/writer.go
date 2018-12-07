package collectors

import (
	"bufio"
	"context"
	"errors"
	"fmt"
	"io"

	"github.com/go-faces/logger"
	autoctx "github.com/gofunky/automi/api/context"
	"github.com/gofunky/automi/util"
)

type WriterCollector struct {
	wrtParam io.Writer
	writer   *bufio.Writer
	input    <-chan interface{}
	log      logger.Interface
}

func Writer(writer io.Writer) *WriterCollector {
	return &WriterCollector{
		wrtParam: writer,
	}
}

func (c *WriterCollector) SetInput(in <-chan interface{}) {
	c.input = in
}

func (c *WriterCollector) Open(ctx context.Context) <-chan error {
	c.log = autoctx.GetLogger(ctx)
	util.Log(c.log, "opening io.Writer collector")
	result := make(chan error)

	if err := c.setupWriter(); err != nil {
		go func() { result <- err }()
		return result
	}

	go func() {
		defer func() {
			if err := c.writer.Flush(); err != nil {
				go func() { result <- err }()
				return
			}
			close(result)
			util.Log(c.log, "closing io.Writer collector")
		}()

		for val := range c.input {
			switch data := val.(type) {
			case string:
				fmt.Fprint(c.writer, data)
			case []byte:
				if _, err := c.writer.Write(data); err != nil {
					util.Log(c.log, err)
					//TODO runtime error handling
					continue
				}
			default:
				// other types are serialized using string representation
				// extracted by fmt
				fmt.Fprintf(c.writer, "%v", data)
			}
		}
	}()

	return result
}

func (c *WriterCollector) setupWriter() error {
	if c.wrtParam == nil {
		return errors.New("missing io.Writer parameter")
	}
	c.writer = bufio.NewWriter(c.wrtParam)

	return nil
}
