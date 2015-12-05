package file

import (
	"encoding/csv"
	"fmt"
	"io"
	"os"

	"golang.org/x/net/context"

	"github.com/Sirupsen/logrus"
	"github.com/vladimirvivien/automi/api"
	autoctx "github.com/vladimirvivien/automi/context"
)

// CsvSource implements an Source process that reads the content of a
// specified file and emits its record via its Output Channel
// and serializes each row as a slice []string.
type CsvSource struct {
	Name          string                                         // string identifer for the Csv emitter
	FilePath      string                                         // path for the file
	DelimiterChar rune                                           // Delimiter charater, defaults to comma
	CommentChar   rune                                           // Charater indicating line is a comment
	Headers       []string                                       // Column header names (specified here or read from file)
	HasHeaderRow  bool                                           // indicates first row is for headers (default false). Overrides the Headers attribute.
	FieldCount    int                                            // if greater than zero is used to validate field count

	op api.Op
	file   *os.File
	reader *csv.Reader
	log    *logrus.Entry
	outCh chan interface{}
	output api.ReadStream
}

func (c *CsvSource) init(ctx context.Context) error {
	c.outCh = make(chan interface{}, 1024)
	c.output = api.NewReadStream(c.outCh)

	// extract logger
	log, ok := autoctx.GetLogEntry(ctx)
	if !ok {
		log = logrus.WithField("Proc", "CsvSource")
		log.Error("No logger found incontext")
	}
	c.log = log.WithFields(logrus.Fields{
		"Component": "CsvSource",
		"Type":      fmt.Sprintf("%T", c),
	}) 

	if c.FilePath == "" {
		return fmt.Errorf("Missing FilePath")
	}

	// establish defaults
	if c.DelimiterChar == 0 {
		c.DelimiterChar = ','
	}

	if c.CommentChar == 0 {
		c.CommentChar = '#'
	}

	// open file
	file, err := os.Open(c.FilePath)
	if err != nil {
		return api.ProcError{
			ProcName: c.Name,
			Err:      fmt.Errorf("Failed to open file: %s ", err),
		}
	}
	c.file = file
	
	c.reader = csv.NewReader(file)
	c.reader.Comment = c.CommentChar
	c.reader.Comma = c.DelimiterChar

	// resolve header and field count
	if c.HasHeaderRow {
		if headers, err := c.reader.Read(); err == nil {
			c.FieldCount = len(headers)
			c.Headers = headers
		} else {
			return api.ProcError{
				ProcName: "CsvSource",
				Err:      fmt.Errorf("Unable to read header row: %s", err),
			}
		}
	} else {
		if c.Headers != nil {
			c.FieldCount = len(c.Headers)
		}
	}
	log.Debug("HasHeaderRow ", c.HasHeaderRow, " Headers [", c.Headers, "]")

	c.log.Info("Component initiated OK: reading from file ", c.file.Name())

	return nil
}

func (c *CsvSource) SetOp(op api.Op){
	c.op = op
}

func (c *CsvSource) Close(ctx context.Context) error {
	close(c.outCh)
	if err := c.file.Close(); err != nil {
		return err
	}
	c.log.Info("Component closed")

	c.outCh = nil
	c.file = nil
	return nil
}

func (c *CsvSource) GetReadStream() api.ReadStream {
	return c.output
}

func (c *CsvSource) Exec(ctx context.Context) (err error) {
	exeCtx, cancel := context.WithCancel(ctx)
	// init
	if err = c.init(ctx); err != nil {
		return
	}

	c.log.Info("Execution started")

	go func() {
		defer func() {
			if err = c.Close(ctx); err != nil {
				c.log.Error(err)
			}
			c.log.Info("Execution completed")
		}()

		for {
			row, err := c.reader.Read()

			if err != nil {
				if err == io.EOF {
					return
				}
				perr := api.ProcError{
					Err:      fmt.Errorf("Error reading row: %s", err),
					ProcName: "CsvSource",
				}
				c.log.Error(perr)
				continue
			}

			// if operator provided apply it, else submit row downstream
			if c.op != nil {
				result := c.op.Apply(exeCtx, row)
				switch val := result.(type) {
				case nil:
					continue
				case error, api.ProcError:
					c.log.Error(val)
					continue
				default:
					c.outCh <- val
				}
			} else {
				c.outCh <- row
			}

			select {
			case <-ctx.Done():
				cancel()
				return
			default:
			}
		}
	}()

	return nil
}
