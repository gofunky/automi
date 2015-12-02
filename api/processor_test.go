package api

import (
	"testing"

	"golang.org/x/net/context"
)

type testPE func (context.Context, StreamData, WriteStream) error
func (f testPE) Apply(ctx context.Context, data StreamData, out WriteStream) error {
	return f(ctx, data, out)
}


func TestDefaultProcessor_New(t *testing.T) {
	p := newDefaultProcessor()
	if p.input == nil {
		t.Fatal("Missing input")
	}

		if len(p.input) != 0 {
		t.Fatal("Input len should be zero")
	}

	if p.output == nil {
		t.Fatal("Missing output")
	}

	if p.procElem != nil {
		t.Fatal("Processing element should be nil")
	}

	if p.concurrency != 1 {
		t.Fatal("Concurrency should be initialized to 1.")
	}


}

func TestDefaultProcessor_SetParams(t *testing.T) {
	p := newDefaultProcessor()
	pe := testPE(func(ctx context.Context, data StreamData, out WriteStream) error {
		return nil
	})
	p.SetProcessingElement(pe)
	if p.procElem == nil {
		t.Fatal("process Elem not set")
	}

	p.SetConcurrency(4)
	if p.concurrency != 4 {
		t.Fatal("Concurrency not being set")
	}

	p.AddInputStream(NewReadStream(make(chan StreamData)))
	if len(p.input) != 1 {
		t.Fatal("Input not added properly")
	}
}