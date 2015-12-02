package api

import (
	"testing"

	"golang.org/x/net/context"
)

func TestDefaultProcessor_New(t *testing.T) {
	p := newDefaultProcessor(context.Background())

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

func TestDefaultProcessor_Params(t *testing.T) {
	p := newDefaultProcessor(context.Background())
	pe := ProcElemFunc(func(ctx context.Context, data StreamData, out WriteStream) error {
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

	in := NewReadStream(make(chan StreamData))
	p.SetInputStream(in)
	if p.input != in {
		t.Fatal("InputStream not added properly")
	}

	if p.GetOutputStream == nil {
		t.Fatal("Outputstream not set")
	}
}

func TestDefaultProcessor_Exec_1_Input(t *testing.T) {
	in := make(chan StreamData)
	go func() {
		in <- StreamData{Tuple: NewTuple("A", "B", "C")}
		in <- StreamData{Tuple: NewTuple("D", "E")}
		in <- StreamData{Tuple: NewTuple("G")}
		close(in)
	}()

	p := newDefaultProcessor(context.Background())

	pe := ProcElemFunc(func(ctx context.Context, data StreamData, out WriteStream) error {
		tuple := data.Tuple.Values
		t.Logf("Processing data %v", tuple)
		out.Put() <- StreamData{Tuple: NewTuple(len(tuple))}
		return nil
	})

	p.SetInputStream(NewReadStream(in))
	p.SetProcessingElement(pe)

	wait := make(chan struct{})
	go func() {
		defer close(wait)
		for data := range p.GetOutputStream().Get() {
			val, ok := data.Tuple.Values[0].(int)
			t.Logf("Got value %v", val)
			if !ok {
				t.Fatalf("Expeting type int, got %T, value %v", val, val)
			}
			if val != 3 && val != 2 && val != 1 {
				t.Fatalf("Expecting values 3, 2, or 1, but got %d", val)
			}
		}
	}()

	if err := p.Exec(context.Background()); err != nil {
		t.Fatal(err)
	}

	<-wait

}
