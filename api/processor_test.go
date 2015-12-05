package api

import (
	"testing"
	"time"
	"sync"

	"golang.org/x/net/context"

	"github.com/vladimirvivien/automi/testutil"
	"github.com/vladimirvivien/automi/api/tuple"
)

func TestDefaultProcessor_New(t *testing.T) {
	p := newDefaultProcessor(context.Background())

	if p.output == nil {
		t.Fatal("Missing output")
	}

	if p.op != nil {
		t.Fatal("Processing element should be nil")
	}

	if p.concurrency != 1 {
		t.Fatal("Concurrency should be initialized to 1.")
	}

}

func TestDefaultProcessor_Params(t *testing.T) {
	p := newDefaultProcessor(context.Background())
	op := OpFunc(func(ctx context.Context, data interface{}) interface{} {
		return nil
	})
	p.SetOp(op)
	if p.op == nil {
		t.Fatal("process Elem not set")
	}

	p.SetConcurrency(4)
	if p.concurrency != 4 {
		t.Fatal("Concurrency not being set")
	}

	if p.GetReadStream() == nil {
		t.Fatal("InputStream not initialized")
	}

	if p.GetWriteStream() == nil {
		t.Fatal("Outputstream not initialized")
	}
}

func TestDefaultProcessor_Exec(t *testing.T) {
	ctx, _ := context.WithCancel(context.Background())
	p := newDefaultProcessor(ctx)

	op := OpFunc(func(ctx context.Context, data interface{}) interface{} {
		values := data.(tuple.Tuple).Values()
		t.Logf("Processing data %v, sending %d", values, len(values))
		return len(values)
	})
	p.SetOp(op)

	in := p.GetWriteStream().Put()
	go func() {
		in <- tuple.New("A", "B", "C")
		in <- tuple.New("D", "E")
		in <- tuple.New("G")
		p.Close(ctx)
	}()

	wait := make(chan struct{})
	go func() {
		defer close(wait)
		for data := range p.GetReadStream().Get() {
			val, ok := data.(int)
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

	select{
	case <-wait:
	case <-time.After(50  * time.Millisecond):
		t.Fatal("Took too long...")
	}
}

func BenchmarkDefaultProcessor_Exec(b *testing.B) {
	ctx := context.Background()
	p := newDefaultProcessor(ctx)
	N := b.N
	in := p.GetWriteStream().Put()	

	go func() {
		for i := 0; i < N; i++ {
			in <- tuple.New(testutil.GenWord())
		}
		p.Close(ctx)
	}()

	counter := 0
	var m sync.RWMutex

	op := OpFunc(func(ctx context.Context, data interface{} ) interface{} {
		m.Lock()
		counter++
		m.Unlock()
		return data
	})
	p.SetOp(op)

	// process output
	done := make(chan struct{})
	go func() {
		defer close(done)
		for _ = range p.GetReadStream().Get() {
		}
	}()

	
	if err := p.Exec(ctx); err != nil {
		b.Fatal("Error during execution:", err)
	}


	select {
	case <-done:
	case <-time.After(time.Second * 60):
		b.Fatal("Took too long")
	}
	m.RLock()
	b.Logf("Input %d, counted %d", N, counter)
	if counter != N {
		b.Fatalf("Expected %d items processed,  got %d", N, counter)
	}
	m.RUnlock()
}
