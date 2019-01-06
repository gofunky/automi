package binary

import (
	"context"
	"testing"
	"time"

	"github.com/gofunky/automi/api"
	"github.com/gofunky/automi/testutil"
)

func TestBinaryOp_New(t *testing.T) {
	o := New(context.Background())

	if o.output == nil {
		t.Fatal("Missing output")
	}

	if o.op != nil {
		t.Fatal("Processing element should be nil")
	}

	if o.concurrency != 1 {
		t.Fatal("Concurrency should be initialized to 1.")
	}
}
func TestBinaryOp_Params(t *testing.T) {
	o := New(context.Background())
	op := api.BinFunc(func(ctx context.Context, op1, op2 interface{}) (interface{}, error) {
		return nil, nil
	})
	in := make(chan interface{})

	o.SetOperation(op)
	if o.op == nil {
		t.Fatal("process Elem not set")
	}

	o.SetConcurrency(4)
	if o.concurrency != 4 {
		t.Fatal("Concurrency not being set")
	}

	o.SetInput(in)
	if o.input == nil {
		t.Fatal("Input not being set")
	}

	if o.GetOutput() == nil {
		t.Fatal("Output not set")
	}
}

func TestBinaryOp_Exec(t *testing.T) {
	ctx, _ := context.WithCancel(context.Background())
	o := New(ctx)

	o.SetInitialState(0)
	op := api.BinFunc(func(ctx context.Context, op1, op2 interface{}) (interface{}, error) {
		init := op1.(int)
		items := op2.([]int)
		for _, item := range items {
			init += item
		}
		return init, nil
	})
	o.SetOperation(op)

	in := make(chan interface{})
	go func() {
		in <- []int{1}
		in <- []int{1, 2}
		in <- []int{1, 2, 3}
		close(in)
	}()
	o.SetInput(in)

	drain := make(chan error)
	o.Exec(drain)

	select {
	case out := <-o.GetOutput():
		select {
		case err := <-drain:
			t.Fatal(err)
		default:
		}
		val := out.(int)
		if val != 10 {
			t.Fatal("Values not adding up to expected 10")
		}
	case <-time.After(50 * time.Millisecond):
		t.Fatal("Took too long...")
	}
}

func BenchmarkBinaryOp_Exec(b *testing.B) {
	ctx := context.Background()
	o := New(ctx)
	N := b.N

	chanSize := func() int {
		if N == 1 {
			return N
		}
		return int(float64(0.5) * float64(N))
	}()

	in := make(chan interface{}, chanSize)
	o.SetInput(in)
	o.SetInitialState(0)
	go func() {
		for i := 0; i < N; i++ {
			in <- len(testutil.GenWord())
		}
		close(in)
	}()

	op := api.BinFunc(func(ctx context.Context, op1, op2 interface{}) (interface{}, error) {
		val0 := op1.(int)
		val1 := op2.(int)
		return val0 + val1, nil
	})
	o.SetOperation(op)

	// process output

	drain := make(chan error)
	o.Exec(drain)

	if err := <-drain; err != nil {
		b.Fatal("Error during execution:", err)
	}

	select {
	case out := <-o.GetOutput():
		val := out.(int)
		if val == 0 {
			b.Fatal("Numbers did not get added")
		}
	case <-time.After(time.Second * 60):
		b.Fatal("Took too long")
	}
}
