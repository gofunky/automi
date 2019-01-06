package stream

import (
	"errors"
	"testing"
	"time"

	"github.com/gofunky/automi/collectors"
	"github.com/gofunky/automi/emitters"
)

func TestStream_Reduce(t *testing.T) {
	snk := collectors.Slice()
	strm := New(emitters.Slice([]int{1, 2, 3, 4, 5})).Reduce(0, func(op1, op2 int) int {
		return op1 + op2
	}).Into(snk)

	actual := 15

	select {
	case err := <-strm.Open():
		if err != nil {
			t.Fatal(err)
		}
		val := snk.Get()[0].(int)
		if val != actual {
			t.Fatal("expecting ", actual, "got", val)
		}
	case <-time.After(10 * time.Millisecond):
		t.Fatal("Took too long")
	}
}

func TestStream_Reduce_WithError(t *testing.T) {
	snk := collectors.Slice()
	strm := New(emitters.Slice([]int{1, 2, 3, 4, 5})).Reduce(0, func(op1, op2 int) (int, error) {
		return 0, errors.New("test error")
	}).Into(snk)

	select {
	case err := <-strm.Open():
		if err == nil {
			t.Fatal(err)
		}
	case <-time.After(10 * time.Millisecond):
		t.Fatal("Took too long")
	}
}
