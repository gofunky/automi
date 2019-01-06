package stream

import (
	"errors"
	"github.com/deckarep/golang-set"
	"github.com/gofunky/automi/api/tuple"
	"strings"
	"testing"
	"time"

	"github.com/gofunky/automi/collectors"
	"github.com/gofunky/automi/emitters"
)

func TestStream_Process(t *testing.T) {
	src := emitters.Slice([]string{"hello", "world"})
	snk := collectors.Slice()
	strm := New(src).Process(func(s string) string {
		return strings.ToUpper(s)
	}).Into(snk)

	select {
	case err := <-strm.Open():
		if err != nil {
			t.Fatal(err)
		}
		for _, data := range snk.Get() {
			val := data.(string)
			if val != "HELLO" && val != "WORLD" {
				t.Fatalf("got unexpected value %v of type %T", val, val)
			}
		}
	case <-time.After(50 * time.Millisecond):
		t.Fatal("Waited too long ...")
	}
}

func TestStream_Filter(t *testing.T) {
	src := emitters.Slice([]string{"HELLO", "WORLD", "HOW", "ARE", "YOU"})
	snk := collectors.Slice()
	strm := New(src).Filter(func(data string) bool {
		return !strings.Contains(data, "O")
	})
	strm.Into(snk)

	select {
	case err := <-strm.Open():
		if err != nil {
			t.Fatal(err)
		}
	case <-time.After(50 * time.Millisecond):
		t.Fatal("Waited too long ...")
	}
	if len(snk.Get()) != 1 {
		t.Fatal("Filter failed, expected 1 element, got ", len(snk.Get()))
	}
}

func TestStream_Map(t *testing.T) {

	src := emitters.Slice([]string{"HELLO", "WORLD", "HOW", "ARE", "YOU"})
	snk := collectors.Slice()
	strm := New(src).Map(func(data string) int {
		return len(data)
	}).Into(snk)

	count := 0

	select {
	case err := <-strm.Open():
		if err != nil {
			t.Fatal(err)
		}
		for _, data := range snk.Get() {
			val := data.(int)
			count += val
		}
	case <-time.After(50 * time.Millisecond):
		t.Fatal("Waited too long for stream to Open...")
	}

	if count != 19 {
		t.Fatal("Map failed, expected count 19, got ", count)
	}
}

func TestStream_Map_WithError(t *testing.T) {
	expectedErr := errors.New("test error")
	src := emitters.Slice([]string{"HELLO", "WORLD", "HOW", "ARE", "YOU"})
	snk := collectors.Slice()
	strm := New(src).Map(func(data string) (int, error) {
		return 0, expectedErr
	}).Into(snk)

	select {
	case err := <-strm.Open():
		if err != expectedErr {
			t.Fatal(err)
		}
	case <-time.After(50 * time.Millisecond):
		t.Fatal("Waited too long for stream to Open...")
	}
}

func TestStream_FlatMap(t *testing.T) {
	src := emitters.Slice([]string{"HELLO WORLD", "HOW ARE YOU?"})
	snk := collectors.Slice()
	strm := New(src).FlatMap(func(data string) []string {
		return strings.Split(data, " ")
	}).Into(snk)

	count := 0
	expected := 20

	select {
	case err := <-strm.Open():
		if err != nil {
			t.Fatal(err)
		}

		for _, data := range snk.Get() {
			vals := data.(string)
			count += len(vals)
		}

		if count != expected {
			t.Fatalf("Expecting %d words, got %d", expected, count)
		}
	case <-time.After(50 * time.Millisecond):
		t.Fatal("Waited too long ...")
	}
}

func TestStream_FlatMap_WithSet(t *testing.T) {
	src := emitters.Slice([]string{"HELLO WORLD", "HOW ARE YOU?"})
	snk := collectors.Slice()
	strm := New(src).FlatMap(func(data string) (result mapset.Set) {
		items := strings.Split(data, " ")
		result = mapset.NewSet()
		for _, item := range items {
			result.Add(item)
		}
		return result
	}).Into(snk)

	count := 0
	expected := 20

	select {
	case err := <-strm.Open():
		if err != nil {
			t.Fatal(err)
		}

		for _, data := range snk.Get() {
			vals := data.(string)
			count += len(vals)
		}

		if count != expected {
			t.Fatalf("Expecting %d words, got %d", expected, count)
		}
	case <-time.After(50 * time.Millisecond):
		t.Fatal("Waited too long ...")
	}
}

func TestStream_FlatMap_WithMap(t *testing.T) {
	src := emitters.Slice([]string{"HELLO WORLD", "HOW ARE YOU?"})
	snk := collectors.Slice()
	strm := New(src).FlatMap(func(data string) (result map[interface{}]interface{}) {
		items := strings.Split(data, " ")
		result = make(map[interface{}]interface{})
		for i, item := range items {
			result[i] = item
		}
		return result
	}).Into(snk)

	count := 0
	expected := 10

	select {
	case err := <-strm.Open():
		if err != nil {
			t.Fatal(err)
		}

		for _, data := range snk.Get() {
			vals := data.(tuple.KV)
			count += len(vals)
		}

		if count != expected {
			t.Fatalf("Expecting %d words, got %d", expected, count)
		}
	case <-time.After(50 * time.Millisecond):
		t.Fatal("Waited too long ...")
	}
}
