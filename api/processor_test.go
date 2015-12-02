package api

import (
	"testing"
)

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
}