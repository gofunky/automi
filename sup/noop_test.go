package sup

import "testing"
import "golang.org/x/net/context"

func TestNoop(t *testing.T) {
	p := &Noop{Id:"Noop"}
	if err := p.Init(context.TODO()); err != nil {
		t.Fatal(err)
	}
}
