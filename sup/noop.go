package sup

import (
	"fmt"

	"github.com/Sirupsen/logrus"
	"github.com/vladimirvivien/automi/api"
	autoctx "github.com/vladimirvivien/automi/context"
	"golang.org/x/net/context"
)

// Noop represents a non-operational process.
// It simply return its input as the output channel.
// A Noop processor can be useful in testing and
// setting up an Automi process graph.
type Noop struct {
	Id string
	F api.Function
	log   *logrus.Entry
}

func (n *Noop) GetId() string {
	return n.Id
}

func (n *Noop) GetFunc() api.Function {
	return func (c context.Context, data api.ChannelData, out chan<- interface{}) error {
		return nil 
	} 
}

func (n *Noop) Init(ctx context.Context) error {
	log, ok := autoctx.GetLogEntry(ctx)
	if !ok {
		log = logrus.WithField("Proc", "Noop")
		log.Error("Logger not found in context")
	}

	n.log = log.WithFields(logrus.Fields{
		"Component": n.Id,
		"Type":      fmt.Sprintf("%T", n),
	})

	return nil
}

func (n *Noop) Exec(ctx context.Context) error {
	return nil
}


func (n *Noop) Uninit(ctx context.Context) error {
	return nil
}
