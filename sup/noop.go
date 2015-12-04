package sup

import (
	"fmt"

	"github.com/Sirupsen/logrus"
	_"github.com/vladimirvivien/automi/api"
	autoctx "github.com/vladimirvivien/automi/context"
	"golang.org/x/net/context"
)

// Noop represents a non-operational process.
// It simply return its input as the output channel.
// A Noop processor can be useful in testing and
// setting up an Automi process graph.
type Noop struct { 
	log   *logrus.Entry
}


func (n *Noop) Init(ctx context.Context) error {
	log, ok := autoctx.GetLogEntry(ctx)
	if !ok {
		log = logrus.WithField("Proc", "Noop")
		log.Error("Logger not found in context")
	}

	n.log = log.WithFields(logrus.Fields{
		"Component": "NoOp",
		"Type":      fmt.Sprintf("%T", n),
	})

	return nil
}

