package api

type ReadStream interface {
	Get() <-chan interface{}
}
type defaultReadStream struct {
	ch chan interface{}
}

func NewReadStream(ch chan interface{}) ReadStream {
	return newDefaultReadStream(ch)
}

func newDefaultReadStream(ch chan interface{}) *defaultReadStream {
	return &defaultReadStream{
		ch: ch,
	}
}

func (s *defaultReadStream) Get() <-chan interface{} {
	return s.ch
}


type WriteStream interface {
	Put() chan<- interface{}
}

type defaultWriteStream struct {
	ch chan interface{}
}

func NewWriteStream(ch chan interface{}) WriteStream {
	return newDefaultWriteStream(ch)
}

func newDefaultWriteStream(ch chan interface{}) *defaultWriteStream {
	return &defaultWriteStream{
		ch: ch,
	}
}

func (s *defaultWriteStream) Put() chan<- interface{} {
	return s.ch
}

type ReadWriteStream interface {
	ReadStream
	WriteStream
}
