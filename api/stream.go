package api

type defaultReadStream struct {
	ch chan StreamData
}

func NewReadStream(ch chan StreamData) ReadStream {
	return newDefaultReadStream(ch)
}

func newDefaultReadStream(ch chan StreamData) *defaultReadStream {
	return &defaultReadStream{
		ch: ch,
	}
}

func (s *defaultReadStream) Get() <-chan StreamData {
	return s.ch
}

func (s *defaultReadStream) Close() {
	if s.ch != nil {
		close(s.ch)
	}
}

type defaultWriteStream struct {
	ch chan StreamData
}

func NewWriteStream(ch chan StreamData) WriteStream {
	return newDefaultWriteStream(ch)
}

func newDefaultWriteStream(ch chan StreamData) *defaultWriteStream {
	return &defaultWriteStream{
		ch: ch,
	}
}

func (s *defaultWriteStream) Put() chan<- StreamData {
	return s.ch
}

func (s *defaultWriteStream) Close() {
	if s.ch != nil {
		close(s.ch)
	}
}
