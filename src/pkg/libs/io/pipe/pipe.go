// Copyright 2016 CodisLabs. All Rights Reserved.
// Licensed under the MIT (MIT-LICENSE.txt) license.

package pipe

import (
	"io"
	"os"
	"sync"

	"github.com/alibaba/RedisShake/pkg/libs/errors"
)

type buffer interface {
	readSome(b []byte) (int, error)
	writeSome(b []byte) (int, error)

	buffered() int
	available() int

	rclose() error
	wclose() error
}

type pipe struct {
	rl sync.Mutex
	wl sync.Mutex
	mu sync.Mutex

	rwait *sync.Cond
	wwait *sync.Cond

	rerr error
	werr error

	store buffer
}

func roffset(blen int, size, rpos, wpos uint64) (maxlen, offset uint64) {
	maxlen = uint64(blen)
	if n := wpos - rpos; n < maxlen {
		maxlen = n
	}
	offset = rpos % size
	if n := size - offset; n < maxlen {
		maxlen = n
	}
	return
}

func woffset(blen int, size, rpos, wpos uint64) (maxlen, offset uint64) {
	maxlen = uint64(blen)
	if n := size + rpos - wpos; n < maxlen {
		maxlen = n
	}
	offset = wpos % size
	if n := size - offset; n < maxlen {
		maxlen = n
	}
	return
}

func align(size, unit int) int {
	if size < unit {
		return unit
	}
	return (size + unit - 1) / unit * unit
}

func newPipe(store buffer) (Reader, Writer) {
	p := &pipe{}
	p.rwait = sync.NewCond(&p.mu)
	p.wwait = sync.NewCond(&p.mu)
	p.store = store
	r := &reader{p}
	w := &writer{p}
	return r, w
}

func (p *pipe) Read(b []byte) (int, error) {
	p.rl.Lock()
	defer p.rl.Unlock()
	for {
		n, err := p.readSome(b)
		if err != nil || n != 0 {
			return n, err
		}
		if len(b) == 0 {
			return 0, nil
		}
	}
}

func (p *pipe) readSome(b []byte) (int, error) {
	p.mu.Lock()
	defer p.mu.Unlock()
	if p.rerr != nil {
		return 0, errors.Trace(io.ErrClosedPipe)
	}
	if len(b) == 0 {
		if p.store.buffered() != 0 {
			return 0, nil
		}
		return 0, p.werr
	}
	n, err := p.store.readSome(b)
	if err != nil || n != 0 {
		p.wwait.Signal()
		return n, err
	}
	if p.werr != nil {
		return 0, p.werr
	}
	p.rwait.Wait()
	return 0, nil
}

func (p *pipe) Write(b []byte) (int, error) {
	p.wl.Lock()
	defer p.wl.Unlock()
	var nn int
	for {
		n, err := p.writeSome(b)
		if err != nil {
			return nn + n, err
		}
		nn, b = nn+n, b[n:]
		if len(b) == 0 {
			return nn, nil
		}
	}
}

func (p *pipe) writeSome(b []byte) (int, error) {
	p.mu.Lock()
	defer p.mu.Unlock()
	if p.werr != nil {
		return 0, errors.Trace(io.ErrClosedPipe)
	}
	if p.rerr != nil {
		return 0, p.rerr
	}
	if len(b) == 0 {
		return 0, nil
	}
	n, err := p.store.writeSome(b)
	if err != nil || n != 0 {
		p.rwait.Signal()
		return n, err
	}
	p.wwait.Wait()
	return 0, nil
}

func (p *pipe) RClose(err error) error {
	if err == nil {
		err = errors.Trace(io.ErrClosedPipe)
	}
	p.mu.Lock()
	defer p.mu.Unlock()
	if p.rerr == nil {
		p.rerr = err
	}
	p.rwait.Signal()
	p.wwait.Signal()
	return p.store.rclose()
}

func (p *pipe) WClose(err error) error {
	if err == nil {
		err = errors.Trace(io.EOF)
	}
	p.mu.Lock()
	defer p.mu.Unlock()
	if p.werr == nil {
		p.werr = err
	}
	p.rwait.Signal()
	p.wwait.Signal()
	return p.store.wclose()
}

func (p *pipe) Buffered() (int, error) {
	p.mu.Lock()
	defer p.mu.Unlock()
	if p.rerr != nil {
		return 0, p.rerr
	}
	if n := p.store.buffered(); n != 0 {
		return n, nil
	}
	return 0, p.werr
}

func (p *pipe) Available() (int, error) {
	p.mu.Lock()
	defer p.mu.Unlock()
	if p.werr != nil {
		return 0, p.werr
	}
	if p.rerr != nil {
		return 0, p.rerr
	}
	return p.store.available(), nil
}

func New() (Reader, Writer) {
	return NewSize(BuffSizeAlign)
}

func NewSize(buffSize int) (Reader, Writer) {
	return newPipe(newMemBuffer(buffSize))
}

func NewFilePipe(fileSize int, f *os.File) (Reader, Writer) {
	return newPipe(newFileBuffer(fileSize, f))
}
