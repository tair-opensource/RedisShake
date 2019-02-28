// Copyright 2016 CodisLabs. All Rights Reserved.
// Licensed under the MIT (MIT-LICENSE.txt) license.

package backlog

import (
	"os"
	"sync"

	"pkg/libs/errors"
)

var (
	ErrClosedBacklog = errors.New("closed backlog")
	ErrInvalidOffset = errors.New("invalid offset")
)

type buffer interface {
	readSomeAt(b []byte, rpos uint64) (int, error)
	writeSome(b []byte) (int, error)

	dataRange() (rpos, wpos uint64)

	close() error
}

type Backlog struct {
	wl sync.Mutex
	mu sync.Mutex

	err error

	rwait *sync.Cond
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

func woffset(blen int, size, wpos uint64) (maxlen, offset uint64) {
	maxlen = uint64(blen)
	if size < maxlen {
		maxlen = size
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

func newBacklog(store buffer) *Backlog {
	bl := &Backlog{}
	bl.rwait = sync.NewCond(&bl.mu)
	bl.store = store
	return bl
}

func (bl *Backlog) ReadAt(b []byte, offset uint64) (int, error) {
	for {
		n, err := bl.readSomeAt(b, offset)
		if err != nil || n != 0 {
			return n, err
		}
		if len(b) == 0 {
			return 0, nil
		}
	}
}

func (bl *Backlog) readSomeAt(b []byte, rpos uint64) (int, error) {
	bl.mu.Lock()
	defer bl.mu.Unlock()
	if bl.store == nil {
		return 0, errors.Trace(ErrClosedBacklog)
	}
	if len(b) == 0 || bl.err != nil {
		return 0, bl.err
	}
	n, err := bl.store.readSomeAt(b, rpos)
	if err != nil || n != 0 {
		return n, err
	}
	bl.rwait.Wait()
	return 0, nil
}

func (bl *Backlog) Write(b []byte) (int, error) {
	bl.wl.Lock()
	defer bl.wl.Unlock()
	var nn int
	for {
		n, err := bl.writeSome(b)
		if err != nil {
			return nn + n, err
		}
		nn, b = nn+n, b[n:]
		if len(b) == 0 {
			return nn, nil
		}
	}
}

func (bl *Backlog) writeSome(b []byte) (int, error) {
	bl.mu.Lock()
	defer bl.mu.Unlock()
	if bl.store == nil {
		return 0, errors.Trace(ErrClosedBacklog)
	}
	if len(b) == 0 || bl.err != nil {
		return 0, bl.err
	}
	n, err := bl.store.writeSome(b)
	if err != nil || n != 0 {
		bl.rwait.Broadcast()
		return n, err
	}
	return 0, nil
}

func (bl *Backlog) Close() error {
	return bl.CloseWithError(nil)
}

func (bl *Backlog) CloseWithError(err error) error {
	if err == nil {
		err = errors.Trace(ErrClosedBacklog)
	}
	bl.mu.Lock()
	defer bl.mu.Unlock()
	if bl.err != nil {
		bl.err = err
	}
	bl.rwait.Broadcast()
	if bl.store != nil {
		return bl.store.close()
	}
	return nil
}

func (bl *Backlog) DataRange() (rpos, wpos uint64, err error) {
	bl.mu.Lock()
	defer bl.mu.Unlock()
	if bl.store == nil {
		return 0, 0, errors.Trace(ErrClosedBacklog)
	}
	if bl.err != nil {
		return 0, 0, bl.err
	}
	rpos, wpos = bl.store.dataRange()
	return rpos, wpos, nil
}

func (bl *Backlog) NewReader() (*Reader, error) {
	bl.mu.Lock()
	defer bl.mu.Unlock()
	if bl.store == nil {
		return nil, errors.Trace(ErrClosedBacklog)
	}
	if bl.err != nil {
		return nil, bl.err
	}
	_, wpos := bl.store.dataRange()
	return &Reader{bl: bl, seek: wpos}, nil
}

type Reader struct {
	bl   *Backlog
	seek uint64
}

func (r *Reader) Read(b []byte) (int, error) {
	n, err := r.bl.ReadAt(b, r.seek)
	r.seek += uint64(n)
	return n, err
}

func (r *Reader) DataRange() (rpos, wpos uint64, err error) {
	return r.bl.DataRange()
}

func (r *Reader) IsValid() bool {
	rpos, wpos, err := r.DataRange()
	if err != nil {
		return false
	}
	return r.seek >= rpos && r.seek <= wpos
}

func (r *Reader) Offset() uint64 {
	return r.seek
}

func (r *Reader) SeekTo(seek uint64) bool {
	r.seek = seek
	return r.IsValid()
}

func New() *Backlog {
	return NewSize(BuffSizeAlign)
}

func NewSize(buffSize int) *Backlog {
	return newBacklog(newMemBuffer(buffSize))
}

func NewFileBacklog(fileSize int, f *os.File) *Backlog {
	return newBacklog(newFileBuffer(fileSize, f))
}
