// Copyright 2016 CodisLabs. All Rights Reserved.
// Licensed under the MIT (MIT-LICENSE.txt) license.

package backlog

import "pkg/libs/errors"

const (
	BuffSizeAlign = 1024 * 4
)

type memBuffer struct {
	b    []byte
	size uint64
	wpos uint64
}

func newMemBuffer(buffSize int) *memBuffer {
	n := align(buffSize, BuffSizeAlign)
	if n <= 0 {
		panic("invalid backlog buffer size")
	}
	return &memBuffer{b: make([]byte, n), size: uint64(n)}
}

func (p *memBuffer) readSomeAt(b []byte, rpos uint64) (int, error) {
	if p.b == nil {
		return 0, errors.Trace(ErrClosedBacklog)
	}
	if rpos > p.wpos || rpos+p.size < p.wpos {
		return 0, errors.Trace(ErrInvalidOffset)
	}
	maxlen, offset := roffset(len(b), p.size, rpos, p.wpos)
	if maxlen == 0 {
		return 0, nil
	}
	n := copy(b, p.b[offset:offset+maxlen])
	return n, nil
}

func (p *memBuffer) writeSome(b []byte) (int, error) {
	if p.b == nil {
		return 0, errors.Trace(ErrClosedBacklog)
	}
	maxlen, offset := woffset(len(b), p.size, p.wpos)
	if maxlen == 0 {
		return 0, nil
	}
	n := copy(p.b[offset:offset+maxlen], b)
	p.wpos += uint64(n)
	return n, nil
}

func (p *memBuffer) dataRange() (rpos, wpos uint64) {
	if p.b == nil {
		return 0, 0
	} else {
		if p.wpos >= p.size {
			return p.wpos - p.size, p.wpos
		}
		return 0, p.wpos
	}
}

func (p *memBuffer) close() error {
	p.b = nil
	return nil
}
