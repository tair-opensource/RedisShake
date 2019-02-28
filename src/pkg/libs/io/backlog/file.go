// Copyright 2016 CodisLabs. All Rights Reserved.
// Licensed under the MIT (MIT-LICENSE.txt) license.

package backlog

import (
	"os"

	"pkg/libs/errors"
)

const (
	FileSizeAlign = 1024 * 1024 * 4
)

type fileBuffer struct {
	f    *os.File
	size uint64
	wpos uint64
}

func newFileBuffer(fileSize int, f *os.File) *fileBuffer {
	n := align(fileSize, FileSizeAlign)
	if n <= 0 {
		panic("invalid backlog buffer size")
	}
	return &fileBuffer{f: f, size: uint64(n)}
}

func (p *fileBuffer) readSomeAt(b []byte, rpos uint64) (int, error) {
	if p.f == nil {
		return 0, errors.Trace(ErrClosedBacklog)
	}
	if rpos > p.wpos || rpos+p.size < p.wpos {
		return 0, errors.Trace(ErrInvalidOffset)
	}
	maxlen, offset := roffset(len(b), p.size, rpos, p.wpos)
	if maxlen == 0 {
		return 0, nil
	}
	n, err := p.f.ReadAt(b[:maxlen], int64(offset))
	return n, errors.Trace(err)
}

func (p *fileBuffer) writeSome(b []byte) (int, error) {
	if p.f == nil {
		return 0, errors.Trace(ErrClosedBacklog)
	}
	maxlen, offset := woffset(len(b), p.size, p.wpos)
	if maxlen == 0 {
		return 0, nil
	}
	n, err := p.f.WriteAt(b[:maxlen], int64(offset))
	p.wpos += uint64(n)
	return n, errors.Trace(err)
}

func (p *fileBuffer) dataRange() (rpos, wpos uint64) {
	if p.f == nil {
		return 0, 0
	} else {
		if p.wpos >= p.size {
			return p.wpos - p.size, p.wpos
		}
		return 0, p.wpos
	}
}

func (p *fileBuffer) close() error {
	if f := p.f; f != nil {
		p.f = nil
		defer f.Close()
		return errors.Trace(f.Truncate(0))
	}
	return nil
}
