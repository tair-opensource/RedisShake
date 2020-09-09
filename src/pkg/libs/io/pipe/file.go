// Copyright 2016 CodisLabs. All Rights Reserved.
// Licensed under the MIT (MIT-LICENSE.txt) license.

package pipe

import (
	"io"
	"os"

	"github.com/alibaba/RedisShake/pkg/libs/errors"
)

const (
	FileSizeAlign = 1024 * 1024 * 4
)

type fileBuffer struct {
	f    *os.File
	size uint64
	rpos uint64
	wpos uint64
}

func newFileBuffer(fileSize int, f *os.File) *fileBuffer {
	n := align(fileSize, FileSizeAlign)
	if n <= 0 {
		panic("invalid pipe buffer size")
	}
	return &fileBuffer{f: f, size: uint64(n)}
}

func (p *fileBuffer) readSome(b []byte) (int, error) {
	if p.f == nil {
		return 0, errors.Trace(io.ErrClosedPipe)
	}
	maxlen, offset := roffset(len(b), p.size, p.rpos, p.wpos)
	if maxlen == 0 {
		return 0, nil
	}
	n, err := p.f.ReadAt(b[:maxlen], int64(offset))
	p.rpos += uint64(n)
	if p.rpos == p.wpos {
		p.rpos = 0
		p.wpos = 0
		if err == nil {
			err = p.f.Truncate(0)
		}
	}
	return n, errors.Trace(err)
}

func (p *fileBuffer) writeSome(b []byte) (int, error) {
	if p.f == nil {
		return 0, errors.Trace(io.ErrClosedPipe)
	}
	maxlen, offset := woffset(len(b), p.size, p.rpos, p.wpos)
	if maxlen == 0 {
		return 0, nil
	}
	n, err := p.f.WriteAt(b[:maxlen], int64(offset))
	p.wpos += uint64(n)
	return n, errors.Trace(err)
}

func (p *fileBuffer) buffered() int {
	if p.f == nil {
		return 0
	}
	return int(p.wpos - p.rpos)
}

func (p *fileBuffer) available() int {
	if p.f == nil {
		return 0
	}
	return int(p.size + p.rpos - p.wpos)
}

func (p *fileBuffer) rclose() error {
	if f := p.f; f != nil {
		p.f = nil
		defer f.Close()
		return errors.Trace(f.Truncate(0))
	}
	return nil
}

func (p *fileBuffer) wclose() error {
	return nil
}
