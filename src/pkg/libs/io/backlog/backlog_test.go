// Copyright 2016 CodisLabs. All Rights Reserved.
// Licensed under the MIT (MIT-LICENSE.txt) license.

package backlog

import (
	"bytes"
	"io"
	"math/rand"
	"os"
	"testing"
	"time"

	"pkg/libs/assert"
	"pkg/libs/errors"
)

func openFile(fileName string) *os.File {
	f, err := os.OpenFile(fileName, os.O_CREATE|os.O_RDWR|os.O_TRUNC, 0600)
	assert.MustNoError(err)
	return f
}

func checkWriter(bl *Backlog, b []byte) {
	n, err := bl.Write(b)
	assert.MustNoError(err)
	assert.Must(n == len(b))
}

func checkReader(r io.Reader, b []byte) {
	x := make([]byte, len(b))
	n, err := io.ReadFull(r, x)
	assert.MustNoError(err)
	assert.Must(n == len(b) && bytes.Equal(x, b))
}

func randSlice(n int) []byte {
	b := make([]byte, n)
	r := rand.New(rand.NewSource(time.Now().UnixNano()))
	for i := 0; i < len(b); i++ {
		b[i] = byte(r.Int())
	}
	return b
}

func testBacklog(t *testing.T, bl *Backlog, size int) {
	input := randSlice(32)

	r1, err := bl.NewReader()
	assert.MustNoError(err)

	checkWriter(bl, input)
	checkReader(r1, input)
	checkReader(r1, []byte{})

	input = randSlice(size)
	checkWriter(bl, input)
	checkReader(r1, input)
	checkWriter(bl, randSlice(size))

	assert.Must(r1.IsValid() == true)

	r2, err := bl.NewReader()
	assert.MustNoError(err)

	input = []byte{0xde, 0xad, 0xbe, 0xef}
	checkWriter(bl, input)

	assert.Must(r1.IsValid() == false)

	_, err = r1.Read([]byte{0})
	assert.Must(errors.Equal(err, ErrInvalidOffset))

	b := make([]byte, len(input))
	n, err := io.ReadFull(r2, b)
	assert.MustNoError(err)
	assert.Must(n == len(b) && bytes.Equal(b, input))

	bl.Close()

	assert.Must(r1.IsValid() == false)
	assert.Must(r2.IsValid() == false)

	_, err = r1.Read([]byte{0})
	assert.Must(errors.Equal(err, ErrClosedBacklog))

	_, err = r2.Read([]byte{0})
	assert.Must(errors.Equal(err, ErrClosedBacklog))

	_, err = bl.Write([]byte{0})
	assert.Must(errors.Equal(err, ErrClosedBacklog))
}

func TestBacklog1(t *testing.T) {
	const size = BuffSizeAlign * 2
	bl := NewSize(size)
	testBacklog(t, bl, size)
}

func TestBacklog2(t *testing.T) {
	f := openFile("/tmp/backlog.test")
	defer f.Close()
	const size = FileSizeAlign * 2
	bl := NewFileBacklog(size, f)
	testBacklog(t, bl, size)
}
