package rdb

import (
	"bytes"
	"testing"

	"github.com/stretchr/testify/require"
)

func TestCapturingReader(t *testing.T) {
	// Test that capturingReader captures all bytes read
	input := []byte("hello world")
	cr := &capturingReader{rd: bytes.NewReader(input)}

	buf := make([]byte, len(input))
	n, err := cr.Read(buf)
	require.NoError(t, err)
	require.Equal(t, len(input), n)
	require.Equal(t, input, buf)
	require.Equal(t, input, cr.Bytes())
}

func TestCapturingReader_MultipleReads(t *testing.T) {
	// Test that capturingReader captures bytes across multiple reads
	input := []byte("hello world")
	cr := &capturingReader{rd: bytes.NewReader(input)}

	// Read in small chunks
	buf := make([]byte, 5)
	n, err := cr.Read(buf)
	require.NoError(t, err)
	require.Equal(t, 5, n)
	require.Equal(t, []byte("hello"), buf)

	n, err = cr.Read(buf)
	require.NoError(t, err)
	require.Equal(t, 5, n)
	require.Equal(t, []byte(" worl"), buf)

	// Last read
	buf = make([]byte, 5)
	n, err = cr.Read(buf)
	require.NoError(t, err)
	require.Equal(t, 1, n)
	require.Equal(t, []byte("d"), buf[:n])

	// Check captured bytes
	require.Equal(t, input, cr.Bytes())
}

func TestCapturingReader_Reset(t *testing.T) {
	input := []byte("hello world")
	cr := &capturingReader{rd: bytes.NewReader(input)}

	buf := make([]byte, len(input))
	cr.Read(buf)
	require.Equal(t, input, cr.Bytes())

	cr.Reset()
	require.Equal(t, 0, len(cr.Bytes()))
}

func TestCapturingReader_Empty(t *testing.T) {
	cr := &capturingReader{rd: bytes.NewReader([]byte{})}
	// Bytes() returns nil when buffer is empty, which is fine
	// Both nil and empty slice represent "no bytes captured"
	require.True(t, len(cr.Bytes()) == 0 || cr.Bytes() == nil)
}
