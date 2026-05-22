package reader

import (
	"fmt"
	"io"
	"strings"

	"github.com/klauspost/compress/snappy"
	"github.com/klauspost/compress/zstd"
)

const (
	rdbBaseFilename         = "dump.rdb"
	tempFileCompressionNone = "none"
	tempFileCompressionZstd = "zstd"
	tempFileCompressionSnap = "snappy"
)

type tempFileCompression struct {
	method string
}

func newTempFileCompression(method string) (tempFileCompression, error) {
	method = strings.ToLower(strings.TrimSpace(method))
	switch method {
	case "", tempFileCompressionNone:
		return tempFileCompression{method: tempFileCompressionNone}, nil
	case tempFileCompressionZstd, tempFileCompressionSnap:
		return tempFileCompression{method: method}, nil
	default:
		return tempFileCompression{}, fmt.Errorf("unsupported temp file compression: %s", method)
	}
}

func (c tempFileCompression) RDBFilename() string {
	return rdbBaseFilename + c.FileExtension()
}

func (c tempFileCompression) FileExtension() string {
	switch c.method {
	case tempFileCompressionZstd:
		return ".zst"
	case tempFileCompressionSnap:
		return ".snappy"
	default:
		return ""
	}
}

func (c tempFileCompression) WrapWriter(w io.Writer) (io.WriteCloser, error) {
	switch c.method {
	case tempFileCompressionZstd:
		writer, err := zstd.NewWriter(w, zstd.WithEncoderLevel(zstd.SpeedFastest))
		if err != nil {
			return nil, fmt.Errorf("create zstd writer: %w", err)
		}
		return writer, nil
	case tempFileCompressionSnap:
		return snappy.NewBufferedWriter(w), nil
	default:
		return nopWriteCloser{Writer: w}, nil
	}
}

func (c tempFileCompression) WrapReader(r io.Reader) (io.ReadCloser, error) {
	switch c.method {
	case tempFileCompressionZstd:
		decoder, err := zstd.NewReader(r)
		if err != nil {
			return nil, fmt.Errorf("create zstd reader: %w", err)
		}
		return decoder.IOReadCloser(), nil
	case tempFileCompressionSnap:
		return io.NopCloser(snappy.NewReader(r)), nil
	default:
		return io.NopCloser(r), nil
	}
}

type nopWriteCloser struct {
	io.Writer
}

func (nopWriteCloser) Close() error {
	return nil
}
