package rdb

import (
	"bytes"
	"context"
	"io"
	"testing"

	"RedisShake/internal/entry"

	"github.com/stretchr/testify/require"
)

func TestLoaderParseRDBFromReader(t *testing.T) {
	payload := append([]byte("REDIS0006"), byte(kEOF))
	ch := make(chan *entry.Entry, 1)
	var offset int64

	loader := NewLoader("test", func(n int64) {
		offset = n
	}, io.NopCloser(bytes.NewReader(payload)), ch)

	require.Equal(t, 0, loader.ParseRDB(context.Background()))
	require.Equal(t, int64(len(payload)), offset)
	require.Empty(t, ch)
}
