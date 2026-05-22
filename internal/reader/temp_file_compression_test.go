package reader

import (
	"bytes"
	"io"
	"testing"

	"github.com/stretchr/testify/require"
)

func TestTempFileCompressionRoundTrip(t *testing.T) {
	payload := bytes.Repeat([]byte("REDIS0012"), 1024)
	for _, method := range []string{"none", "zstd", "snappy"} {
		t.Run(method, func(t *testing.T) {
			compression, err := newTempFileCompression(method)
			require.NoError(t, err)

			var compressed bytes.Buffer
			writer, err := compression.WrapWriter(&compressed)
			require.NoError(t, err)
			_, err = writer.Write(payload)
			require.NoError(t, err)
			require.NoError(t, writer.Close())

			reader, err := compression.WrapReader(bytes.NewReader(compressed.Bytes()))
			require.NoError(t, err)
			got, err := io.ReadAll(reader)
			require.NoError(t, err)
			require.NoError(t, reader.Close())
			require.Equal(t, payload, got)
		})
	}
}
