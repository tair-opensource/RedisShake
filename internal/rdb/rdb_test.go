package rdb

import (
	"context"
	"io"
	"os"
	"testing"

	"RedisShake/internal/entry"
)

// BenchmarkParseRDB is a benchmark for ParseRDB
// The baseline is "20	 350030327 ns/op	213804114 B/op	 1900715 allocs/op"
func BenchmarkParseRDB(b *testing.B) {
	sourcePath := "./dump.rdb"
	sourceFile, err := os.Open(sourcePath)
	if err != nil {
		panic(err)
	}
	defer sourceFile.Close()

	destPath := "/tmp/dump.rdb"
	destFile, err := os.Create(destPath)
	if err != nil {
		panic(err)
	}
	defer destFile.Close()

	// 复制文件内容
	_, err = io.Copy(destFile, sourceFile)
	if err != nil {
		panic(err)
	}
	b.ResetTimer()
	b.ReportAllocs()
	b.ResetTimer()
	tempChan := make(chan *entry.Entry, 1024)
	updateFunc := func(offset int64) {

	}

	for i := 0; i < b.N; i++ {
		loader := NewLoader("rdb", updateFunc, "./dump.rdb", tempChan)
		go func() {
			for _ = range tempChan {

			}
		}()
		loader.ParseRDB(context.Background())
	}
}
