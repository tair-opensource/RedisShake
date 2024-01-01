package rdb

import (
	"context"
	"testing"

	"RedisShake/internal/entry"
)

// BenchmarkParseRDB is a benchmark for ParseRDB
// The baseline is "20	 350030327 ns/op	213804114 B/op	 1900715 allocs/op"
func BenchmarkParseRDB(b *testing.B) {
	b.ReportAllocs()
	b.ResetTimer()
	tempChan := make(chan *entry.Entry, 1024)
	updateFunc := func(offset int64) {

	}
	b.N = 20

	for i := 0; i < b.N; i++ {
		loader := NewLoader("rdb", updateFunc, "./dump.rdb", tempChan)
		go func() {
			for temp := range tempChan {
				print(temp.CmdName)
			}
		}()
		loader.ParseRDB(context.Background())
	}
}
