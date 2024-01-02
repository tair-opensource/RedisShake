package function

import (
	"testing"

	"RedisShake/internal/config"
	"RedisShake/internal/entry"
)

// BenchmarkRunFunction is a benchmark for RunFunction
// Command is `go test -benchmem -bench="RunFunction$" -count=5 RedisShake/internal/function`
// Output is:
//
// BenchmarkRunFunction-16             6741            182470 ns/op          234715 B/op       1079 allocs/op
// BenchmarkRunFunction-16             7443            174567 ns/op          234710 B/op       1079 allocs/op
// BenchmarkRunFunction-16             7101            178651 ns/op          234711 B/op       1079 allocs/op
// BenchmarkRunFunction-16             6856            164739 ns/op          234722 B/op       1079 allocs/op
// BenchmarkRunFunction-16             6804            174768 ns/op          234713 B/op       1079 allocs/op
func BenchmarkRunFunction(b *testing.B) {
	config.Opt = config.ShakeOptions{
		Function: `
local prefix = "mlpSummary:"
local prefix_len = #prefix
if KEYS[1] == nil then
  return
end
if KEYS[1] == "" then
  return
end
if string.sub(KEYS[1], 1, prefix_len) ~= prefix then
  return
end
shake.call(DB, ARGV)
`,
	}
	luaRuntime := New(config.Opt.Function)
	e := &entry.Entry{
		DbId:           0,
		Argv:           []string{"set", "mlpSummary:1", "1"},
		CmdName:        "set",
		Group:          "default",
		Keys:           []string{"mlpSummary:1"},
		KeyIndexes:     []int{1},
		Slots:          []int{0},
		SerializedSize: 32,
	}
	b.ResetTimer()
	b.ReportAllocs()
	for i := 0; i < b.N; i++ {
		luaRuntime.RunFunction(e)
	}
}
