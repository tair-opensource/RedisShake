package filter

import (
	"testing"

	"RedisShake/internal/config"
	"RedisShake/internal/entry"
)

// BenchmarkRunFunction is a benchmark for RunFunction
// Command is `go test -benchmem -bench="RunFunction$" -count=5 RedisShake/internal/function`
// Output is:
//
// cpu: Intel(R) Xeon(R) Platinum 8259CL CPU @ 2.50GHz
// BenchmarkRunFunction-16           152046              8494 ns/op           15283 B/op         42 allocs/op
// BenchmarkRunFunction-16           150916              7630 ns/op           15274 B/op         42 allocs/op
// BenchmarkRunFunction-16           149980              8467 ns/op           15292 B/op         42 allocs/op
// BenchmarkRunFunction-16           158834              7722 ns/op           15278 B/op         42 allocs/op
// BenchmarkRunFunction-16           118228              8482 ns/op           15292 B/op         42 allocs/op
func BenchmarkRunFunction(b *testing.B) {
	config.Opt = config.ShakeOptions{
		Filter: config.FilterOptions{
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
		},
	}
	luaRuntime := NewFunctionFilter(config.Opt.Filter.Function)
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
