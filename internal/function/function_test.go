package function

import (
	"testing"

	"RedisShake/internal/config"
	"RedisShake/internal/entry"
)

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
	Init()
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
		RunFunction(e)
	}
}
