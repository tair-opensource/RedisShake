package types

import (
	"io"

	"github.com/alibaba/RedisShake/internal/rdb/structure"
)

type StringObject struct {
	value string
	key   string
}

func (o *StringObject) LoadFromBuffer(rd io.Reader, key string, _ byte) {
	o.key = key
	o.value = structure.ReadString(rd)
}
func (o *StringObject) LoadFromBufferWithOffset(rd io.Reader, key string, _ byte) int64 {
	var offsets int64
	o.key = key
	o.value, offsets = structure.ReadStringWithOffset(rd)
	return offsets
}
func (o *StringObject) Rewrite() []RedisCmd {
	cmd := RedisCmd{}
	cmd = append(cmd, "set", o.key, o.value)
	return []RedisCmd{cmd}
}
