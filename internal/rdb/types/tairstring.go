package types

import (
	"io"

	"RedisShake/internal/rdb/structure"
)

type TairStringValue struct {
	version   string
	flags     string
	tairValue string
}

type TairStringObject struct {
	value TairStringValue
	key   string
}

func (o *TairStringObject) LoadFromBuffer(rd io.Reader, key string, typeByte byte) {
	o.key = key
	o.value.version = structure.ReadModuleUnsigned(rd)
	o.value.flags = structure.ReadModuleUnsigned(rd)
	o.value.tairValue = structure.ReadModuleString(rd)
	structure.ReadModuleEof(rd)
}

func (o *TairStringObject) Rewrite() []RedisCmd {
	cmd := RedisCmd{}
	cmd = append(cmd, "EXSET", o.key, o.value.tairValue, "ABS", o.value.version, "FLAGS", o.value.flags)
	return []RedisCmd{cmd}
}
