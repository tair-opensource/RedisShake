package types

import (
	"RedisShake/internal/rdb/structure"
	"io"
)

type TairStringObject struct {
	key  string
	rd   io.Reader
	cmdC chan RedisCmd
}

func (o *TairStringObject) LoadFromBuffer(rd io.Reader, key string, typeByte byte) {
	o.key = key
	o.rd = rd
	o.cmdC = make(chan RedisCmd)
}

func (o *TairStringObject) Rewrite() <-chan RedisCmd {
	go func() {
		defer close(o.cmdC)
		rd := o.rd
		version := structure.ReadModuleUnsigned(rd)
		flags := structure.ReadModuleUnsigned(rd)
		tairValue := structure.ReadModuleString(rd)
		structure.ReadModuleEof(rd)
		o.cmdC <- RedisCmd{"EXSET", o.key, tairValue, "ABS", version, "FLAGS", flags}
	}()
	return o.cmdC
}
