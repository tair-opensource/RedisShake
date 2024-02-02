package types

import (
	"RedisShake/internal/rdb/structure"
	"io"
)

type StringObject struct {
	key string
	rd  io.Reader
}

func (o *StringObject) LoadFromBuffer(rd io.Reader, key string, _ byte) {
	o.key = key
	o.rd = rd
}

func (o *StringObject) Rewrite() <-chan RedisCmd {
	cmdC := make(chan RedisCmd)
	go func() {
		defer close(cmdC)
		value := structure.ReadString(o.rd)
		cmdC <- RedisCmd{"set", o.key, value}
	}()
	return cmdC
}
