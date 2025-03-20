package types

import (
	"RedisShake/internal/log"
	"io"

	"RedisShake/internal/rdb/structure"
)

type SetObject struct {
	key      string
	typeByte byte
	rd       io.Reader
	cmdC     chan RedisCmd
}

func (o *SetObject) LoadFromBuffer(rd io.Reader, key string, typeByte byte) {
	o.key = key
	o.typeByte = typeByte
	o.rd = rd
	o.cmdC = make(chan RedisCmd)
}

func (o *SetObject) Rewrite() <-chan RedisCmd {
	go func() {
		defer close(o.cmdC)
		o.cmdC <- RedisCmd{"del", o.key}
		switch o.typeByte {
		case rdbTypeSet:
			o.readSet()
		case rdbTypeSetIntset:
			o.readIntset()
		case rdbTypeSetListpack:
			o.readListpack()
		default:
			log.Panicf("unknown set type. typeByte=[%d]", o.typeByte)
		}
	}()
	return o.cmdC
}

func (o *SetObject) readSet() {
	rd := o.rd
	size := int(structure.ReadLength(rd))
	for i := 0; i < size; i++ {
		val := structure.ReadString(rd)
		o.cmdC <- RedisCmd{"sadd", o.key, val}
	}
}

func (o *SetObject) readIntset() {
	elements := structure.ReadIntset(o.rd)
	for _, ele := range elements {
		o.cmdC <- RedisCmd{"sadd", o.key, ele}
	}
}

func (o *SetObject) readListpack() {
	elements := structure.ReadListpack(o.rd)
	for _, ele := range elements {
		o.cmdC <- RedisCmd{"sadd", o.key, ele}
	}
}
