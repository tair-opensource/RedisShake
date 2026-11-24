package types

import (
	"io"
	"strconv"

	"RedisShake/internal/log"
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
		case rdbTypeSetListpackEx:
			o.readSetListpackEx()
		case rdbTypeSetMetadata:
			o.readSetMetadata()
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

func (o *SetObject) readSetListpackEx() {
	_ = structure.ReadUint64(o.rd) // minExpire
	elements := structure.ReadListpack(o.rd)
	for i := 0; i < len(elements); i += 2 {
		member := elements[i]
		expireAt, err := strconv.ParseInt(elements[i+1], 10, 64)
		if err != nil {
			log.Panicf("readSetListpackEx parsing expireAt %s error", elements[i+1])
		}
		o.cmdC <- RedisCmd{"sadd", o.key, member}
		if expireAt != 0 {
			o.cmdC <- RedisCmd{"spexpireat", o.key, strconv.FormatInt(expireAt, 10), "members", "1", member}
		}
	}
}

func (o *SetObject) readSetMetadata() {
	minExpire := int64(structure.ReadUint64(o.rd))
	size := int(structure.ReadLength(o.rd))
	for i := 0; i < size; i++ {
		ttl := int64(structure.ReadLength(o.rd))
		var expireAt int64
		if ttl != 0 {
			expireAt = ttl + minExpire - 1
		}
		member := structure.ReadString(o.rd)
		o.cmdC <- RedisCmd{"sadd", o.key, member}
		if expireAt != 0 {
			o.cmdC <- RedisCmd{"spexpireat", o.key, strconv.FormatInt(expireAt, 10), "members", "1", member}
		}
	}
}
