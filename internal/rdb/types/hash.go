package types

import (
	"io"

	"RedisShake/internal/log"
	"RedisShake/internal/rdb/structure"
)

type HashObject struct {
	key      string
	typeByte byte
	rd       io.Reader
	cmdC     chan RedisCmd
}

func (o *HashObject) LoadFromBuffer(rd io.Reader, key string, typeByte byte) {
	o.key = key
	o.typeByte = typeByte
	o.rd = rd
	o.cmdC = make(chan RedisCmd)
}

func (o *HashObject) Rewrite() <-chan RedisCmd {
	go func() {
		defer close(o.cmdC)
		switch o.typeByte {
		case rdbTypeHash:
			o.readHash()
		case rdbTypeHashZipmap:
			o.readHashZipmap()
		case rdbTypeHashZiplist:
			o.readHashZiplist()
		case rdbTypeHashListpack:
			o.readHashListpack()
		default:
			log.Panicf("unknown hash type. typeByte=[%d]", o.typeByte)
		}
	}()
	return o.cmdC
}

func (o *HashObject) readHash() {
	rd := o.rd
	size := int(structure.ReadLength(rd))
	for i := 0; i < size; i++ {
		key := structure.ReadString(rd)
		value := structure.ReadString(rd)
		o.cmdC <- RedisCmd{"hset", o.key, key, value}
	}
}

func (o *HashObject) readHashZipmap() {
	log.Panicf("not implemented rdbTypeZipmap")
}

func (o *HashObject) readHashZiplist() {
	rd := o.rd
	list := structure.ReadZipList(rd)
	size := len(list)
	for i := 0; i < size; i += 2 {
		key := list[i]
		value := list[i+1]
		o.cmdC <- RedisCmd{"hset", o.key, key, value}
	}
}

func (o *HashObject) readHashListpack() {
	rd := o.rd
	list := structure.ReadListpack(rd)
	size := len(list)
	for i := 0; i < size; i += 2 {
		key := list[i]
		value := list[i+1]
		o.cmdC <- RedisCmd{"hset", o.key, key, value}
	}
}
