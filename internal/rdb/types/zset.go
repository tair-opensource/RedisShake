package types

import (
	"fmt"
	"io"

	"RedisShake/internal/log"
	"RedisShake/internal/rdb/structure"
)

type ZsetObject struct {
	key      string
	typeByte byte
	rd       io.Reader
	cmdC     chan RedisCmd
}

func (o *ZsetObject) LoadFromBuffer(rd io.Reader, key string, typeByte byte) {
	o.key = key
	o.typeByte = typeByte
	o.rd = rd
	o.cmdC = make(chan RedisCmd)
}

func (o *ZsetObject) Rewrite() <-chan RedisCmd {
	go func() {
		defer close(o.cmdC)
		o.cmdC <- RedisCmd{"del", o.key}
		switch o.typeByte {
		case rdbTypeZSet:
			o.readZset()
		case rdbTypeZSet2:
			o.readZset2()
		case rdbTypeZSetZiplist:
			o.readZsetZiplist()
		case rdbTypeZSetListpack:
			o.readZsetListpack()
		default:
			log.Panicf("unknown zset type. typeByte=[%d]", o.typeByte)
		}
	}()
	return o.cmdC
}

func (o *ZsetObject) readZset() {
	rd := o.rd
	size := int(structure.ReadLength(rd))
	for i := 0; i < size; i++ {
		member := structure.ReadString(rd)
		score := structure.ReadFloat(rd)
		o.cmdC <- RedisCmd{"zadd", o.key, fmt.Sprintf("%.17g", score), member}
	}
}

func (o *ZsetObject) readZset2() {
	rd := o.rd
	size := int(structure.ReadLength(rd))
	for i := 0; i < size; i++ {
		member := structure.ReadString(rd)
		score := structure.ReadDouble(rd)
		o.cmdC <- RedisCmd{"zadd", o.key, fmt.Sprintf("%.17g", score), member}
	}
}

func (o *ZsetObject) readZsetZiplist() {
	rd := o.rd
	list := structure.ReadZipList(rd)
	size := len(list)
	if size%2 != 0 {
		log.Panicf("zset listpack size is not even. size=[%d]", size)
	}
	for i := 0; i < size; i += 2 {
		member := list[i]
		score := list[i+1]
		o.cmdC <- RedisCmd{"zadd", o.key, score, member}
	}
}

func (o *ZsetObject) readZsetListpack() {
	rd := o.rd
	list := structure.ReadListpack(rd)
	size := len(list)
	if size%2 != 0 {
		log.Panicf("zset listpack size is not even. size=[%d]", size)
	}
	for i := 0; i < size; i += 2 {
		member := list[i]
		score := list[i+1]
		o.cmdC <- RedisCmd{"zadd", o.key, score, member}
	}
}
