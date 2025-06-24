package types

import (
	"io"
	"strconv"

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
		o.cmdC <- RedisCmd{"del", o.key}
		switch o.typeByte {
		case rdbTypeHash:
			o.readHash()
		case rdbTypeHashZipmap:
			o.readHashZipmap()
		case rdbTypeHashZiplist:
			o.readHashZiplist()
		case rdbTypeHashListpack:
			o.readHashListpack()
		case rdbTypeHashMetadataPreGa:
			o.readHashTtl(true)
		case rdbTypeHashListpackExPre:
			o.readHashListpackTtl(true)
		case rdbTypeHashMetadata:
			o.readHashTtl(false)
		case rdbTypeHashListpackEx:
			o.readHashListpackTtl(false)
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


func (o *HashObject) readHashListpackTtl(isPre bool)  {
	rd := o.rd
	if !isPre {
		// read minExpire
		_ = int64(structure.ReadUint64(rd))
	}
	list := structure.ReadListpack(rd)
	size := len(list)
	for i := 0; i < size; i += 3 {
		key := list[i]
		value := list[i+1]

		expireAt,err := strconv.ParseInt(list[i+2], 10, 64)
		if err != nil{
			log.Panicf("readHashListpackTtl parsing expireAt %s error", list[i])
			return
		}
		o.cmdC <- RedisCmd{"hset", o.key, key, value}
		if expireAt != 0{
			o.cmdC <- RedisCmd{"hpexpireat", o.key, strconv.FormatInt(expireAt, 10), "fields", "1", key}
		}
	}
}


func (o *HashObject) readHashTtl(isPre bool){
	rd := o.rd
	var minExpire int64
	if !isPre {
		minExpire = int64(structure.ReadUint64(rd))
		log.Debugf("%s minExpire is %d", o.key, minExpire)
	}

	size := int(structure.ReadLength(rd))
	for i := 0; i < size; i++ {
		expireAt := int64(structure.ReadLength(rd))
		if !isPre{
			if expireAt != 0{
				expireAt = expireAt + minExpire - 1
			}
		}
		key := structure.ReadString(rd)
		value := structure.ReadString(rd)
		//HPEXPIREAT key unix-time-seconds [NX | XX | GT | LT] FIELDS numfields
		o.cmdC <- RedisCmd{"hset", o.key, key, value}
		if expireAt != 0{
			o.cmdC <- RedisCmd{"hpexpireat", o.key, strconv.FormatInt(expireAt, 10), "fields", "1", key}
		}
	}

}