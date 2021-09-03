package types

import (
	"github.com/alibaba/RedisShake/internal/log"
	"github.com/alibaba/RedisShake/internal/rdb/structure"
	"io"
)

type HashObject struct {
	key   string
	value map[string]string
}

func (o *HashObject) LoadFromBuffer(rd io.Reader, key string, typeByte byte) {
	o.key = key
	o.value = make(map[string]string)
	switch typeByte {
	case rdbTypeHash:
		o.readHash(rd)
	case rdbTypeHashZipmap:
		o.readHashZipmap(rd)
	case rdbTypeHashZiplist:
		o.readHashZiplist(rd)
	case rdbTypeHashListpack:
		o.readHashListpack(rd)
	default:
		log.Panicf("unknown hash type. typeByte=[%d]", typeByte)
	}
}

func (o *HashObject) readHash(rd io.Reader) {
	size := int(structure.ReadLength(rd))
	for i := 0; i < size; i++ {
		key := structure.ReadString(rd)
		value := structure.ReadString(rd)
		o.value[key] = value
	}
}

func (o *HashObject) readHashZipmap(rd io.Reader) {
	log.Panicf("not implemented rdbTypeZipmap")
}

func (o *HashObject) readHashZiplist(rd io.Reader) {
	list := structure.ReadZipList(rd)
	size := len(list)
	for i := 0; i < size; i += 2 {
		key := list[i]
		value := list[i+1]
		o.value[key] = value
	}
}

func (o *HashObject) readHashListpack(rd io.Reader) {
	list := structure.ReadListpack(rd)
	size := len(list)
	for i := 0; i < size; i += 2 {
		key := list[i]
		value := list[i+1]
		o.value[key] = value
	}
}

func (o *HashObject) Rewrite() []RedisCmd {
	var cmds []RedisCmd
	for k, v := range o.value {
		cmd := RedisCmd{"hset", o.key, k, v}
		cmds = append(cmds, cmd)
	}
	return cmds
}
