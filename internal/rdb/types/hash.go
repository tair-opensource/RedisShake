package types

import (
	"io"

	"github.com/alibaba/RedisShake/internal/log"
	"github.com/alibaba/RedisShake/internal/rdb/structure"
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

func (o *HashObject) LoadFromBufferWithOffset(rd io.Reader, key string, typeByte byte) int64 {
	o.key = key
	o.value = make(map[string]string)
	switch typeByte {
	case rdbTypeHash:
		offset := o.readHashWithOffset(rd)
		return offset
	case rdbTypeHashZipmap:
		o.readHashZipmap(rd)
	case rdbTypeHashZiplist:
		offset := o.readHashZiplistWithOffset(rd)
		return offset
	case rdbTypeHashListpack:
		offset := o.readHashListpackWithOffset(rd)
		return offset
	default:
		log.Panicf("unknown hash type. typeByte=[%d]", typeByte)
		return 0
	}
	return 0
}
func (o *HashObject) readHash(rd io.Reader) {
	size := int(structure.ReadLength(rd))
	for i := 0; i < size; i++ {
		key := structure.ReadString(rd)
		value := structure.ReadString(rd)
		o.value[key] = value
	}
}

func (o *HashObject) readHashWithOffset(rd io.Reader) int64 {
	size, offset := structure.ReadLengthWithOffset(rd)
	for i := 0; i < int(size); i++ {
		key, tempOffsets := structure.ReadStringWithOffset(rd)
		offset += tempOffsets
		value, tempOffsets := structure.ReadStringWithOffset(rd)
		offset += tempOffsets
		o.value[key] = value
	}
	return offset
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

func (o *HashObject) readHashZiplistWithOffset(rd io.Reader) int64 {
	list, offset := structure.ReadZipListWithOffset(rd)
	size := len(list)
	for i := 0; i < size; i += 2 {
		key := list[i]
		value := list[i+1]
		o.value[key] = value
	}
	return offset
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

func (o *HashObject) readHashListpackWithOffset(rd io.Reader) int64 {
	list, offset := structure.ReadListpackWithOffset(rd)
	size := len(list)
	for i := 0; i < size; i += 2 {
		key := list[i]
		value := list[i+1]
		o.value[key] = value
	}
	return offset
}

func (o *HashObject) Rewrite() []RedisCmd {
	var cmds []RedisCmd
	for k, v := range o.value {
		cmd := RedisCmd{"hset", o.key, k, v}
		cmds = append(cmds, cmd)
	}
	return cmds
}
