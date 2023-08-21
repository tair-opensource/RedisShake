package types

import (
	"io"

	"github.com/alibaba/RedisShake/internal/log"
	"github.com/alibaba/RedisShake/internal/rdb/structure"
)

type SetObject struct {
	key      string
	elements []string
}

func (o *SetObject) LoadFromBuffer(rd io.Reader, key string, typeByte byte) {
	o.key = key
	switch typeByte {
	case rdbTypeSet:
		o.readSet(rd)
	case rdbTypeSetIntset:
		o.elements = structure.ReadIntset(rd)
	default:
		log.Panicf("unknown set type. typeByte=[%d]", typeByte)
	}
}
func (o *SetObject) LoadFromBufferWithOffset(rd io.Reader, key string, typeByte byte) int64 {
	o.key = key
	switch typeByte {
	case rdbTypeSet:
		tempOffset := o.readSetWithOffset(rd)
		return tempOffset
	case rdbTypeSetIntset:
		var tempOffsets int64
		o.elements, tempOffsets = structure.ReadIntsetWithOffset(rd)
		return tempOffsets
	default:
		log.Panicf("unknown set type. typeByte=[%d]", typeByte)
	}
	return 0
}

func (o *SetObject) readSet(rd io.Reader) {
	size := int(structure.ReadLength(rd))
	o.elements = make([]string, size)
	for i := 0; i < size; i++ {
		val := structure.ReadString(rd)
		o.elements[i] = val
	}
}
func (o *SetObject) readSetWithOffset(rd io.Reader) int64 {
	size, offset := structure.ReadLengthWithOffset(rd)
	o.elements = make([]string, size)
	for i := 0; i < int(size); i++ {
		val, TempOffsets := structure.ReadStringWithOffset(rd)
		offset += TempOffsets
		o.elements[i] = val
	}
	return offset
}

func (o *SetObject) Rewrite() []RedisCmd {
	cmds := make([]RedisCmd, len(o.elements))
	for inx, ele := range o.elements {
		cmd := RedisCmd{"sadd", o.key, ele}
		cmds[inx] = cmd
	}
	return cmds
}
