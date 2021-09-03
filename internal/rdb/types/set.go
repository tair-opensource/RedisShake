package types

import (
	"github.com/alibaba/RedisShake/internal/log"
	"github.com/alibaba/RedisShake/internal/rdb/structure"
	"io"
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

func (o *SetObject) readSet(rd io.Reader) {
	size := int(structure.ReadLength(rd))
	o.elements = make([]string, size)
	for i := 0; i < size; i++ {
		val := structure.ReadString(rd)
		o.elements[i] = val
	}
}

func (o *SetObject) Rewrite() []RedisCmd {
	cmds := make([]RedisCmd, len(o.elements))
	for inx, ele := range o.elements {
		cmd := RedisCmd{"sadd", o.key, ele}
		cmds[inx] = cmd
	}
	return cmds
}
