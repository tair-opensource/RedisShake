package types

import (
	"fmt"
	"github.com/alibaba/RedisShake/internal/log"
	"github.com/alibaba/RedisShake/internal/rdb/structure"
	"io"
)

type ZSetEntry struct {
	Member string
	Score  string
}

type ZsetObject struct {
	key      string
	elements []ZSetEntry
}

func (o *ZsetObject) LoadFromBuffer(rd io.Reader, key string, typeByte byte) {
	o.key = key
	switch typeByte {
	case rdbTypeZSet:
		o.readZset(rd)
	case rdbTypeZSet2:
		o.readZset2(rd)
	case rdbTypeZSetZiplist:
		o.readZsetZiplist(rd)
	case rdbTypeZSetListpack:
		o.readZsetListpack(rd)
	default:
		log.Panicf("unknown zset type. typeByte=[%d]", typeByte)
	}
}

func (o *ZsetObject) readZset(rd io.Reader) {
	size := int(structure.ReadLength(rd))
	o.elements = make([]ZSetEntry, size)
	for i := 0; i < size; i++ {
		o.elements[i].Member = structure.ReadString(rd)
		score := structure.ReadFloat(rd)
		o.elements[i].Score = fmt.Sprintf("%f", score)
	}
}

func (o *ZsetObject) readZset2(rd io.Reader) {
	size := int(structure.ReadLength(rd))
	o.elements = make([]ZSetEntry, size)
	for i := 0; i < size; i++ {
		o.elements[i].Member = structure.ReadString(rd)
		score := structure.ReadDouble(rd)
		o.elements[i].Score = fmt.Sprintf("%f", score)
	}
}

func (o *ZsetObject) readZsetZiplist(rd io.Reader) {
	list := structure.ReadZipList(rd)
	size := len(list)
	if size%2 != 0 {
		log.Panicf("zset listpack size is not even. size=[%d]", size)
	}
	o.elements = make([]ZSetEntry, size/2)
	for i := 0; i < size; i += 2 {
		o.elements[i/2].Member = list[i]
		o.elements[i/2].Score = list[i+1]
	}
}

func (o *ZsetObject) readZsetListpack(rd io.Reader) {
	list := structure.ReadListpack(rd)
	size := len(list)
	if size%2 != 0 {
		log.Panicf("zset listpack size is not even. size=[%d]", size)
	}
	o.elements = make([]ZSetEntry, size/2)
	for i := 0; i < size; i += 2 {
		o.elements[i/2].Member = list[i]
		o.elements[i/2].Score = list[i+1]
	}
}

func (o *ZsetObject) Rewrite() []RedisCmd {
	cmds := make([]RedisCmd, len(o.elements))
	for inx, ele := range o.elements {
		cmd := RedisCmd{"zadd", o.key, ele.Score, ele.Member}
		cmds[inx] = cmd
	}
	return cmds
}
