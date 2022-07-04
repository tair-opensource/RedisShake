package types

import (
	"github.com/alibaba/RedisShake/internal/log"
	"github.com/alibaba/RedisShake/internal/rdb/structure"
	"io"
)

// quicklist node container formats
const (
	quicklistNodeContainerPlain  = 1 // QUICKLIST_NODE_CONTAINER_PLAIN
	quicklistNodeContainerPacked = 2 // QUICKLIST_NODE_CONTAINER_PACKED
)

type ListObject struct {
	key string

	elements []string
}

func (o *ListObject) LoadFromBuffer(rd io.Reader, key string, typeByte byte) {
	o.key = key
	switch typeByte {
	case rdbTypeList:
		o.readList(rd)
	case rdbTypeListZiplist:
		o.elements = structure.ReadZipList(rd)
	case rdbTypeListQuicklist:
		o.readQuickList(rd)
	case rdbTypeListQuicklist2:
		o.readQuickList2(rd)
	default:
		log.Panicf("unknown list type %d", typeByte)
	}
}

func (o *ListObject) Rewrite() []RedisCmd {
	cmds := make([]RedisCmd, len(o.elements))
	for inx, ele := range o.elements {
		cmd := RedisCmd{"rpush", o.key, ele}
		cmds[inx] = cmd
	}
	return cmds
}

func (o *ListObject) readList(rd io.Reader) {
	size := int(structure.ReadLength(rd))
	for i := 0; i < size; i++ {
		ele := structure.ReadString(rd)
		o.elements = append(o.elements, ele)
	}
}

func (o *ListObject) readQuickList(rd io.Reader) {
	size := int(structure.ReadLength(rd))
	for i := 0; i < size; i++ {
		ziplistElements := structure.ReadZipList(rd)
		o.elements = append(o.elements, ziplistElements...)
	}
}

func (o *ListObject) readQuickList2(rd io.Reader) {
	size := int(structure.ReadLength(rd))
	for i := 0; i < size; i++ {
		container := structure.ReadLength(rd)
		if container == quicklistNodeContainerPlain {
			ele := structure.ReadString(rd)
			o.elements = append(o.elements, ele)
		} else if container == quicklistNodeContainerPacked {
			listpackElements := structure.ReadListpack(rd)
			o.elements = append(o.elements, listpackElements...)
		} else {
			log.Panicf("unknown quicklist container %d", container)
		}
	}
}
