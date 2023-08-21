package types

import (
	"io"

	"github.com/alibaba/RedisShake/internal/log"
	"github.com/alibaba/RedisShake/internal/rdb/structure"
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
func (o *ListObject) LoadFromBufferWithOffset(rd io.Reader, key string, typeByte byte) int64 {
	var offset int64 = 0
	o.key = key
	switch typeByte {
	case rdbTypeList:
		offset = o.readListWithOffset(rd)
		return offset
	case rdbTypeListZiplist:
		o.elements, offset = structure.ReadZipListWithOffset(rd)
		return offset
	case rdbTypeListQuicklist:
		offset = o.readQuickListWithOffset(rd)
		return offset
	case rdbTypeListQuicklist2:
		offset = o.readQuickList2WithOffset(rd)
		return offset
	default:
		log.Panicf("unknown list type %d", typeByte)
	}
	return offset
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
func (o *ListObject) readListWithOffset(rd io.Reader) int64 {
	sizes, offset := structure.ReadLengthWithOffset(rd)
	size := int(sizes)
	for i := 0; i < size; i++ {
		ele, offsets := structure.ReadStringWithOffset(rd)
		offset += offsets
		o.elements = append(o.elements, ele)
	}
	return offset
}

func (o *ListObject) readQuickList(rd io.Reader) {
	size := int(structure.ReadLength(rd))
	for i := 0; i < size; i++ {
		ziplistElements := structure.ReadZipList(rd)
		o.elements = append(o.elements, ziplistElements...)
	}
}
func (o *ListObject) readQuickListWithOffset(rd io.Reader) int64 {
	size, offset := structure.ReadLengthWithOffset(rd)
	for i := 0; i < int(size); i++ {
		ziplistElements, offsets := structure.ReadZipListWithOffset(rd)
		offset += offsets
		o.elements = append(o.elements, ziplistElements...)
	}
	return offset
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

func (o *ListObject) readQuickList2WithOffset(rd io.Reader) int64 {
	size, offset := structure.ReadLengthWithOffset(rd)
	for i := 0; i < int(size); i++ {
		container, offsets := structure.ReadLengthWithOffset(rd)
		offset += offsets
		if container == quicklistNodeContainerPlain {
			ele, offsets := structure.ReadStringWithOffset(rd)
			offset += offsets
			o.elements = append(o.elements, ele)
		} else if container == quicklistNodeContainerPacked {
			listpackElements, TempOffsets := structure.ReadListpackWithOffset(rd)
			offset += TempOffsets
			o.elements = append(o.elements, listpackElements...)
		} else {
			log.Panicf("unknown quicklist container %d", container)
		}
	}
	return offset
}
