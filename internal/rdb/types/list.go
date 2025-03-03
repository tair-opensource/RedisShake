package types

import (
	"io"

	"RedisShake/internal/log"
	"RedisShake/internal/rdb/structure"
)

// quicklist node container formats
const (
	quicklistNodeContainerPlain  = 1 // QUICKLIST_NODE_CONTAINER_PLAIN
	quicklistNodeContainerPacked = 2 // QUICKLIST_NODE_CONTAINER_PACKED
)

type ListObject struct {
	key      string
	typeByte byte
	rd       io.Reader
	cmdC     chan RedisCmd
}

func (o *ListObject) LoadFromBuffer(rd io.Reader, key string, typeByte byte) {
	o.key = key
	o.typeByte = typeByte
	o.rd = rd
	o.cmdC = make(chan RedisCmd)
}

func (o *ListObject) Rewrite() <-chan RedisCmd {
	go func() {
		defer close(o.cmdC)
		o.cmdC <- RedisCmd{"del", o.key}
		switch o.typeByte {
		case rdbTypeList:
			o.readList()
		case rdbTypeListZiplist:
			o.readZipList()
		case rdbTypeListQuicklist:
			o.readQuickList()
		case rdbTypeListQuicklist2:
			o.readQuickList2()
		default:
			log.Panicf("unknown list type %d", o.typeByte)
		}
	}()
	return o.cmdC
}

func (o *ListObject) readList() {
	rd := o.rd
	size := int(structure.ReadLength(rd))
	for i := 0; i < size; i++ {
		ele := structure.ReadString(rd)
		o.cmdC <- RedisCmd{"rpush", o.key, ele}
	}
}

func (o *ListObject) readZipList() {
	rd := o.rd
	elements := structure.ReadZipList(rd)
	for _, ele := range elements {
		o.cmdC <- RedisCmd{"rpush", o.key, ele}
	}
}

func (o *ListObject) readQuickList() {
	rd := o.rd
	size := int(structure.ReadLength(rd))
	for i := 0; i < size; i++ {
		ziplistElements := structure.ReadZipList(rd)
		for _, ele := range ziplistElements {
			o.cmdC <- RedisCmd{"rpush", o.key, ele}
		}
	}
}

func (o *ListObject) readQuickList2() {
	rd := o.rd
	cmdC := o.cmdC
	size := int(structure.ReadLength(rd))
	for i := 0; i < size; i++ {
		container := structure.ReadLength(rd)
		if container == quicklistNodeContainerPlain {
			ele := structure.ReadString(rd)
			cmdC <- RedisCmd{"rpush", o.key, ele}
		} else if container == quicklistNodeContainerPacked {
			listpackElements := structure.ReadListpack(rd)
			for _, ele := range listpackElements {
				cmdC <- RedisCmd{"rpush", o.key, ele}
			}
		} else {
			log.Panicf("unknown quicklist container %d", container)
		}
	}
}
