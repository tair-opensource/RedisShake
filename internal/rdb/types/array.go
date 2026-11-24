package types

import (
	"io"
	"strconv"

	"RedisShake/internal/log"
	"RedisShake/internal/rdb/structure"
)

const (
	arRdbTagSds      = 0
	arRdbTagInt      = 1
	arRdbTagFloat    = 2
	arRdbTagSmallStr = 3
)

// ArrayObject parses Redis 8.12 RDB_TYPE_ARRAY (type 28).
type ArrayObject struct {
	key  string
	rd   io.Reader
	cmdC chan RedisCmd
}

func (o *ArrayObject) LoadFromBuffer(rd io.Reader, key string, typeByte byte) {
	o.key = key
	o.rd = rd
	o.cmdC = make(chan RedisCmd)
}

func (o *ArrayObject) Rewrite() <-chan RedisCmd {
	go func() {
		defer close(o.cmdC)
		o.cmdC <- RedisCmd{"del", o.key}
		count := structure.ReadLength(o.rd)
		insertIdxFlag := structure.ReadLength(o.rd)
		if insertIdxFlag == 1 {
			_ = structure.ReadLength(o.rd) // insert_idx
		}
		for i := uint64(0); i < count; i++ {
			idx := structure.ReadLength(o.rd)
			typeTag := structure.ReadLength(o.rd)
			var val string
			switch typeTag {
			case arRdbTagSds, arRdbTagSmallStr:
				val = structure.ReadString(o.rd)
			case arRdbTagInt:
				val = strconv.FormatInt(structure.ReadInt64(o.rd), 10)
			case arRdbTagFloat:
				val = strconv.FormatFloat(structure.ReadDouble(o.rd), 'g', -1, 64)
			default:
				log.Panicf("unknown array element type_tag. key=[%s], tag=[%d]", o.key, typeTag)
			}
			o.cmdC <- RedisCmd{"arset", o.key, strconv.FormatUint(idx, 10), val}
		}
	}()
	return o.cmdC
}
