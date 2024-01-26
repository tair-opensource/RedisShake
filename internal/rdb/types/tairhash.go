package types

import (
	"io"
	"strconv"

	"RedisShake/internal/rdb/structure"
)

type TairHashObject struct {
	dictSize string
	key      string
	rd       io.Reader
	cmdC     chan RedisCmd
}

func (o *TairHashObject) LoadFromBuffer(rd io.Reader, key string, typeByte byte) {
	o.dictSize = structure.ReadModuleUnsigned(rd)
	o.key = structure.ReadModuleString(rd)
	o.cmdC = make(chan RedisCmd)
}

func (o *TairHashObject) Rewrite() <-chan RedisCmd {
	rd := o.rd
	cmdC := o.cmdC
	go func() {
		defer close(o.cmdC)
		size, _ := strconv.Atoi(o.dictSize)
		for i := 0; i < size; i++ {
			skey := structure.ReadModuleString(rd)
			version := structure.ReadModuleUnsigned(rd)
			expireText := structure.ReadModuleUnsigned(rd)
			fieldValue := structure.ReadModuleString(rd)
			expire, _ := strconv.Atoi(expireText)
			if expire == 0 {
				cmdC <- RedisCmd{"EXHSET", o.key, skey, fieldValue}
			} else {
				cmdC <- RedisCmd{"EXHSET", o.key, skey, fieldValue,
					"ABS", version,
					"PXAT", expireText}
			}
		}
		structure.ReadModuleEof(rd)
	}()
	return cmdC
}
