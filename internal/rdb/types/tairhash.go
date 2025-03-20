package types

import (
	"io"
	"strconv"

	"RedisShake/internal/rdb/structure"
)

type TairHashObject struct {
	rd   io.Reader
	cmdC chan RedisCmd
}

func (o *TairHashObject) LoadFromBuffer(rd io.Reader, key string, typeByte byte) {
	// `key` and `typeByte` are not used
	o.rd = rd
	o.cmdC = make(chan RedisCmd)
}

func (o *TairHashObject) Rewrite() <-chan RedisCmd {
	rd := o.rd
	cmdC := o.cmdC
	go func() {
		defer close(o.cmdC)
		dictSizeStr := structure.ReadModuleUnsigned(rd)
		key := structure.ReadModuleString(rd)
		size, _ := strconv.Atoi(dictSizeStr)
		cmdC <- RedisCmd{"del", key}
		for i := 0; i < size; i++ {
			skey := structure.ReadModuleString(rd)
			version := structure.ReadModuleUnsigned(rd)
			expireText := structure.ReadModuleUnsigned(rd)
			fieldValue := structure.ReadModuleString(rd)
			expire, _ := strconv.Atoi(expireText)
			if expire == 0 {
				cmdC <- RedisCmd{"EXHSET", key, skey, fieldValue}
			} else {
				cmdC <- RedisCmd{"EXHSET", key, skey, fieldValue,
					"ABS", version,
					"PXAT", expireText}
			}
		}
		structure.ReadModuleEof(rd)
	}()
	return cmdC
}
