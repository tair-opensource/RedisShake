package types

import (
	"io"
	"strconv"
	"strings"

	"RedisShake/internal/rdb/structure"
)

type TairZsetObject struct {
	key      string
	length   string
	scoreNum string
	rd       io.Reader
	cmdC     chan RedisCmd
}

func (o *TairZsetObject) LoadFromBuffer(rd io.Reader, key string, typeByte byte) {
	o.key = key
	o.length = structure.ReadModuleUnsigned(rd)
	o.scoreNum = structure.ReadModuleUnsigned(rd)
	o.cmdC = make(chan RedisCmd)
}

func (o *TairZsetObject) Rewrite() <-chan RedisCmd {
	rd := o.rd
	cmdC := o.cmdC
	go func() {
		defer close(cmdC)
		length, _ := strconv.Atoi(o.length)
		scoreNum, _ := strconv.Atoi(o.scoreNum)
		for i := 0; i < length; i++ {
			key := structure.ReadModuleString(rd)
			var values []string
			for j := 0; j < scoreNum; j++ {
				values = append(values, structure.ReadModuleDouble(rd))
			}
			score := strings.Join(values, "#")
			cmdC <- RedisCmd{"EXZADD", o.key, score, key}
		}
		structure.ReadModuleEof(rd)
	}()
	return cmdC
}
