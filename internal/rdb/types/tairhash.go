package types

import (
	"io"
	"strconv"

	"github.com/alibaba/RedisShake/internal/rdb/structure"
)

type TairHashValue struct {
	skey       string
	version    string
	expire     string
	fieldValue string
}

type TairHashObject struct {
	dictSize string
	key      string
	value    []TairHashValue
}

func (o *TairHashObject) LoadFromBuffer(rd io.Reader, key string, typeByte byte) {
	o.dictSize = structure.ReadModuleUnsigned(rd)
	o.key = structure.ReadModuleString(rd)

	size, _ := strconv.Atoi(o.dictSize)
	for i := 0; i < size; i++ {
		hashValue := TairHashValue{
			structure.ReadModuleString(rd),
			structure.ReadModuleUnsigned(rd),
			structure.ReadModuleUnsigned(rd),
			structure.ReadModuleString(rd),
		}
		o.value = append(o.value, hashValue)
	}
	structure.ReadModuleEof(rd)
}

func (o *TairHashObject) Rewrite() []RedisCmd {
	var cmds []RedisCmd
	size, _ := strconv.Atoi(o.dictSize)
	for i := 0; i < size; i++ {
		cmd := []string{}
		expire, _ := strconv.Atoi(o.value[i].expire)
		if expire == 0 {
			cmd = append(cmd, "EXHSET", o.key, o.value[i].skey, o.value[i].fieldValue)
		} else {
			cmd = append(cmd, "EXHSET", o.key, o.value[i].skey, o.value[i].fieldValue,
				"ABS", o.value[i].version,
				"PXAT", o.value[i].expire)
		}

		cmds = append(cmds, cmd)
	}
	return cmds
}
