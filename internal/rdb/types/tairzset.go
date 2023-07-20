package types

import (
	"io"
	"strconv"
	"strings"

	"github.com/alibaba/RedisShake/internal/rdb/structure"
)

type TairZsetObject struct {
	key      string
	length   string
	scoreNum string
	value    map[string][]string
}

func (o *TairZsetObject) LoadFromBuffer(rd io.Reader, key string, typeByte byte) {
	o.key = key
	o.length = structure.ReadModuleUnsigned(rd)
	o.scoreNum = structure.ReadModuleUnsigned(rd)

	len, _ := strconv.Atoi(o.length)
	scoreNum, _ := strconv.Atoi(o.scoreNum)
	valueMap := make(map[string][]string)
	for i := 0; i < len; i++ {
		key := structure.ReadModuleString(rd)
		values := []string{}
		for j := 0; j < scoreNum; j++ {
			values = append(values, structure.ReadModuleDouble(rd))
		}
		valueMap[key] = values
	}
	o.value = valueMap
	structure.ReadModuleEof(rd)
}

func (o *TairZsetObject) Rewrite() []RedisCmd {
	var cmds []RedisCmd
	for k, v := range o.value {
		score := strings.Join(v, "#")
		cmd := RedisCmd{"EXZADD", o.key, score, k}
		cmds = append(cmds, cmd)
	}
	return cmds
}
