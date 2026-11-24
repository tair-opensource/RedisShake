package types

import (
	"io"
	"strconv"

	"RedisShake/internal/rdb/structure"
)

// PathHashObject parses Valkey 9.2 RDB_TYPE_PATH_HASH (type 23).
type PathHashObject struct {
	key  string
	rd   io.Reader
	cmdC chan RedisCmd
}

func (o *PathHashObject) LoadFromBuffer(rd io.Reader, key string, typeByte byte) {
	o.key = key
	o.rd = rd
	o.cmdC = make(chan RedisCmd)
}

func (o *PathHashObject) Rewrite() <-chan RedisCmd {
	go func() {
		defer close(o.cmdC)
		o.cmdC <- RedisCmd{"del", o.key}
		pathCount := int(structure.ReadLength(o.rd))
		for i := 0; i < pathCount; i++ {
			path := structure.ReadString(o.rd)
			fieldCount := int(structure.ReadLength(o.rd))
			cmd := make(RedisCmd, 0, 4+fieldCount*2)
			cmd = append(cmd, "phset", o.key, path, "fields", strconv.Itoa(fieldCount))
			for j := 0; j < fieldCount; j++ {
				field := structure.ReadString(o.rd)
				val := structure.ReadString(o.rd)
				cmd = append(cmd, field, val)
			}
			o.cmdC <- cmd
		}
	}()
	return o.cmdC
}
