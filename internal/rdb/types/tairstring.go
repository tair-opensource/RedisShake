package types

import (
	"io"

	"github.com/alibaba/RedisShake/internal/log"
	"github.com/alibaba/RedisShake/internal/rdb/structure"
)

type TairStringValue struct {
	version   string
	flags     string
	tairValue string
}

type TairStringObject struct {
	value TairStringValue
	key   string
}

func (o *TairStringObject) LoadFromBuffer(rd io.Reader, key string, typeByte byte) {
	o.key = key

	// o.value.version err = structure.ReadModuleUnsigned(rd)
	// println("version :", o.value.version)
	version, err := structure.ReadModuleUnsigned(rd)
	if err != nil {
		log.Panicf("fail to read the version of tairstring", err)
	} else {
		o.value.version = version
	}

	flags, err := structure.ReadModuleUnsigned(rd)
	if err != nil {
		log.Panicf("fail to read the flags of tairstring", err)
	} else {

		o.value.flags = flags
	}

	tairValue, err := structure.ReadModuleString(rd)
	if err != nil {
		log.Panicf("fail to read the value of tairstring", err)
	} else {

		o.value.tairValue = tairValue
	}

	// println("tairValue:", o.value.tairValue)

	if structure.ReadModuleEof(rd) != nil {
		log.Panicf("fail to read the EOF marker of tairstring", err)
	}

}

func (o *TairStringObject) Rewrite() []RedisCmd {
	cmd := RedisCmd{}
	cmd = append(cmd, "EXSET", o.key, o.value.tairValue, "ABS", o.value.version, "FLAGS", o.value.flags)
	// for key, value := range cmd {
	// 	println(key, value)
	// }
	return []RedisCmd{cmd}
}
