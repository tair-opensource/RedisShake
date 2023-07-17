package types

import (
	"io"

	"github.com/alibaba/RedisShake/internal/log"
	"github.com/alibaba/RedisShake/internal/rdb/structure"
)

type ModuleObject interface {
	RedisObject
}

func PareseModuleType(rd io.Reader, key string, typeByte byte) ModuleObject {
	if typeByte == rdbTypeModule {
		log.Panicf("module type with version 1 is not supported, key=[%s]", key)
	}
	moduleId := structure.ReadLength(rd)
	moduleName := moduleTypeNameByID(moduleId)
	switch moduleName {
	case "exstrtype":

		o := new(TairStringObject)

		o.LoadFromBuffer(rd, key, typeByte)

		return o
	default:
		log.Panicf("unsupported module type: %s", moduleName)
		return nil

	}

}
