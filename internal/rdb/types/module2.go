package types

import (
	"io"

	"RedisShake/internal/log"
	"RedisShake/internal/rdb/structure"
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
	case "tairhash-":
		o := new(TairHashObject)
		o.LoadFromBuffer(rd, key, typeByte)
		return o
	case "tairzset_":
		o := new(TairZsetObject)
		o.LoadFromBuffer(rd, key, typeByte)
		return o
	case "MBbloom--":
		o := new(BloomObject)
		o.encver = int(moduleId & 1023)
		o.LoadFromBuffer(rd, key, typeByte)
		return o
	default:
		log.Panicf("unsupported module type: %s", moduleName)
		return nil

	}

}
