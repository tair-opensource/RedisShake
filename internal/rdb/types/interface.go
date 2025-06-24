package types

import (
	"io"

	"RedisShake/internal/log"
)

const (
	// StringType is redis string
	StringType = "string"
	// ListType is redis list
	ListType = "list"
	// SetType is redis set
	SetType = "set"
	// HashType is redis hash
	HashType = "hash"
	// ZSetType is redis sorted set
	ZSetType = "zset"
	// AuxType is redis metadata key-value pair
	AuxType = "aux"
	// DBSizeType is for _OPCODE_RESIZEDB
	DBSizeType = "dbsize"
)

const (
	rdbTypeString  = 0 // RDB_TYPE_STRING
	rdbTypeList    = 1
	rdbTypeSet     = 2
	rdbTypeZSet    = 3
	rdbTypeHash    = 4 // RDB_TYPE_HASH
	rdbTypeZSet2   = 5 // ZSET version 2 with doubles stored in binary.
	rdbTypeModule  = 6 // RDB_TYPE_MODULE
	rdbTypeModule2 = 7 // RDB_TYPE_MODULE2 Module value with annotations for parsing without the generating module being loaded.

	// Object types for encoded objects.

	rdbTypeHashZipmap       = 9
	rdbTypeListZiplist      = 10
	rdbTypeSetIntset        = 11
	rdbTypeZSetZiplist      = 12
	rdbTypeHashZiplist      = 13
	rdbTypeListQuicklist    = 14 // RDB_TYPE_LIST_QUICKLIST
	rdbTypeStreamListpacks  = 15 // RDB_TYPE_STREAM_LISTPACKS
	rdbTypeHashListpack     = 16 // RDB_TYPE_HASH_ZIPLIST
	rdbTypeZSetListpack     = 17 // RDB_TYPE_ZSET_LISTPACK
	rdbTypeListQuicklist2   = 18 // RDB_TYPE_LIST_QUICKLIST_2 https://github.com/redis/redis/pull/9357
	rdbTypeStreamListpacks2 = 19 // RDB_TYPE_STREAM_LISTPACKS2
	rdbTypeSetListpack      = 20 // RDB_TYPE_SET_LISTPACK
	rdbTypeStreamListpacks3 = 21 // RDB_TYPE_STREAM_LISTPACKS_3

	// https://github.com/redis/redis/pull/13391
	rdbTypeHashMetadataPreGa = 22 // RDB_TYPE_HASH_METADATA_PRE_GA
	rdbTypeHashListpackExPre = 23 // RDB_TYPE_HASH_LISTPACK_EX_PRE_GA
	rdbTypeHashMetadata      = 24 // RDB_TYPE_HASH_METADATA
	rdbTypeHashListpackEx    = 25 // RDB_TYPE_HASH_LISTPACK_EX

	moduleTypeNameCharSet = "ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz0123456789-_"

	rdbModuleOpcodeEOF    = 0 // End of module value.
	rdbModuleOpcodeSINT   = 1 // Signed integer.
	rdbModuleOpcodeUINT   = 2 // Unsigned integer.
	rdbModuleOpcodeFLOAT  = 3 // Float.
	rdbModuleOpcodeDOUBLE = 4 // Double.
	rdbModuleOpcodeSTRING = 5 // String.
)

type RedisCmd []string

// RedisObject is interface for a redis object
type RedisObject interface {
	LoadFromBuffer(rd io.Reader, key string, typeByte byte)
	Rewrite() <-chan RedisCmd
}

func ParseObject(rd io.Reader, typeByte byte, key string) RedisObject {
	switch typeByte {
	case rdbTypeString: // string
		o := new(StringObject)
		o.LoadFromBuffer(rd, key, typeByte)
		return o
	case rdbTypeList, rdbTypeListZiplist, rdbTypeListQuicklist, rdbTypeListQuicklist2: // list
		o := new(ListObject)
		o.LoadFromBuffer(rd, key, typeByte)
		return o
	case rdbTypeSet, rdbTypeSetIntset, rdbTypeSetListpack: // set
		o := new(SetObject)
		o.LoadFromBuffer(rd, key, typeByte)
		return o
	case rdbTypeZSet, rdbTypeZSet2, rdbTypeZSetZiplist, rdbTypeZSetListpack: // zset
		o := new(ZsetObject)
		o.LoadFromBuffer(rd, key, typeByte)
		return o
	case rdbTypeHash, rdbTypeHashZipmap, rdbTypeHashZiplist, rdbTypeHashListpack,
		rdbTypeHashMetadataPreGa, rdbTypeHashListpackExPre, rdbTypeHashMetadata, rdbTypeHashListpackEx: // hash
		o := new(HashObject)
		o.LoadFromBuffer(rd, key, typeByte)
		return o
	case rdbTypeStreamListpacks, rdbTypeStreamListpacks2, rdbTypeStreamListpacks3: // stream
		o := new(StreamObject)
		o.LoadFromBuffer(rd, key, typeByte)
		return o
	case rdbTypeModule, rdbTypeModule2: // module
		o := PareseModuleType(rd, key, typeByte)
		return o
	}
	log.Panicf("unknown rdb value type byte. key=[%s], type=[%d]", key, typeByte)
	return nil
}

func ModuleTypeNameByID(moduleId uint64) string {
	nameList := make([]byte, 9)
	moduleId >>= 10
	for i := 8; i >= 0; i-- {
		nameList[i] = moduleTypeNameCharSet[moduleId&63]
		moduleId >>= 6
	}
	return string(nameList)
}
