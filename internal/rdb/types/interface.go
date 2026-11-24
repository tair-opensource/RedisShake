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

	// Hash with field expiration (type 22-25) & Valkey 9.2 Path Hash (type 23)
	// Note: Types 22 and 23 have different formats across Redis and Valkey:
	//   - Type 22: Redis RDB_TYPE_HASH_METADATA_PRE_GA vs Valkey RDB_TYPE_HASH_2
	//   - Type 23: Redis RDB_TYPE_HASH_LISTPACK_EX_PRE_GA vs Valkey RDB_TYPE_PATH_HASH
	// See: https://github.com/redis/redis/pull/13391
	rdbTypeHashWithExpiry22 = 22
	rdbTypeHashWithExpiry23 = 23 // Redis: RDB_TYPE_HASH_LISTPACK_EX_PRE_GA; Valkey: RDB_TYPE_PATH_HASH
	rdbTypeHashWithExpiry24 = 24 // Redis: RDB_TYPE_HASH_METADATA
	rdbTypeHashWithExpiry25 = 25 // Redis: RDB_TYPE_HASH_LISTPACK_EX

	// Redis 8.6 - 8.12 types (26-36)
	rdbTypeStreamListpacks4 = 26 // RDB_TYPE_STREAM_LISTPACKS_4 (IDMP)
	rdbTypeStreamListpacks5 = 27 // RDB_TYPE_STREAM_LISTPACKS_5 (XNACK)
	rdbTypeArray            = 28 // RDB_TYPE_ARRAY (sparse array)
	rdbTypeHashTmplLp       = 29 // RDB_TYPE_HASH_TMPL_LP (self-contained)
	rdbTypeHashTmplLpRef    = 30 // RDB_TYPE_HASH_TMPL_LP_REF (RDB-only template ref)
	rdbTypeHashTmplArray    = 31 // RDB_TYPE_HASH_TMPL_ARRAY (self-contained)
	rdbTypeHashTmplArrayRef = 32 // RDB_TYPE_HASH_TMPL_ARRAY_REF (RDB-only template ref)
	rdbTypeBitmap           = 33 // RDB_TYPE_BITMAP (CRoaring64)
	rdbTypeSetListpackEx    = 34 // RDB_TYPE_SET_LISTPACK_EX (set listpack with member expiry)
	rdbTypeSetMetadata      = 35 // RDB_TYPE_SET_METADATA (set hashtable with member expiry)
	rdbTypeGcra             = 36 // RDB_TYPE_GCRA (GCRA rate limiter)

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

// HashObjectValkey is interface for hash objects that need Valkey format detection
type HashObjectValkey interface {
	RedisObject
	SetIsValkey(isValkey bool)
}

// RequiresRewrite reports whether typeByte cannot be sent via RESTORE and must
// be rewritten as individual commands (e.g. Redis 8.12 RDB-only hash template refs).
func RequiresRewrite(typeByte byte, isValkey bool) bool {
	return !isValkey && (typeByte == rdbTypeHashTmplLpRef || typeByte == rdbTypeHashTmplArrayRef)
}

func ParseObject(rd io.Reader, typeByte byte, key string, isValkey bool) RedisObject {
	return ParseObjectWithTemplates(rd, typeByte, key, isValkey, nil)
}

func ParseObjectWithTemplates(rd io.Reader, typeByte byte, key string, isValkey bool, hashTemplates map[uint64][]string) RedisObject {
	switch typeByte {
	case rdbTypeString: // string
		o := new(StringObject)
		o.LoadFromBuffer(rd, key, typeByte)
		return o
	case rdbTypeList, rdbTypeListZiplist, rdbTypeListQuicklist, rdbTypeListQuicklist2: // list
		o := new(ListObject)
		o.LoadFromBuffer(rd, key, typeByte)
		return o
	case rdbTypeSet, rdbTypeSetIntset, rdbTypeSetListpack, rdbTypeSetListpackEx, rdbTypeSetMetadata: // set
		o := new(SetObject)
		o.LoadFromBuffer(rd, key, typeByte)
		return o
	case rdbTypeZSet, rdbTypeZSet2, rdbTypeZSetZiplist, rdbTypeZSetListpack: // zset
		o := new(ZsetObject)
		o.LoadFromBuffer(rd, key, typeByte)
		return o
	case rdbTypeHashWithExpiry23:
		if isValkey {
			o := new(PathHashObject)
			o.LoadFromBuffer(rd, key, typeByte)
			return o
		}
		o := new(HashObject)
		o.SetIsValkey(false)
		o.LoadFromBuffer(rd, key, typeByte)
		return o
	case rdbTypeHash, rdbTypeHashZipmap, rdbTypeHashZiplist, rdbTypeHashListpack,
		rdbTypeHashWithExpiry22, rdbTypeHashWithExpiry24, rdbTypeHashWithExpiry25,
		rdbTypeHashTmplLp, rdbTypeHashTmplLpRef, rdbTypeHashTmplArray, rdbTypeHashTmplArrayRef: // hash
		o := new(HashObject)
		o.SetIsValkey(isValkey)
		o.SetHashTemplates(hashTemplates)
		o.LoadFromBuffer(rd, key, typeByte)
		return o
	case rdbTypeStreamListpacks, rdbTypeStreamListpacks2, rdbTypeStreamListpacks3,
		rdbTypeStreamListpacks4, rdbTypeStreamListpacks5: // stream
		o := new(StreamObject)
		o.LoadFromBuffer(rd, key, typeByte)
		return o
	case rdbTypeArray:
		o := new(ArrayObject)
		o.LoadFromBuffer(rd, key, typeByte)
		return o
	case rdbTypeBitmap:
		o := new(BitmapObject)
		o.LoadFromBuffer(rd, key, typeByte)
		return o
	case rdbTypeGcra:
		o := new(GcraObject)
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
