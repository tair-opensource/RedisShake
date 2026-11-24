package types

import (
	"io"
	"strconv"

	"RedisShake/internal/log"
	"RedisShake/internal/rdb/structure"
)

type HashObject struct {
	key           string
	typeByte      byte
	rd            io.Reader
	cmdC          chan RedisCmd
	isValkey      bool
	hashTemplates map[uint64][]string
}

func (o *HashObject) SetIsValkey(isValkey bool) {
	o.isValkey = isValkey
}

func (o *HashObject) SetHashTemplates(templates map[uint64][]string) {
	o.hashTemplates = templates
}

func (o *HashObject) LoadFromBuffer(rd io.Reader, key string, typeByte byte) {
	o.key = key
	o.typeByte = typeByte
	o.rd = rd
	o.cmdC = make(chan RedisCmd)
}

func (o *HashObject) Rewrite() <-chan RedisCmd {
	go func() {
		defer close(o.cmdC)
		o.cmdC <- RedisCmd{"del", o.key}
		switch o.typeByte {
		case rdbTypeHash:
			o.readHash()
		case rdbTypeHashZipmap:
			o.readHashZipmap()
		case rdbTypeHashZiplist:
			o.readHashZiplist()
		case rdbTypeHashListpack:
			o.readHashListpack()
		case rdbTypeHashWithExpiry22:
			// Type 22: Redis uses RDB_TYPE_HASH_METADATA_PRE_GA, Valkey uses RDB_TYPE_HASH_2
			if o.isValkey {
				o.readHashValkey() // Valkey 9.0 HASH_2 format: field, value, TTL (8-byte ms)
			} else {
				o.readHashTtl(true) // Redis 8.0 format: TTL (length), field, value
			}
		case rdbTypeHashWithExpiry23:
			o.readHashListpackTtl(true)
		case rdbTypeHashWithExpiry24:
			o.readHashTtl(false)
		case rdbTypeHashWithExpiry25:
			o.readHashListpackTtl(false)
		case rdbTypeHashTmplLp:
			o.readHashTmplLp()
		case rdbTypeHashTmplLpRef:
			o.readHashTmplLpRef()
		case rdbTypeHashTmplArray:
			o.readHashTmplArray()
		case rdbTypeHashTmplArrayRef:
			o.readHashTmplArrayRef()
		default:
			log.Panicf("unknown hash type. typeByte=[%d]", o.typeByte)
		}
	}()
	return o.cmdC
}

func (o *HashObject) readHash() {
	rd := o.rd
	size := int(structure.ReadLength(rd))
	for i := 0; i < size; i++ {
		key := structure.ReadString(rd)
		value := structure.ReadString(rd)
		o.cmdC <- RedisCmd{"hset", o.key, key, value}
	}
}

func (o *HashObject) readHashZipmap() {
	log.Panicf("not implemented rdbTypeZipmap")
}

func (o *HashObject) readHashZiplist() {
	rd := o.rd
	list := structure.ReadZipList(rd)
	size := len(list)
	for i := 0; i < size; i += 2 {
		key := list[i]
		value := list[i+1]
		o.cmdC <- RedisCmd{"hset", o.key, key, value}
	}
}

func (o *HashObject) readHashListpack() {
	rd := o.rd
	list := structure.ReadListpack(rd)
	size := len(list)
	for i := 0; i < size; i += 2 {
		key := list[i]
		value := list[i+1]
		o.cmdC <- RedisCmd{"hset", o.key, key, value}
	}
}

// readHashValkey reads Valkey 9.0's RDB_TYPE_HASH_2 format
// Format: size, then for each entry: field (string), value (string), TTL (8-byte ms timestamp)
func (o *HashObject) readHashValkey() {
	rd := o.rd
	size := int(structure.ReadLength(rd))
	for i := 0; i < size; i++ {
		key := structure.ReadString(rd)
		value := structure.ReadString(rd)
		// TTL is stored as 8-byte little-endian millisecond timestamp; -1 means no expiration
		expireAt := int64(structure.ReadUint64(rd))
		o.cmdC <- RedisCmd{"hset", o.key, key, value}
		if expireAt > 0 {
			o.cmdC <- RedisCmd{"hpexpireat", o.key, strconv.FormatInt(expireAt, 10), "fields", "1", key}
		}
	}
}


func (o *HashObject) readHashListpackTtl(isPre bool)  {
	rd := o.rd
	if !isPre {
		// read minExpire
		_ = int64(structure.ReadUint64(rd))
	}
	list := structure.ReadListpack(rd)
	size := len(list)
	for i := 0; i < size; i += 3 {
		key := list[i]
		value := list[i+1]

		expireAt,err := strconv.ParseInt(list[i+2], 10, 64)
		if err != nil{
			log.Panicf("readHashListpackTtl parsing expireAt %s error", list[i])
			return
		}
		o.cmdC <- RedisCmd{"hset", o.key, key, value}
		if expireAt != 0{
			o.cmdC <- RedisCmd{"hpexpireat", o.key, strconv.FormatInt(expireAt, 10), "fields", "1", key}
		}
	}
}


func (o *HashObject) readHashTtl(isPre bool){
	rd := o.rd
	var minExpire int64
	if !isPre {
		minExpire = int64(structure.ReadUint64(rd))
		log.Debugf("%s minExpire is %d", o.key, minExpire)
	}

	size := int(structure.ReadLength(rd))
	for i := 0; i < size; i++ {
		expireAt := int64(structure.ReadLength(rd))
		if !isPre{
			if expireAt != 0{
				expireAt = expireAt + minExpire - 1
			}
		}
		key := structure.ReadString(rd)
		value := structure.ReadString(rd)
		//HPEXPIREAT key unix-time-seconds [NX | XX | GT | LT] FIELDS numfields
		o.cmdC <- RedisCmd{"hset", o.key, key, value}
		if expireAt != 0{
			o.cmdC <- RedisCmd{"hpexpireat", o.key, strconv.FormatInt(expireAt, 10), "fields", "1", key}
		}
	}

}

func (o *HashObject) readHashTmplFields() []string {
	fmt := structure.ReadLength(o.rd)
	switch fmt {
	case 0:
		return structure.ReadListpack(o.rd)
	case 1:
		count := int(structure.ReadLength(o.rd))
		fields := make([]string, count)
		for i := 0; i < count; i++ {
			fields[i] = structure.ReadString(o.rd)
		}
		return fields
	default:
		log.Panicf("unknown hash template fields format. key=[%s], fmt=[%d]", o.key, fmt)
		return nil
	}
}

func (o *HashObject) readHashTmplLp() {
	fields := o.readHashTmplFields()
	values := structure.ReadListpack(o.rd)
	if len(values) != len(fields)+1 {
		log.Panicf("hash template listpack count mismatch. key=[%s], fields=[%d], values=[%d]", o.key, len(fields), len(values))
	}
	for i, field := range fields {
		o.cmdC <- RedisCmd{"hset", o.key, field, values[i+1]}
	}
}

func (o *HashObject) readHashTmplLpRef() {
	values := structure.ReadListpack(o.rd)
	if len(values) == 0 {
		log.Panicf("empty hash template listpack ref. key=[%s]", o.key)
	}
	tid, err := strconv.ParseUint(values[0], 10, 64)
	if err != nil {
		log.Panicf("invalid hash template id. key=[%s], id=[%s]", o.key, values[0])
	}
	fields, ok := o.hashTemplates[tid]
	if !ok {
		log.Panicf("hash template id not found. key=[%s], tid=[%d]", o.key, tid)
	}
	if len(values) != len(fields)+1 {
		log.Panicf("hash template listpack ref count mismatch. key=[%s], fields=[%d], values=[%d]", o.key, len(fields), len(values))
	}
	for i, field := range fields {
		o.cmdC <- RedisCmd{"hset", o.key, field, values[i+1]}
	}
}

func (o *HashObject) readHashTmplArray() {
	fields := o.readHashTmplFields()
	for _, field := range fields {
		val := structure.ReadString(o.rd)
		o.cmdC <- RedisCmd{"hset", o.key, field, val}
	}
}

func (o *HashObject) readHashTmplArrayRef() {
	tid := structure.ReadLength(o.rd)
	fields, ok := o.hashTemplates[tid]
	if !ok {
		log.Panicf("hash template id not found. key=[%s], tid=[%d]", o.key, tid)
	}
	for _, field := range fields {
		val := structure.ReadString(o.rd)
		o.cmdC <- RedisCmd{"hset", o.key, field, val}
	}
}