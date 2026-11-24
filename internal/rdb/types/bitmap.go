package types

import (
	"io"

	"RedisShake/internal/rdb/structure"
)

// BitmapObject parses Redis 8.12 RDB_TYPE_BITMAP (type 33, CRoaring64).
// Consumed so RESTORE passthrough keeps the raw payload intact.
type BitmapObject struct {
	key  string
	rd   io.Reader
	cmdC chan RedisCmd
}

func (o *BitmapObject) LoadFromBuffer(rd io.Reader, key string, typeByte byte) {
	o.key = key
	o.rd = rd
	o.cmdC = make(chan RedisCmd)
}

func (o *BitmapObject) Rewrite() <-chan RedisCmd {
	go func() {
		defer close(o.cmdC)
		_ = structure.ReadLength(o.rd) // logical byte length
		_ = structure.ReadString(o.rd) // CRoaring64 portable blob
	}()
	return o.cmdC
}

// GcraObject parses Redis 8.12 RDB_TYPE_GCRA (type 36).
// Consumed so RESTORE passthrough keeps the raw payload intact.
type GcraObject struct {
	key  string
	rd   io.Reader
	cmdC chan RedisCmd
}

func (o *GcraObject) LoadFromBuffer(rd io.Reader, key string, typeByte byte) {
	o.key = key
	o.rd = rd
	o.cmdC = make(chan RedisCmd)
}

func (o *GcraObject) Rewrite() <-chan RedisCmd {
	go func() {
		defer close(o.cmdC)
		_ = structure.ReadLength(o.rd) // TaT timestamp
	}()
	return o.cmdC
}
