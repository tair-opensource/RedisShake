package structure

import (
	"io"
	"sync"

	"RedisShake/internal/log"
)

var BytesPoolWithCap1 = sync.Pool{
	New: func() interface{} {
		tmp := make([]byte, 1)
		return tmp
	},
}

var BytesPoolWithCap2 = sync.Pool{
	New: func() interface{} {
		tmp := make([]byte, 2)
		return tmp
	},
}

var BytesPoolWithCap4 = sync.Pool{
	New: func() interface{} {
		tmp := make([]byte, 4)
		return tmp
	},
}

var BytesPoolWithCap8 = sync.Pool{
	New: func() interface{} {
		tmp := make([]byte, 8)
		return tmp
	},
}

func ReadByte(rd io.Reader) byte {
	data := BytesPoolWithCap1.Get().([]byte)
	if _, err := io.ReadFull(rd, data); err != nil {
		log.Panicf(err.Error())
	}
	result := data[0]
	BytesPoolWithCap1.Put(data)
	return result
}

func ReadBytes(rd io.Reader, n int) []byte {
	buf := make([]byte, n)
	_, err := io.ReadFull(rd, buf)
	if err != nil {
		log.Panicf(err.Error())
	}
	return buf
}
