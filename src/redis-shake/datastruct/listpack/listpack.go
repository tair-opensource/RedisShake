package listpack

import (
	"encoding/binary"
	"github.com/alibaba/RedisShake/pkg/libs/log"
	"math"
	"strconv"
)

/*
data struct: https://l1n.wang/2020/06/redis-stream/#listpack
*/

type Listpack struct {
	data        []byte //
	p           uint32 //
	numBytes    uint32 // 4 byte, the number of bytes
	numElements uint16 // 2 byte, the number of Elements
}

func NewListpack(data []byte) *Listpack {
	lp := new(Listpack) // lp means Listpack

	lp.data = data
	lp.numBytes = binary.LittleEndian.Uint32(data[:4])
	lp.numElements = binary.LittleEndian.Uint16(data[4:6])
	lp.p = 4 + 2 // numBytes: 4byte, numElements: 2byte

	return lp
}

// return next value of Listpack
// see Redis/Listpack.c lpGet()
func (lp *Listpack) Next() string {

	inx := lp.p
	data := lp.data

	var val int64
	var uval, negstart, negmax uint64

	if (data[inx]>>7)&1 == 0 { // 7bit uint

		uval = uint64(data[inx] & 0x7f) // 7bit  to int64
		negmax = 0
		negstart = math.MaxUint64 /* 7 bit int is always positive. */

		lp.p += lpEncodeBacklen(1)

	} else if (data[inx]>>6)&1 == 0 { // 6bit str

		length := uint32(data[inx] & 0x3f) // 6bit
		lp.p += lpEncodeBacklen(1 + length)
		return string(lp.data[inx+1 : inx+1+length])

	} else if (data[inx]>>5)&1 == 0 { // 13bit int

		uval = (uint64(data[inx]&0x1f) << 8) + uint64(data[inx+1]) // 5bit + 8bit
		negstart = uint64(1) << 12
		negmax = 8191 // uint13_max

		lp.p += lpEncodeBacklen(2)

	} else if (data[inx]>>4)&1 == 0 { // 12bit str

		length := (uint32(data[inx]&0x0f) << 8) + uint32(lp.data[inx+1]) // 4bit + 8bit
		lp.p += lpEncodeBacklen(2 + length)
		return string(lp.data[inx+2 : inx+2+length])

	} else if data[inx] == 0xf0 { // 32bit str

		length := (uint32(data[inx+1]) << 0) + (uint32(data[inx+2]) << 8) + (uint32(data[inx+3]) << 16) + (uint32(data[inx+4]) << 24)
		lp.p += lpEncodeBacklen(1 + 4 + length)
		return string(lp.data[inx+1+4 : inx+1+4+length]) // encode: 1byte, str length: 4byte

	} else if data[inx] == 0xf1 { // 16bit int

		uval = (uint64(data[inx+1]) << 0) + (uint64(data[inx+2]) << 8)
		negstart = uint64(1) << 15
		negmax = 65535 // uint16_max

		lp.p += lpEncodeBacklen(1 + 2) // encode: 1byte, int: 2byte

	} else if data[inx] == 0xf2 { // 24bit int

		uval = (uint64(data[inx+1]) << 0) + (uint64(data[inx+2]) << 8) + (uint64(data[inx+3]) << 16)
		negstart = uint64(1) << 23
		negmax = math.MaxUint32 >> 8 // uint24_max

		lp.p += lpEncodeBacklen(1 + 3) // encode: 1byte, int: 3byte

	} else if data[inx] == 0xf3 { // 32bit int

		uval = (uint64(data[inx+1]) << 0) + (uint64(data[inx+2]) << 8) + (uint64(data[inx+3]) << 16) + (uint64(data[inx+4]) << 24)
		negstart = uint64(1) << 31
		negmax = math.MaxUint32 // uint32_max

		lp.p += lpEncodeBacklen(1 + 4) // encode: 1byte, int: 4byte

	} else if data[inx] == 0xf4 { // 64bit int

		uval = (uint64(data[inx+1]) << 0) + (uint64(data[inx+2]) << 8) + (uint64(data[inx+3]) << 16) + (uint64(data[inx+4]) << 24) +
			(uint64(data[inx+5]) << 32) + (uint64(data[inx+6]) << 40) + (uint64(data[inx+7]) << 48) + (uint64(data[inx+8]) << 56)
		negstart = uint64(1) << 63
		negmax = math.MaxUint64 // uint64_max

		lp.p += lpEncodeBacklen(1 + 8)

	} else { // log error
		log.Panicf("decode error! encode byte is %v\n", data[inx])
	}

	/* We reach this code path only for integer encodings.
	 * Convert the unsigned value to the signed one using two's complement
	 * rule. */
	if uval >= negstart {
		/* This three steps conversion should avoid undefined behaviors
		 * in the unsigned -> signed conversion. */

		uval = negmax - uval
		val = int64(uval)
		val = -val - 1
	} else {
		val = int64(uval)
	}

	return strconv.FormatInt(val, 10)
}

func (lp *Listpack) NextInteger() int64 {
	str := lp.Next()
	ret, err := strconv.ParseInt(str, 10, 64)
	if err != nil {
		log.Errorf("str to int error: %v\n", str)
	}
	return ret
}

/* the function just returns the length(byte) of `backlen`. */
func lpEncodeBacklen(len uint32) uint32 {
	if len <= 127 {
		return len + 1
	} else if len < 16383 {
		return len + 2
	} else if len < 2097151 {
		return len + 3
	} else if len < 268435455 {
		return len + 4
	} else {
		return len + 5
	}
}
