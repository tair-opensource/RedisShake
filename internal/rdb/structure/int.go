package structure

import (
	"encoding/binary"
	"io"

	"RedisShake/internal/log"
)

func ReadUint8(rd io.Reader) uint8 {
	b := ReadByte(rd)
	return b
}

func ReadUint16(rd io.Reader) uint16 {
	data := BytesPoolWithCap2.Get().(*[]byte)
	if _, err := io.ReadFull(rd, *data); err != nil {
		log.Panicf(err.Error())
	}
	result := binary.LittleEndian.Uint16(*data)
	BytesPoolWithCap2.Put(data)
	return result
}

func ReadUint24(rd io.Reader) uint32 {
	buf := ReadBytes(rd, 3)
	buf = append(buf, 0)
	return binary.LittleEndian.Uint32(buf)
}

func ReadUint32(rd io.Reader) uint32 {
	data := BytesPoolWithCap4.Get().(*[]byte)
	if _, err := io.ReadFull(rd, *data); err != nil {
		log.Panicf(err.Error())
	}
	result := binary.LittleEndian.Uint32(*data)
	BytesPoolWithCap4.Put(data)
	return result
}

func ReadUint64(rd io.Reader) uint64 {
	data := BytesPoolWithCap8.Get().(*[]byte)
	if _, err := io.ReadFull(rd, *data); err != nil {
		log.Panicf(err.Error())
	}
	result := binary.LittleEndian.Uint64(*data)
	BytesPoolWithCap8.Put(data)
	return result
}

func ReadInt8(rd io.Reader) int8 {
	b := ReadByte(rd)
	return int8(b)
}

func ReadInt16(rd io.Reader) int16 {
	data := BytesPoolWithCap2.Get().(*[]byte)
	if _, err := io.ReadFull(rd, *data); err != nil {
		log.Panicf(err.Error())
	}
	result := int16(binary.LittleEndian.Uint16(*data))
	BytesPoolWithCap2.Put(data)
	return result
}

func ReadInt24(rd io.Reader) int32 {
	buf := ReadBytes(rd, 3)
	buf = append([]byte{0}, buf...)
	return int32(binary.LittleEndian.Uint32(buf)) >> 8
}

func ReadInt32(rd io.Reader) int32 {
	data := BytesPoolWithCap4.Get().(*[]byte)
	if _, err := io.ReadFull(rd, *data); err != nil {
		log.Panicf(err.Error())
	}
	result := int32(binary.LittleEndian.Uint32(*data))
	BytesPoolWithCap4.Put(data)
	return result
}

func ReadInt64(rd io.Reader) int64 {
	data := BytesPoolWithCap8.Get().(*[]byte)
	if _, err := io.ReadFull(rd, *data); err != nil {
		log.Panicf(err.Error())
	}
	result := int64(binary.LittleEndian.Uint64(*data))
	BytesPoolWithCap8.Put(data)
	return result
}
