// Copyright 2016 CodisLabs. All Rights Reserved.
// Licensed under the MIT (MIT-LICENSE.txt) license.

package rdb

import (
	"bytes"
	"encoding/binary"
	"fmt"
	"io"
	"math"
	// "runtime/debug"
	"strconv"

	"pkg/libs/errors"

)

var FromVersion int64 = 9
var ToVersion int64 = 6

const (
	RdbTypeString = 0
	RdbTypeList   = 1
	RdbTypeSet    = 2
	RdbTypeZSet   = 3
	RdbTypeHash   = 4
	RdbTypeZSet2  = 5

	RdbTypeHashZipmap      = 9
	RdbTypeListZiplist     = 10
	RdbTypeSetIntset       = 11
	RdbTypeZSetZiplist     = 12
	RdbTypeHashZiplist     = 13
	RdbTypeQuicklist       = 14
	RDBTypeStreamListPacks = 15 // stream

	rdbFlagModuleAux = 0xf7
	rdbFlagIdle      = 0xf8
	rdbFlagFreq      = 0xf9
	RdbFlagAUX       = 0xfa
	rdbFlagResizeDB  = 0xfb
	rdbFlagExpiryMS  = 0xfc
	rdbFlagExpiry    = 0xfd
	rdbFlagSelectDB  = 0xfe
	rdbFlagEOF       = 0xff

	// Module serialized values sub opcodes
	rdbModuleOpcodeEof    = 0
	rdbModuleOpcodeSint   = 1
	rdbModuleOpcodeUint   = 2
	rdbModuleOpcodeFloat  = 3
	rdbModuleOpcodeDouble = 4
	rdbModuleOpcodeString = 5
)

const (
	rdb6bitLen  = 0
	rdb14bitLen = 1
	rdb32bitLen = 0x80
	rdb64bitLen = 0x81
	rdbEncVal   = 3

	rdbEncInt8  = 0
	rdbEncInt16 = 1
	rdbEncInt32 = 2
	rdbEncLZF   = 3

	rdbZiplist6bitlenString  = 0
	rdbZiplist14bitlenString = 1
	rdbZiplist32bitlenString = 2

	rdbZiplistInt16 = 0xc0
	rdbZiplistInt32 = 0xd0
	rdbZiplistInt64 = 0xe0
	rdbZiplistInt24 = 0xf0
	rdbZiplistInt8  = 0xfe
	rdbZiplistInt4  = 15
)

type rdbReader struct {
	raw            io.Reader
	buf            [8]byte
	nread          int64
	remainMember   uint32
	lastReadCount  uint32
	totMemberCount uint32
}

func NewRdbReader(r io.Reader) *rdbReader {
	return &rdbReader{raw: r, remainMember: 0, lastReadCount: 0}
}

func (r *rdbReader) Read(p []byte) (int, error) {
	n, err := r.raw.Read(p)
	r.nread += int64(n)
	return n, errors.Trace(err)
}

func (r *rdbReader) offset() int64 {
	return r.nread
}

func (r *rdbReader) readObjectValue(t byte, l *Loader) ([]byte, error) {
	var b bytes.Buffer
	r = NewRdbReader(io.TeeReader(r, &b)) // the result will be written into b when calls r.Read()
	lr := l.rdbReader
	switch t {
	default:
		return nil, errors.Errorf("unknown object-type %02x", t)
	case RdbFlagAUX:
		fallthrough
	case rdbFlagResizeDB:
		fallthrough
	case RdbTypeHashZipmap:
		fallthrough
	case RdbTypeListZiplist:
		fallthrough
	case RdbTypeSetIntset:
		fallthrough
	case RdbTypeZSetZiplist:
		fallthrough
	case RdbTypeHashZiplist:
		fallthrough
	case RdbTypeString:
		lr.lastReadCount, lr.remainMember, lr.totMemberCount = 0, 0, 0
		_, err := r.ReadString()
		if err != nil {
			return nil, err
		}
	case RdbTypeList, RdbTypeSet, RdbTypeQuicklist:
		lr.lastReadCount, lr.remainMember, lr.totMemberCount = 0, 0, 0
		if n, err := r.ReadLength(); err != nil {
			return nil, err
		} else {
			for i := 0; i < int(n); i++ {
				if _, err := r.ReadString(); err != nil {
					return nil, err
				}
			}
		}
	case RdbTypeZSet, RdbTypeZSet2:
		lr.lastReadCount, lr.remainMember, lr.totMemberCount = 0, 0, 0
		if n, err := r.ReadLength(); err != nil {
			return nil, err
		} else {
			// log.Debug("zset length: ", n)
			for i := 0; i < int(n); i++ {
				if _, err := r.ReadString(); err != nil {
					return nil, err
				}
				// log.Debug("zset read: ", i)
				if t == RdbTypeZSet2 {
					if _, err := r.ReadDouble(); err != nil {
						return nil, err
					}
				} else {
					if _, err := r.ReadFloat(); err != nil {
						return nil, err
					}
				}
			}
		}
	case RdbTypeHash:
		var n uint32
		if lr.remainMember != 0 {
			n = lr.remainMember
		} else {
			rlen, err := r.ReadLength()
			if err != nil {
				return nil, err
			} else {
				n = rlen
				lr.totMemberCount = rlen
			}
		}
		lr.lastReadCount = 0
		for i := 0; i < int(n); i++ {
			// read twice for hash field and value
			if _, err := r.ReadString(); err != nil {
				return nil, err
			}
			if _, err := r.ReadString(); err != nil {
				return nil, err
			}
			lr.lastReadCount++
			if b.Len() > 16*1024*1024 && i != int(n-1) {
				lr.remainMember = n - uint32(i) - 1
				// log.Infof("r %p", lr)
				// log.Info("r: ", lr, " set remainMember:", lr.remainMember)
				// debug.FreeOSMemory()
				break
			}
		}
		if lr.lastReadCount == n {
			lr.remainMember = 0
		}
	case RDBTypeStreamListPacks:
		// TODO, need to judge big key
		lr.lastReadCount, lr.remainMember, lr.totMemberCount = 0, 0, 0
		// list pack length
		nListPacks, err := r.ReadLength()
		if err != nil {
			return nil, err
		}
		for i := 0; i < int(nListPacks); i++ {
			// read twice
			if _, err := r.ReadString(); err != nil {
				return nil, err
			}
			if _, err := r.ReadString(); err != nil {
				return nil, err
			}
		}

		// items
		if _, err := r.ReadLength(); err != nil {
			return nil, err
		}
		// last_entry_id timestamp second
		if _, err := r.ReadLength(); err != nil {
			return nil, err
		}
		// last_entry_id timestamp millisecond
		if _, err := r.ReadLength(); err != nil {
			return nil, err
		}

		// cgroups length
		nCgroups, err := r.ReadLength()
		if err != nil {
			return nil, err
		}
		for i := 0; i < int(nCgroups); i++ {
			// cname
			if _, err := r.ReadString(); err != nil {
				return nil, err
			}

			// last_cg_entry_id timestamp second
			if _, err := r.ReadLength(); err != nil {
				return nil, err
			}
			// last_cg_entry_id timestamp millisecond
			if _, err := r.ReadLength(); err != nil {
				return nil, err
			}

			// pending number
			nPending, err := r.ReadLength()
			if err != nil {
				return nil, err
			}
			for i := 0; i < int(nPending); i++ {
				// eid, read 16 bytes
				b := make([]byte, 16)
				if err := r.readFull(b); err != nil {
					return nil, err
				}

				// seen_time
				b = make([]byte, 8)
				if err := r.readFull(b); err != nil {
					return nil, err
				}

				// delivery_count
				if _, err := r.ReadLength(); err != nil {
					return nil, err
				}
			}

			// consumers
			nConsumers, err := r.ReadLength()
			if err != nil {
				return nil, err
			}
			for i := 0; i < int(nConsumers); i++ {
				// cname
				if _, err := r.ReadString(); err != nil {
					return nil, err
				}

				// seen_time
				b := make([]byte, 8)
				if err := r.readFull(b); err != nil {
					return nil, err
				}

				// pending
				nPending2, err := r.ReadLength()
				if err != nil {
					return nil, err
				}
				for i := 0; i < int(nPending2); i++ {
					// eid, read 16 bytes
					b := make([]byte, 16)
					if err := r.readFull(b); err != nil {
						return nil, err
					}
				}
			}
		}
	}
	return b.Bytes(), nil
}

func (r *rdbReader) ReadString() ([]byte, error) {
	length, encoded, err := r.readEncodedLength()
	if err != nil {
		return nil, err
	}
	if !encoded {
		return r.ReadBytes(int(length))
	}
	switch t := uint8(length); t {
	default:
		return nil, errors.Errorf("invalid encoded-string %02x", t)
	case rdbEncInt8:
		i, err := r.readInt8()
		return []byte(strconv.FormatInt(int64(i), 10)), err
	case rdbEncInt16:
		i, err := r.readInt16()
		return []byte(strconv.FormatInt(int64(i), 10)), err
	case rdbEncInt32:
		i, err := r.readInt32()
		return []byte(strconv.FormatInt(int64(i), 10)), err
	case rdbEncLZF:
		var inlen, outlen uint32
		if inlen, err = r.ReadLength(); err != nil {
			return nil, err
		}
		if outlen, err = r.ReadLength(); err != nil {
			return nil, err
		}
		if in, err := r.ReadBytes(int(inlen)); err != nil {
			return nil, err
		} else {
			return lzfDecompress(in, int(outlen))
		}
	}
}

func (r *rdbReader) readEncodedLength() (length uint32, encoded bool, err error) {
	u, err := r.readUint8()
	if err != nil {
		return
	}
	switch u >> 6 {
	case rdb6bitLen:
		length = uint32(u & 0x3f)
	case rdb14bitLen:
		var u2 uint8
		u2, err = r.readUint8()
		length = (uint32(u & 0x3f) << 8) + uint32(u2)
	case rdbEncVal:
		encoded = true
		length = uint32(u & 0x3f)
	default:
		switch u {
		case rdb32bitLen:
			length, err = r.readUint32BigEndian()
		case rdb64bitLen:
			length, err = r.readUint64BigEndian()
		default:
			length, err = 0, fmt.Errorf("unknown encoding length[%v]", u)
		}
	}
	return
}

func (r *rdbReader) ReadLength() (uint32, error) {
	length, encoded, err := r.readEncodedLength()
	if err == nil && encoded {
		err = errors.Errorf("encoded-length")
	}
	return length, err
}

func (r *rdbReader) ReadDouble() (float64, error) {
	var buf = make([]byte, 8)
	err := r.readFull(buf)
	if err != nil {
		return 0, err
	}
	bits := binary.LittleEndian.Uint64(buf)
	return float64(math.Float64frombits(bits)), nil
}

func (r *rdbReader) ReadFloat() (float64, error) {
	u, err := r.readUint8()
	if err != nil {
		return 0, err
	}
	switch u {
	case 253:
		return math.NaN(), nil
	case 254:
		return math.Inf(0), nil
	case 255:
		return math.Inf(-1), nil
	default:
		if b, err := r.ReadBytes(int(u)); err != nil {
			return 0, err
		} else {
			v, err := strconv.ParseFloat(string(b), 64)
			return v, errors.Trace(err)
		}
	}
}

func (r *rdbReader) ReadByte() (byte, error) {
	b := r.buf[:1]
	_, err := io.ReadFull(r, b)
	return b[0], errors.Trace(err)
}

func (r *rdbReader) readFull(p []byte) error {
	_, err := io.ReadFull(r, p)
	return errors.Trace(err)
}

func (r *rdbReader) ReadBytes(n int) ([]byte, error) {
	p := make([]byte, n)
	return p, r.readFull(p)
}

func (r *rdbReader) readUint8() (uint8, error) {
	b, err := r.ReadByte()
	return uint8(b), err
}

func (r *rdbReader) readUint16() (uint16, error) {
	b := r.buf[:2]
	err := r.readFull(b)
	return binary.LittleEndian.Uint16(b), err
}

func (r *rdbReader) readUint32() (uint32, error) {
	b := r.buf[:4]
	err := r.readFull(b)
	return binary.LittleEndian.Uint32(b), err
}

func (r *rdbReader) readUint64() (uint64, error) {
	b := r.buf[:8]
	err := r.readFull(b)
	return binary.LittleEndian.Uint64(b), err
}

func (r *rdbReader) readUint32BigEndian() (uint32, error) {
	b := r.buf[:4]
	err := r.readFull(b)
	return binary.BigEndian.Uint32(b), err
}

func (r *rdbReader) readUint64BigEndian() (uint32, error) {
	b := r.buf[:8]
	err := r.readFull(b)
	return binary.BigEndian.Uint32(b), err
}

func (r *rdbReader) readInt8() (int8, error) {
	u, err := r.readUint8()
	return int8(u), err
}

func (r *rdbReader) readInt16() (int16, error) {
	u, err := r.readUint16()
	return int16(u), err
}

func (r *rdbReader) readInt32() (int32, error) {
	u, err := r.readUint32()
	return int32(u), err
}

func (r *rdbReader) readInt64() (int64, error) {
	u, err := r.readUint64()
	return int64(u), err
}

func (r *rdbReader) readInt32BigEndian() (int32, error) {
	u, err := r.readUint32BigEndian()
	return int32(u), err
}

func lzfDecompress(in []byte, outlen int) (out []byte, err error) {
	defer func() {
		if x := recover(); x != nil {
			err = errors.Errorf("decompress exception: %v", x)
		}
	}()
	out = make([]byte, outlen)
	i, o := 0, 0
	for i < len(in) {
		ctrl := int(in[i])
		i++
		if ctrl < 32 {
			for x := 0; x <= ctrl; x++ {
				out[o] = in[i]
				i++
				o++
			}
		} else {
			length := ctrl >> 5
			if length == 7 {
				length = length + int(in[i])
				i++
			}
			ref := o - ((ctrl & 0x1f) << 8) - int(in[i]) - 1
			i++
			for x := 0; x <= length+1; x++ {
				out[o] = out[ref]
				ref++
				o++
			}
		}
	}
	if o != outlen {
		return nil, errors.Errorf("decompress length is %d != expected %d", o, outlen)
	}
	return out, nil
}

func (r *rdbReader) ReadZiplistLength(buf *sliceBuffer) (int64, error) {
	buf.Seek(8, 0) // skip the zlbytes and zltail
	lenBytes, err := buf.Slice(2)
	if err != nil {
		return 0, err
	}
	return int64(binary.LittleEndian.Uint16(lenBytes)), nil
}

func (r *rdbReader) ReadZiplistEntry(buf *sliceBuffer) ([]byte, error) {
	prevLen, err := buf.ReadByte()
	if err != nil {
		return nil, err
	}
	if prevLen == 254 {
		buf.Seek(4, 1) // skip the 4-byte prevlen
	}

	header, err := buf.ReadByte()
	if err != nil {
		return nil, err
	}
	switch {
	case header>>6 == rdbZiplist6bitlenString:
		return buf.Slice(int(header & 0x3f))
	case header>>6 == rdbZiplist14bitlenString:
		b, err := buf.ReadByte()
		if err != nil {
			return nil, err
		}
		return buf.Slice((int(header&0x3f) << 8) | int(b))
	case header>>6 == rdbZiplist32bitlenString:
		lenBytes, err := buf.Slice(4)
		if err != nil {
			return nil, err
		}
		return buf.Slice(int(binary.BigEndian.Uint32(lenBytes)))
	case header == rdbZiplistInt16:
		intBytes, err := buf.Slice(2)
		if err != nil {
			return nil, err
		}
		return []byte(strconv.FormatInt(int64(int16(binary.LittleEndian.Uint16(intBytes))), 10)), nil
	case header == rdbZiplistInt32:
		intBytes, err := buf.Slice(4)
		if err != nil {
			return nil, err
		}
		return []byte(strconv.FormatInt(int64(int32(binary.LittleEndian.Uint32(intBytes))), 10)), nil
	case header == rdbZiplistInt64:
		intBytes, err := buf.Slice(8)
		if err != nil {
			return nil, err
		}
		return []byte(strconv.FormatInt(int64(binary.LittleEndian.Uint64(intBytes)), 10)), nil
	case header == rdbZiplistInt24:
		intBytes := make([]byte, 4)
		_, err := buf.Read(intBytes[1:])
		if err != nil {
			return nil, err
		}
		return []byte(strconv.FormatInt(int64(int32(binary.LittleEndian.Uint32(intBytes))>>8), 10)), nil
	case header == rdbZiplistInt8:
		b, err := buf.ReadByte()
		return []byte(strconv.FormatInt(int64(int8(b)), 10)), err
	case header>>4 == rdbZiplistInt4:
		return []byte(strconv.FormatInt(int64(header&0x0f)-1, 10)), nil
	}

	return nil, fmt.Errorf("rdb: unknown ziplist header byte: %d", header)
}

func (r *rdbReader) ReadZipmapItem(buf *sliceBuffer, readFree bool) ([]byte, error) {
	length, free, err := readZipmapItemLength(buf, readFree)
	if err != nil {
		return nil, err
	}
	if length == -1 {
		return nil, nil
	}
	value, err := buf.Slice(length)
	if err != nil {
		return nil, err
	}
	_, err = buf.Seek(int64(free), 1)
	return value, err
}

func readZipmapItemLength(buf *sliceBuffer, readFree bool) (int, int, error) {
	b, err := buf.ReadByte()
	if err != nil {
		return 0, 0, err
	}
	switch b {
	case 253:
		s, err := buf.Slice(5)
		if err != nil {
			return 0, 0, err
		}
		return int(binary.BigEndian.Uint32(s)), int(s[4]), nil
	case 254:
		return 0, 0, fmt.Errorf("rdb: invalid zipmap item length")
	case 255:
		return -1, 0, nil
	}
	var free byte
	if readFree {
		free, err = buf.ReadByte()
	}
	return int(b), int(free), err
}

func (r *rdbReader) CountZipmapItems(buf *sliceBuffer) (int, error) {
	n := 0
	for {
		strLen, free, err := readZipmapItemLength(buf, n%2 != 0)
		if err != nil {
			return 0, err
		}
		if strLen == -1 {
			break
		}
		_, err = buf.Seek(int64(strLen)+int64(free), 1)
		if err != nil {
			return 0, err
		}
		n++
	}
	_, err := buf.Seek(0, 0)
	return n, err
}
