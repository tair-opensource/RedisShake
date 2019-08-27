// Copyright 2016 CodisLabs. All Rights Reserved.
// Licensed under the MIT (MIT-LICENSE.txt) license.

package rdb

import (
	"bytes"
	"encoding/binary"
	"hash"
	"io"
	"strconv"

	"pkg/libs/errors"
	"pkg/libs/log"
	"pkg/rdb/digest"
)

type Loader struct {
	*rdbReader
	crc       hash.Hash64
	db        uint32
	lastEntry *BinEntry
}

func NewLoader(r io.Reader) *Loader {
	l := &Loader{}
	l.crc = digest.New()
	l.rdbReader = NewRdbReader(io.TeeReader(r, l.crc))
	return l
}

func (l *Loader) Header() error {
	header := make([]byte, 9)
	if err := l.readFull(header); err != nil {
		return err
	}
	if !bytes.Equal(header[:5], []byte("REDIS")) {
		return errors.Errorf("verify magic string, invalid file format")
	}
	if version, err := strconv.ParseInt(string(header[5:]), 10, 64); err != nil {
		return errors.Trace(err)
	} else if version <= 0 || version > FromVersion {
		return errors.Errorf("verify version, invalid RDB version number %d, %d", version, FromVersion)
	}
	return nil
}

func (l *Loader) Footer() error {
	crc1 := l.crc.Sum64()
	if crc2, err := l.readUint64(); err != nil {
		return err
	} else if crc1 != crc2 {
		return errors.Errorf("checksum validation failed")
	}
	return nil
}

type BinEntry struct {
	DB              uint32
	Key             []byte
	Type            byte
	Value           []byte
	ExpireAt        uint64
	RealMemberCount uint32
	NeedReadLen     byte
	IdleTime        uint32
	Freq            uint8
}

func (e *BinEntry) ObjEntry() (*ObjEntry, error) {
	x, err := DecodeDump(e.Value)
	if err != nil {
		return nil, err
	}
	return &ObjEntry{
		DB:              e.DB,
		Key:             e.Key,
		Type:            e.Type,
		Value:           x,
		ExpireAt:        e.ExpireAt,
		RealMemberCount: e.RealMemberCount,
		NeedReadLen:     e.NeedReadLen,
	}, nil
}

type ObjEntry struct {
	DB              uint32
	Key             []byte
	Type            byte
	Value           interface{}
	ExpireAt        uint64
	RealMemberCount uint32
	NeedReadLen     byte
}

func (e *ObjEntry) BinEntry() (*BinEntry, error) {
	p, err := EncodeDump(e.Value)
	if err != nil {
		return nil, err
	}
	return &BinEntry{
		DB:              e.DB,
		Key:             e.Key,
		Type:            e.Type,
		Value:           p,
		ExpireAt:        e.ExpireAt,
		RealMemberCount: e.RealMemberCount,
		NeedReadLen:     e.NeedReadLen,
	}, nil
}

func (l *Loader) NextBinEntry() (*BinEntry, error) {
	var entry = &BinEntry{}
	for {
		var t byte
		if l.remainMember != 0 {
			t = l.lastEntry.Type
		} else {
			rtype, err := l.ReadByte()
			if err != nil {
				return nil, err
			}
			t = rtype
		}
		switch t {
		case RdbFlagAUX:
			aux_key, _ := l.ReadString()
			aux_value, _ := l.ReadString()
			log.Info("Aux information key:", string(aux_key), " value:", string(aux_value))
			if string(aux_key) == "lua" {
				// we should handle the lua script
				entry.DB = l.db
				entry.Key = aux_key
				entry.Type = t
				entry.Value = aux_value
				return entry, nil
			}
		case rdbFlagResizeDB:
			db_size, _ := l.ReadLength()
			expire_size, _ := l.ReadLength()
			log.Info("db_size:", db_size, " expire_size:", expire_size)
		case rdbFlagExpiryMS:
			ttlms, err := l.readUint64()
			if err != nil {
				return nil, err
			}
			entry.ExpireAt = ttlms
		case rdbFlagExpiry:
			ttls, err := l.readUint32()
			if err != nil {
				return nil, err
			}
			entry.ExpireAt = uint64(ttls) * 1000
		case rdbFlagSelectDB:
			dbnum, err := l.ReadLength()
			if err != nil {
				return nil, err
			}
			l.db = dbnum
		case rdbFlagEOF:
			return nil, nil
		case rdbFlagModuleAux:
			// currently, ignore this filed
			_, err := l.ReadLength() // module-id
			if err != nil {
				return nil, err
			}
			// skip module
			if err = rdbLoadCheckModuleValue(l); err != nil {
				return nil, err
			}
		case rdbFlagIdle:
			// ignore idle because target redis doesn't support this for given key
			idle, err := l.ReadLength()
			if err != nil {
				return nil, err
			}
			entry.IdleTime = idle
		case rdbFlagFreq:
			// ignore freq because target redis doesn't support this for given key
			freq, err := l.readUint8()
			if err != nil {
				return nil, err
			}
			entry.Freq = freq
		default:
			var key []byte
			if l.remainMember == 0 {
				// first time visit this key.
				rkey, err := l.ReadString()
				if err != nil {
					return nil, err
				}
				key = rkey
				entry.NeedReadLen = 1 // read value length when it's the first time.
			} else {
				key = l.lastEntry.Key
			}
			//log.Debugf("l %p r %p", l, l.rdbReader)
			//log.Debug("remainMember:", l.remainMember, " key:", string(key[:]), " type:", t)
			//log.Debug("r.remainMember:", l.rdbReader.remainMember)
			val, err := l.readObjectValue(t, l)
			if err != nil {
				return nil, err
			}
			entry.DB = l.db
			entry.Key = key
			entry.Type = t
			entry.Value = createValueDump(t, val)
			// entry.RealMemberCount = l.lastReadCount
			if l.lastReadCount == l.totMemberCount {
				entry.RealMemberCount = 0
			} else {
				// RealMemberCount > 0 means this is big entry which also is a split key.
				entry.RealMemberCount = l.lastReadCount
			}
			l.lastEntry = entry
			return entry, nil
		}
	}
}

func createValueDump(t byte, val []byte) []byte {
	var b bytes.Buffer
	c := digest.New()
	w := io.MultiWriter(&b, c)
	w.Write([]byte{t})
	w.Write(val)
	binary.Write(w, binary.LittleEndian, uint16(ToVersion))
	binary.Write(w, binary.LittleEndian, c.Sum64())
	return b.Bytes()
}
