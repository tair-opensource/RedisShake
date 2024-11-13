package rdb

import (
	"bufio"
	"bytes"
	"context"
	"encoding/binary"
	"io"
	"os"
	"strconv"
	"time"

	"RedisShake/internal/entry"
	"RedisShake/internal/log"
	"RedisShake/internal/rdb/structure"
	"RedisShake/internal/rdb/types"
	"RedisShake/internal/utils"
)

const (
	kFlagSlotInfo  = 244 // (Redis 7.4) RDB_OPCODE_SLOT_INFO: slot info
	kFlagFunction2 = 245 // RDB_OPCODE_FUNCTION2: function library data
	kFlagFunction  = 246 // RDB_OPCODE_FUNCTION_PRE_GA: old function library data for 7.0 rc1 and rc2
	kFlagModuleAux = 247 // RDB_OPCODE_MODULE_AUX: Module auxiliary data.
	kFlagIdle      = 248 // RDB_OPCODE_IDLE: LRU idle time.
	kFlagFreq      = 249 // RDB_OPCODE_FREQ: LFU frequency.
	kFlagAUX       = 250 // RDB_OPCODE_AUX: RDB aux field.
	kFlagResizeDB  = 251 // RDB_OPCODE_RESIZEDB: Hash table resize hint.
	kFlagExpireMs  = 252 // RDB_OPCODE_EXPIRETIME_MS: Expire time in milliseconds.
	kFlagExpire    = 253 // RDB_OPCODE_EXPIRETIME: Old expire time in seconds.
	kFlagSelect    = 254 // RDB_OPCODE_SELECTDB: DB number of the following keys.
	kEOF           = 255 // RDB_OPCODE_EOF: End of the RDB file.
)

const (
	kRDBModuleOpcodeEOF    = 0 // RDB_MODULE_OPCODE_EOF: End of module value.
	kRDBModuleOpcodeSINT   = 1 // RDB_MODULE_OPCODE_SINT: Signed integer.
	kRDBModuleOpcodeUINT   = 2 // RDB_MODULE_OPCODE_UINT: Unsigned integer.
	kRDBModuleOpcodeFLOAT  = 3 // RDB_MODULE_OPCODE_FLOAT: Float.
	kRDBModuleOpcodeDOUBLE = 4 // RDB_MODULE_OPCODE_DOUBLE: Double.
	kRDBModuleOpcodeSTRING = 5 // RDB_MODULE_OPCODE_STRING: String.
)

type Loader struct {
	replStreamDbId int // https://github.com/tair-opensource/RedisShake/pull/430#issuecomment-1099014464

	nowDBId  int
	expireMs int64
	idle     int64
	freq     int64

	filPath string
	fp      *os.File

	ch         chan *entry.Entry
	dumpBuffer bytes.Buffer

	name       string
	updateFunc func(int64)
}

func NewLoader(name string, updateFunc func(int64), filPath string, ch chan *entry.Entry) *Loader {
	ld := new(Loader)
	ld.ch = ch
	ld.filPath = filPath
	ld.name = name
	ld.updateFunc = updateFunc
	return ld
}

// ParseRDB parse rdb file
// return repl stream db id
func (ld *Loader) ParseRDB(ctx context.Context) int {
	var err error
	ld.fp, err = os.OpenFile(ld.filPath, os.O_RDONLY, 0666)
	if err != nil {
		log.Panicf("open file failed. file_path=[%s], error=[%s]", ld.filPath, err)
	}
	defer func() {
		err = ld.fp.Close()
		if err != nil {
			log.Panicf("close file failed. file_path=[%s], error=[%s]", ld.filPath, err)
		}
	}()
	rd := bufio.NewReader(ld.fp)
	// magic + version
	buf := make([]byte, 9)
	_, err = io.ReadFull(rd, buf)
	if err != nil {
		log.Panicf(err.Error())
	}
	if !bytes.Equal(buf[:5], []byte("REDIS")) {
		log.Panicf("verify magic string, invalid file format. bytes=[%v]", buf[:5])
	}
	version, err := strconv.Atoi(string(buf[5:]))
	if err != nil {
		log.Panicf(err.Error())
	}
	log.Debugf("[%s] RDB version: %d", ld.name, version)

	// read entries
	ld.parseRDBEntry(ctx, rd)

	return ld.replStreamDbId
}

func (ld *Loader) parseRDBEntry(ctx context.Context, rd *bufio.Reader) {
	// for stat
	updateProcessSize := func() {
		if ld.updateFunc == nil {
			return
		}
		offset, err := ld.fp.Seek(0, io.SeekCurrent)
		if err != nil {
			log.Panicf(err.Error())
		}
		ld.updateFunc(offset)
	}
	defer updateProcessSize()

	// read one entry
	ticker := time.NewTicker(time.Second * 1)
	defer ticker.Stop()
	for {
		typeByte := structure.ReadByte(rd)
		log.Debugf("RDB type byte is: [%d]", typeByte)
		switch typeByte {
		case kFlagSlotInfo:
			_ = structure.ReadLength(rd) // slot_id
			_ = structure.ReadLength(rd) // slot_size
			_ = structure.ReadLength(rd) // expires_slot_size
		case kFlagFunction, kFlagFunction2:
			log.Panicf("function library data not supported, need PR to support")
		case kFlagModuleAux:
			moduleId := structure.ReadLength(rd) // module id
			moduleName := types.ModuleTypeNameByID(moduleId)
			log.Debugf("[%s] RDB module aux: module_id=[%d], module_name=[%s]", ld.name, moduleId, moduleName)
			_ = structure.ReadLength(rd) // when_opcode
			_ = structure.ReadLength(rd) // when
			opcode := structure.ReadLength(rd)
			for opcode != kRDBModuleOpcodeEOF {
				switch opcode {
				case kRDBModuleOpcodeSINT, kRDBModuleOpcodeUINT:
					_ = structure.ReadLength(rd)
				case kRDBModuleOpcodeFLOAT:
					_ = structure.ReadFloat(rd)
				case kRDBModuleOpcodeDOUBLE:
					_ = structure.ReadDouble(rd)
				case kRDBModuleOpcodeSTRING:
					_ = structure.ReadString(rd)
				default:
					log.Panicf("module aux opcode not found. module_name=[%s], opcode=[%d]", moduleName, opcode)
				}
				opcode = structure.ReadLength(rd)
			}
		case kFlagIdle:
			ld.idle = int64(structure.ReadLength(rd))
		case kFlagFreq:
			ld.freq = int64(structure.ReadByte(rd))
		case kFlagAUX:
			key := structure.ReadString(rd)
			value := structure.ReadString(rd)
			if key == "repl-stream-db" {
				var err error
				ld.replStreamDbId, err = strconv.Atoi(value)
				if err != nil {
					log.Panicf(err.Error())
				}
				log.Debugf("[%s] RDB repl-stream-db: [%s]", ld.name, value)
			} else if key == "lua" {
				e := entry.NewEntry()
				e.Argv = []string{"script", "load", value}
				ld.ch <- e
				log.Debugf("[%s] LUA script: [%s]", ld.name, value)
			} else {
				log.Debugf("[%s] RDB AUX: key=[%s], value=[%s]", ld.name, key, value)
			}
		case kFlagResizeDB:
			dbSize := structure.ReadLength(rd)
			expireSize := structure.ReadLength(rd)
			log.Debugf("[%s] RDB resize db: db_size=[%d], expire_size=[%d]", ld.name, dbSize, expireSize)
		case kFlagExpireMs:
			ld.expireMs = int64(structure.ReadUint64(rd)) - time.Now().UnixMilli()
			if ld.expireMs < 0 {
				ld.expireMs = 1
			}
		case kFlagExpire:
			ld.expireMs = int64(structure.ReadUint32(rd))*1000 - time.Now().UnixMilli()
			if ld.expireMs < 0 {
				ld.expireMs = 1
			}
		case kFlagSelect:
			ld.nowDBId = int(structure.ReadLength(rd))
		case kEOF:
			return
		default:
			key := structure.ReadString(rd)
			o := types.ParseObject(rd, typeByte, key)
			cmdC := o.Rewrite()
			for cmd := range cmdC {
				e := entry.NewEntry()
				e.DbId = ld.nowDBId
				e.Argv = cmd
				ld.ch <- e
			}
			if ld.expireMs != 0 {
				e := entry.NewEntry()
				e.DbId = ld.nowDBId
				e.Argv = []string{"PEXPIRE", key, strconv.FormatInt(ld.expireMs, 10)}
				ld.ch <- e
			}
			ld.expireMs = 0
			ld.idle = 0
			ld.freq = 0
		}
		select {
		case <-ticker.C:
			updateProcessSize()
		case <-ctx.Done():
			return
		default:
		}
	}
}

func (ld *Loader) createValueDump(typeByte byte, val []byte) string {
	ld.dumpBuffer.Reset()
	_, _ = ld.dumpBuffer.Write([]byte{typeByte})
	_, _ = ld.dumpBuffer.Write(val)
	_ = binary.Write(&ld.dumpBuffer, binary.LittleEndian, uint16(6))
	// calc crc
	sum64 := utils.CalcCRC64(ld.dumpBuffer.Bytes())
	_ = binary.Write(&ld.dumpBuffer, binary.LittleEndian, sum64)
	return ld.dumpBuffer.String()
}
