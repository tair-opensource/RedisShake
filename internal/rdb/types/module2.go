package types

import (
	"bytes"
	"encoding/binary"
	"io"
	"sync"

	"RedisShake/internal/config"
	"RedisShake/internal/log"
	"RedisShake/internal/rdb/structure"
	"RedisShake/internal/utils"
)

type ModuleObject interface {
	RedisObject
}

type UnknownModuleObject struct {
	key        string
	moduleName string
	rd         io.Reader
	cmdC       chan RedisCmd
	value      *bytes.Buffer
}

var unknownModuleNames sync.Map

func (o *UnknownModuleObject) LoadFromBuffer(rd io.Reader, key string, typeByte byte) {
	o.key = key
	o.rd = io.TeeReader(rd, o.value)
	o.cmdC = make(chan RedisCmd)
	if _, loaded := unknownModuleNames.LoadOrStore(o.moduleName, struct{}{}); !loaded {
		log.Warnf("unknown module_name=[%s]: using RESTORE; the value cannot be split into commands", o.moduleName)
	}
}

func (o *UnknownModuleObject) Rewrite() <-chan RedisCmd {
	go func() {
		defer close(o.cmdC)
		rd := o.rd
		for opcode := structure.ReadLength(rd); opcode != rdbModuleOpcodeEOF; opcode = structure.ReadLength(rd) {
			switch opcode {
			case rdbModuleOpcodeSINT, rdbModuleOpcodeUINT:
				_ = structure.ReadLength(rd)
			case rdbModuleOpcodeFLOAT:
				_ = structure.ReadUint32(rd) // Module FLOAT is binary float32.
			case rdbModuleOpcodeDOUBLE:
				_ = structure.ReadDouble(rd)
			case rdbModuleOpcodeSTRING:
				_ = structure.ReadString(rd)
			default:
				log.Panicf("unknown module opcode: key=[%s], module_name=[%s], opcode=[%d]", o.key, o.moduleName, opcode)
			}
		}
		// value already includes the type and module id; add version and CRC64.
		dumpSize := uint64(o.value.Len()) + 2 + 8
		maxBulkLen := config.Opt.Advanced.TargetRedisProtoMaxBulkLen
		if dumpSize > maxBulkLen {
			log.Panicf("unknown module requires RESTORE and cannot be split into commands: key=[%s], module_name=[%s], dump size=[%d] exceeds target_redis_proto_max_bulk_len=[%d]", o.key, o.moduleName, dumpSize, maxBulkLen)
		}
		_ = binary.Write(o.value, binary.LittleEndian, uint16(6))
		_ = binary.Write(o.value, binary.LittleEndian, utils.CalcCRC64(o.value.Bytes()))
		o.cmdC <- RedisCmd{"RESTORE", o.key, "0", o.value.String()}
	}()
	return o.cmdC
}

func PareseModuleType(rd io.Reader, key string, typeByte byte) ModuleObject {
	if typeByte == rdbTypeModule {
		log.Panicf("module type with version 1 is not supported, key=[%s]", key)
	}
	// Preserve the original module id encoding for unknown module RESTORE.
	value := bytes.NewBuffer([]byte{typeByte})
	moduleId := structure.ReadLength(io.TeeReader(rd, value))
	moduleName := ModuleTypeNameByID(moduleId)
	switch moduleName {
	case "exstrtype":
		o := new(TairStringObject)
		o.LoadFromBuffer(rd, key, typeByte)
		return o
	case "tairhash-":
		o := new(TairHashObject)
		o.LoadFromBuffer(rd, key, typeByte)
		return o
	case "tairzset_":
		o := new(TairZsetObject)
		o.LoadFromBuffer(rd, key, typeByte)
		return o
	case "MBbloom--":
		o := new(BloomObject)
		o.encver = int(moduleId & 1023)
		o.LoadFromBuffer(rd, key, typeByte)
		return o
	default:
		o := &UnknownModuleObject{moduleName: moduleName, value: value}
		o.LoadFromBuffer(rd, key, typeByte)
		return o
	}
}
