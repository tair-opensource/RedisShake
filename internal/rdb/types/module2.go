package types

import (
	"io"

	"github.com/alibaba/RedisShake/internal/log"
	"github.com/alibaba/RedisShake/internal/rdb/structure"
)

type ModuleObject struct {
}

func (o *ModuleObject) LoadFromBuffer(rd io.Reader, key string, typeByte byte) {
	if typeByte == rdbTypeModule {
		log.Panicf("module type with version 1 is not supported, key=[%s]", key)
	}
	moduleId := structure.ReadLength(rd)
	moduleName := moduleTypeNameByID(moduleId)
	opcode := structure.ReadByte(rd)
	for opcode != rdbModuleOpcodeEOF {
		switch opcode {
		case rdbModuleOpcodeSINT:
		case rdbModuleOpcodeUINT:
			structure.ReadLength(rd)
		case rdbModuleOpcodeFLOAT:
			structure.ReadFloat(rd)
		case rdbModuleOpcodeDOUBLE:
			structure.ReadDouble(rd)
		case rdbModuleOpcodeSTRING:
			structure.ReadString(rd)
		default:
			log.Panicf("unknown module opcode=[%d], module name=[%s]", opcode, moduleName)
		}
		opcode = structure.ReadByte(rd)
	}
}

func (o *ModuleObject) LoadFromBufferWithOffset(rd io.Reader, key string, typeByte byte) int64 {
	if typeByte == rdbTypeModule {
		log.Panicf("module type with version 1 is not supported, key=[%s]", key)
	}
	moduleId, offset := structure.ReadLengthWithOffset(rd)
	moduleName := moduleTypeNameByID(moduleId)
	opcode := structure.ReadByte(rd)
	offset += 1
	for opcode != rdbModuleOpcodeEOF {
		switch opcode {
		case rdbModuleOpcodeSINT:
		case rdbModuleOpcodeUINT:
			_, tempOffset := structure.ReadLengthWithOffset(rd)
			offset += tempOffset
		case rdbModuleOpcodeFLOAT:
			_, tempOffset := structure.ReadFloatWithOffset(rd)
			offset += tempOffset
		case rdbModuleOpcodeDOUBLE:
			structure.ReadDouble(rd)
			offset += 8
		case rdbModuleOpcodeSTRING:
			_, tempOffset := structure.ReadStringWithOffset(rd)
			offset += tempOffset
		default:
			log.Panicf("unknown module opcode=[%d], module name=[%s]", opcode, moduleName)
		}
		opcode = structure.ReadByte(rd)
		offset += 1
	}
	return offset
}

func (o *ModuleObject) Rewrite() []RedisCmd {
	log.Panicf("module Rewrite not implemented")
	return nil
}
