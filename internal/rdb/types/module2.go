package types

import (
	"github.com/alibaba/RedisShake/internal/log"
	"github.com/alibaba/RedisShake/internal/rdb/structure"
	"io"
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

func (o *ModuleObject) Rewrite() []RedisCmd {
	log.Panicf("module Rewrite not implemented")
	return nil
}
