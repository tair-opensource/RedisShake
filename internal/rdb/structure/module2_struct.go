package structure

import (
	"fmt"
	"io"
	"log"
	"strconv"
)

const (
	rdbModuleOpcodeEOF    = 0 // End of module value.
	rdbModuleOpcodeSINT   = 1 // Signed integer.
	rdbModuleOpcodeUINT   = 2 // Unsigned integer.
	rdbModuleOpcodeFLOAT  = 3 // Float.
	rdbModuleOpcodeDOUBLE = 4 // Double.
	rdbModuleOpcodeSTRING = 5 // String.
)

func ReadModuleUnsigned(rd io.Reader) string {
	opcode := ReadByte(rd)
	if opcode != rdbModuleOpcodeUINT {
		log.Panicf("Unknown module unsigned encode type")
	}
	value := ReadLength(rd)
	return strconv.FormatUint(value, 10)
}

func ReadModuleSigned(rd io.Reader) string {
	opcode := ReadByte(rd)
	if opcode != rdbModuleOpcodeSINT {
		log.Panicf("Unknown module signed encode type")
	}
	value := ReadLength(rd)
	return strconv.FormatUint(value, 10)
}

func ReadModuleFloat(rd io.Reader) string {
	opcode := ReadByte(rd)
	if opcode != rdbModuleOpcodeDOUBLE {

		log.Panicf("Unknown module double encode type")
	}
	value := ReadDouble(rd)
	return fmt.Sprintf("%f", value)
}

func ReadModuleDouble(rd io.Reader) string {
	opcode := ReadByte(rd)
	if opcode != rdbModuleOpcodeDOUBLE {
		log.Panicf("Unknown module double encode type")
	}
	value := ReadDouble(rd)
	return fmt.Sprintf("%.15f", value)
}

func ReadModuleString(rd io.Reader) string {
	opcode := ReadByte(rd)
	if opcode != rdbModuleOpcodeSTRING {
		log.Panicf("Unknown module string encode type")
	}
	return ReadString(rd)
}

func ReadModuleEof(rd io.Reader) {
	eof := ReadLength(rd)
	if eof != rdbModuleOpcodeEOF {
		log.Panicf("The RDB file is not teminated by the proper module value EOF marker")
	}
}
