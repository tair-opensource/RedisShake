package structure

import (
	"fmt"
	"io"
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

func ReadModuleUnsigned(rd io.Reader) (string, error) {
	opcode := ReadByte(rd)
	if opcode != rdbModuleOpcodeUINT {
		return "", fmt.Errorf("unknown module unsignd type")
	}
	value := ReadLength(rd)
	return strconv.FormatUint(value, 10), nil
}

func ReadModuleSigned(rd io.Reader) (string, error) {
	opcode := ReadByte(rd)
	if opcode != rdbModuleOpcodeSINT {
		return "", fmt.Errorf("unknown module signd type")
	}
	value := ReadLength(rd)
	return strconv.FormatUint(value, 10), nil
}

func ReadModuleDouble(rd io.Reader) (string, error) {
	opcode := ReadByte(rd)
	if opcode != rdbModuleOpcodeDOUBLE {
		return "", fmt.Errorf("unknown module double type")
	}
	value := ReadFloat(rd)
	return fmt.Sprintf("%f", value), nil
}

func ReadModuleFloat(rd io.Reader) (string, error) {
	opcode := ReadByte(rd)
	if opcode != rdbModuleOpcodeDOUBLE {
		return "", fmt.Errorf("unknown module float type")
	}
	value := ReadFloat(rd)
	return fmt.Sprintf("%f", value), nil
}

func ReadModuleString(rd io.Reader) (string, error) {
	opcode := ReadByte(rd)
	if opcode != rdbModuleOpcodeSTRING {
		return "", fmt.Errorf("unknown module string type")
	}
	return ReadString(rd), nil

}

func ReadModuleEof(rd io.Reader) error {
	eof := ReadLength(rd)
	if eof != rdbModuleOpcodeEOF {
		return fmt.Errorf("The RDB file is not teminated by the proper module value EOF marker")
	}
	return nil

}
