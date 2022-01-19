package rdb

import (
	"bytes"
	"fmt"
)

// this function is used to parse rdb module

func (r *rdbReader) parseModule(moduleName string, moduleId uint64, t byte, b *bytes.Buffer) ([]byte, error) {
	// the module providing the 10 bit encoding version in the lower 10 bits of the module ID.
	encodeVersion := moduleId & 1023
	switch moduleName {
	case "tairhash-":
		// length
		length, err := r.moduleLoadUnsigned(t)
		if err != nil {
			return nil, err
		}

		// key
		if _, err := r.moduleLoadString(t); err != nil {
			return nil, err
		}

		for i := uint64(0); i < length; i++ {
			// skey
			if _, err := r.moduleLoadString(t); err != nil {
				return nil, err
			}

			// version
			if _, err := r.moduleLoadUnsigned(t); err != nil {
				return nil, err
			}

			// expire
			if _, err := r.moduleLoadUnsigned(t); err != nil {
				return nil, err
			}

			// value
			if _, err := r.moduleLoadString(t); err != nil {
				return nil, err
			}
		}
	case "exstrtype":
		// version
		if _, err := r.moduleLoadUnsigned(t); err != nil {
			return nil, err
		}

		if encodeVersion == 1 {
			//flag
			if _, err := r.moduleLoadUnsigned(t); err != nil {
				return nil, err
			}
		}

		// value
		if _, err := r.moduleLoadString(t); err != nil {
			return nil, err
		}

	case "tairzset_":
		length, err := r.moduleLoadUnsigned(t)
		if err != nil {
			return nil, err
		}

		score_num, err := r.moduleLoadUnsigned(t)
		if err != nil {
			return nil, err
		}

		for i := uint64(0); i < length; i++ {
			if _, err := r.moduleLoadString(t); err != nil {
				return nil, err
			}

			for j := uint64(0); j < score_num; j++ {
				if _, err := r.moduleLoadDouble(t); err != nil {
					return nil, err
				}
			}
		}

	default:
		return nil, fmt.Errorf("unknown module name[%v] with module id[%v]", moduleName, moduleId)
	}

	if t == RdbTypeModule2 {
		code, err := r.ReadLength()
		if err != nil {
			return nil, err
		} else if code != rdbModuleOpcodeEof {
			return nil, fmt.Errorf("illegal end code[%v] in module type", code)
		}
	}

	return b.Bytes(), nil
}
