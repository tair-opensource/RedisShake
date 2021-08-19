package rdb

func rdbLoadCheckModuleValue(l *Loader) error {
	var opcode uint32
	var err error
	for {
		if opcode, err = l.ReadLength(); err != nil {
			return err
		} else if opcode == rdbModuleOpcodeEof {
			break
		}

		switch opcode {
		case rdbModuleOpcodeSint:
			fallthrough
		case rdbModuleOpcodeUint:
			if _, err = l.ReadLength(); err != nil {
				return err
			}
		case rdbModuleOpcodeString:
			if _, err = l.ReadString(); err != nil {
				return err
			}
		case rdbModuleOpcodeFloat:
			// float 32 bits
			if _, err = l.ReadFloat(); err != nil {
				return err
			}
		case rdbModuleOpcodeDouble:
			// double 64 bits
			if _, err = l.ReadDouble(); err != nil {
				return err
			}
		}
	}
	return nil
}
