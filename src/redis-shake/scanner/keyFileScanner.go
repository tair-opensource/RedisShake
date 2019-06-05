package scanner

import (
	"bufio"
	"os"

	"redis-shake/configure"
)

type KeyFileScanner struct {
	f       *os.File
	bufScan *bufio.Scanner
	cnt     int // mark the number of this scan. init: -1
}

func (kfs *KeyFileScanner) ScanKey() ([]string, error) {
	keys := make([]string, 0, conf.Options.ScanKeyNumber)
	for i := 0; i < int(conf.Options.ScanKeyNumber) && kfs.bufScan.Scan(); i++ {
		keys = append(keys, kfs.bufScan.Text())
	}

	kfs.cnt = len(keys)

	return keys, kfs.bufScan.Err()
}

func (kfs *KeyFileScanner) EndNode() bool {
	return kfs.cnt != int(conf.Options.ScanKeyNumber)
}

func (kfs *KeyFileScanner) Close() {
	kfs.f.Close()
}
