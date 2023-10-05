package aof

import (
	"bufio"
	"io"
	"os"
	"strconv"
	"strings"

	"RedisShake/internal/entry"
	"RedisShake/internal/log"
)

const (
	AOFNotExist  = 1
	AOFOpenErr   = 3
	AOFOK        = 0
	AOFEmpty     = 2
	AOFFailed    = 4
	AOFTruncated = 5
	SizeMax      = 128
)

type Loader struct {
	filPath string
	ch      chan *entry.Entry
}

func NewLoader(filPath string, ch chan *entry.Entry) *Loader {
	ld := new(Loader)
	ld.ch = ch
	ld.filPath = filPath
	return ld
}

func ReadCompleteLine(reader *bufio.Reader) ([]byte, error) {
	line, isPrefix, err := reader.ReadLine()
	if err != nil {
		return nil, err
	}

	for isPrefix {
		var additional []byte
		additional, isPrefix, err = reader.ReadLine()
		if err != nil {
			return nil, err
		}
		line = append(line, additional...)
	}

	return line, err
}

func (ld *Loader) LoadSingleAppendOnlyFile(AOFTimeStamp int64) int {
	ret := AOFOK
	AOFFilepath := ld.filPath
	fp, err := os.Open(AOFFilepath)
	if err != nil {
		if os.IsNotExist(err) {
			if _, err := os.Stat(AOFFilepath); err == nil || !os.IsNotExist(err) {
				log.Infof("Fatal error: can't open the append log File %v for reading: %v", AOFFilepath, err.Error())
				return AOFOpenErr
			} else {
				log.Infof("The append log File %v doesn't exist: %v", AOFFilepath, err.Error())
				return AOFNotExist
			}

		}
		defer fp.Close()

		stat, _ := fp.Stat()
		if stat.Size() == 0 {
			return AOFEmpty
		}
	}
	reader := bufio.NewReader(fp)
	for {

		line, err := ReadCompleteLine(reader)
		{
			if err != nil {
				if err == io.EOF {
					break
				} else {
					log.Infof("Unrecoverable error reading the append only File %v: %v", AOFFilepath, err)
					ret = AOFFailed
					return ret
				}
			} else {
				_, errs := fp.Seek(0, io.SeekCurrent)
				if errs != nil {
					log.Infof("Unrecoverable error reading the append only File %v: %v", AOFFilepath, errs)
					ret = AOFFailed
					return ret
				}
			}

			if line[0] == '#' {
				if AOFTimeStamp != 0 && strings.HasPrefix(string(line), "#TS:") {
					var ts int64
					ts, err = strconv.ParseInt(strings.TrimPrefix(string(line), "#TS:"), 10, 64)
					if err != nil {
						log.Panicf("Invalid timestamp annotation")
					}

					if ts > AOFTimeStamp {
						ret = AOFTruncated
						log.Infof("Reached recovery timestamp: %s, subsequent data will no longer be read.", line)
						return ret
					}
				}
				continue
			}
			if line[0] != '*' {
				log.Panicf("Bad File format reading the append only File %v:make a backup of your AOF File, then use ./redis-check-AOF --fix <FileName.manifest>", AOFFilepath)
			}
			argc, _ := strconv.ParseInt(string(line[1:]), 10, 64)
			if argc < 1 {
				log.Panicf("Bad File format reading the append only File %v:make a backup of your AOF File, then use ./redis-check-AOF --fix <FileName.manifest>", AOFFilepath)
			}
			if argc > int64(SizeMax) {
				log.Panicf("Bad File format reading the append only File %v:make a backup of your AOF File, then use ./redis-check-AOF --fix <FileName.manifest>", AOFFilepath)
			}
			e := entry.NewEntry()
			var argv []string

			for j := 0; j < int(argc); j++ {
				line, err := ReadCompleteLine(reader)
				if err != nil || line[0] != '$' {
					log.Infof("Bad File format reading the append only File %v:make a backup of your AOF File, then use ./redis-check-AOF --fix <FileName.manifest>", AOFFilepath)
					ret = AOFFailed
					return ret
				}
				v64, _ := strconv.ParseInt(string(line[1:]), 10, 64)
				argString := make([]byte, v64+2)
				argString, err = ReadCompleteLine(reader)
				if err != nil {
					log.Infof("Unrecoverable error reading the append only File %v: %v", AOFFilepath, err)
					ret = AOFFailed
					return ret
				}
				argString = argString[:v64]
				argv = append(argv, string(argString))
			}

			for _, value := range argv {
				e.Argv = append(e.Argv, value)
			}
			ld.ch <- e

		}

	}
	return ret
}
