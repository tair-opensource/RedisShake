package aof

import (
	"bufio"
	"context"
	"io"
	"os"
	"strconv"
	"strings"

	"RedisShake/internal/entry"
	"RedisShake/internal/log"
)

const (
	NotExist  = 1
	OpenErr   = 3
	OK        = 0
	Empty     = 2
	Failed    = 4
	Truncated = 5
	SizeMax   = 128
)

type Loader struct {
	filePath string
	ch       chan *entry.Entry
}

func NewLoader(filePath string, ch chan *entry.Entry) *Loader {
	ld := new(Loader)
	ld.ch = ch
	ld.filePath = filePath
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

func (ld *Loader) LoadSingleAppendOnlyFile(ctx context.Context, timestamp int64) int {
	ret := OK
	filePath := ld.filePath
	fp, err := os.Open(filePath)
	defer func(fp *os.File) {
		err := fp.Close()
		if err != nil {
			log.Infof("Unrecoverable error reading the append only File %v: %v", filePath, err)
			ret = Failed
		}
	}(fp)
	if err != nil {
		if os.IsNotExist(err) {
			if _, err := os.Stat(filePath); err == nil || !os.IsNotExist(err) {
				log.Infof("Fatal error: can't open the append log File %v for reading: %v", filePath, err.Error())
				return OpenErr
			} else {
				log.Infof("The append log File %v doesn't exist: %v", filePath, err.Error())
				return NotExist
			}

		}
		stat, _ := fp.Stat()
		if stat.Size() == 0 {
			return Empty
		}
	}
	reader := bufio.NewReader(fp)
	for {
		select {
		case <-ctx.Done():
			return ret
		default:
			line, err := ReadCompleteLine(reader)
			if err != nil {
				if err == io.EOF {
					return ret
				} else {
					log.Infof("Unrecoverable error reading the append only File %v: %v", filePath, err)
					ret = Failed
					return ret
				}
			} else {
				_, errs := fp.Seek(0, io.SeekCurrent)
				if errs != nil {
					log.Infof("Unrecoverable error reading the append only File %v: %v", filePath, errs)
					ret = Failed
					return ret
				}
			}

			if line[0] == '#' {
				if timestamp != 0 && strings.HasPrefix(string(line), "#TS:") {
					var ts int64
					ts, err = strconv.ParseInt(strings.TrimPrefix(string(line), "#TS:"), 10, 64)
					if err != nil {
						log.Panicf("Invalid timestamp annotation")
					}

					if ts > timestamp {
						ret = Truncated
						log.Infof("Reached recovery timestamp: %s, subsequent data will no longer be read.", line)
						return ret
					}
				}
				continue
			}
			if line[0] != '*' {
				log.Panicf("Bad File format reading the append only File %v:make a backup of your AOF File, then use ./redis-check-AOF --fix <FileName.manifest>", filePath)
			}
			argc, _ := strconv.ParseInt(string(line[1:]), 10, 64)
			if argc < 1 {
				log.Panicf("Bad File format reading the append only File %v:make a backup of your AOF File, then use ./redis-check-AOF --fix <FileName.manifest>", filePath)
			}
			if argc > int64(SizeMax) {
				log.Panicf("Bad File format reading the append only File %v:make a backup of your AOF File, then use ./redis-check-AOF --fix <FileName.manifest>", filePath)
			}
			e := entry.NewEntry()
			var argv []string

			for j := 0; j < int(argc); j++ {
				line, err := ReadCompleteLine(reader)
				if err != nil || line[0] != '$' {
					log.Infof("Bad File format reading the append only File %v:make a backup of your AOF File, then use ./redis-check-AOF --fix <FileName.manifest>", filePath)
					ret = Failed
					return ret
				}
				v64, _ := strconv.ParseInt(string(line[1:]), 10, 64)
				var argString []byte
				argString, err = ReadCompleteLine(reader)
				if err != nil {
					log.Infof("Unrecoverable error reading the append only File %v: %v", filePath, err)
					ret = Failed
					return ret
				}
				argString = argString[:v64]
				argv = append(argv, string(argString))
			}
			e.Argv = append(e.Argv, argv...)
			ld.ch <- e
		}
	}
}
