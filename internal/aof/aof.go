package aof

import (
	"bufio"
	"bytes"
	"context"
	"io"
	"os"
	"path"
	"strconv"
	"strings"

	"RedisShake/internal/entry"
	"RedisShake/internal/log"
	"RedisShake/internal/rdb"
)

const (
	NotExist  = 1
	OpenErr   = 3
	OK        = 0
	Empty     = 2
	Failed    = 4
	Truncated = 5
)

type Loader struct {
	filePath string
	ch       chan *entry.Entry

	name           string
	useRDBPreamble int
}

func NewLoader(name string, useRDBPreamble int, filePath string, ch chan *entry.Entry) *Loader {
	ld := new(Loader)
	ld.ch = ch
	ld.filePath = filePath
	ld.name = name
	ld.useRDBPreamble = useRDBPreamble
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
	isRDB := false
	if ld.useRDBPreamble == 1 {
		sig := make([]byte, 6)
		n, err := fp.Read(sig)
		if err != nil && err != io.EOF {
			log.Infof("Reading signature the append only File %v: %v", path.Base(filePath), err)
			return Failed
		}
		isRDB = (err == nil) && (n >= 5 && bytes.Equal(sig[:5], []byte("REDIS"))) || (n >= 6 && bytes.Equal(sig[:6], []byte("VALKEY")))

		if _, err := fp.Seek(0, io.SeekStart); err != nil {
			log.Infof("Unrecoverable error reading the append only File %v: %v", path.Base(filePath), err)
			return Failed
		}
	}

	reader := bufio.NewReader(fp)
	if isRDB { //Skipped RDB checksum and has not been processed yet.
		log.Infof("Reading RDB Base File on AOF loading...")
		rdbLoader := rdb.NewLoader(ld.name, nil, filePath, ld.ch)
		_ = rdbLoader.ParseRDBStream(ctx, reader)
		log.Infof("[%s] RDB preamble parse done, switching to AOF stream...", ld.name)
	}
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
			e := entry.NewEntry()
			var argv []string

			for j := 0; j < int(argc); j++ {
				line, err := ReadCompleteLine(reader)
				if err != nil || len(line) == 0 || line[0] != '$' {
					log.Infof("Bad File format reading the append only File %v:make a backup of your AOF File, then use ./redis-check-AOF --fix <FileName.manifest>", filePath)
					ret = Failed
					return ret
				}
				v64, _ := strconv.ParseInt(string(line[1:]), 10, 64)
				// Read exactly v64 bytes plus the trailing CRLF. The argument
				// payload may contain '\r' or '\n' (e.g. binary RDB dumps in
				// RESTORE commands), so a line-based read would truncate it.
				buf := make([]byte, v64+2)
				if _, err := io.ReadFull(reader, buf); err != nil {
					log.Infof("Unrecoverable error reading the append only File %v: %v", filePath, err)
					ret = Failed
					return ret
				}
				argv = append(argv, string(buf[:v64]))
			}
			e.Argv = append(e.Argv, argv...)
			ld.ch <- e
		}
	}
}
