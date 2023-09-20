package aof

import (
	"bufio"
	"path"
	"strings"

	"io"
	"os"
	"strconv"

	"RedisShake/internal/entry"
	"RedisShake/internal/log"
)

const (
	COK          = 1
	CERR         = -1
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

func MakePath(Paths string, FileName string) string {
	return path.Join(Paths, FileName)
}

func LoadSingleAppendOnlyFile(AOFDirName string, FileName string, ch chan *entry.Entry, LastFile bool, AOFTimeStamp int64) int {
	ret := AOFOK
	AOFFilepath := MakePath(AOFDirName, FileName)
	fp, err := os.Open(AOFFilepath)
	if err != nil {
		if os.IsNotExist(err) {
			if _, err := os.Stat(AOFFilepath); err == nil || !os.IsNotExist(err) {
				log.Infof("Fatal error: can't open the append log File %v for reading: %v", FileName, err.Error())
				return AOFOpenErr
			} else {
				log.Infof("The append log File %v doesn't exist: %v", FileName, err.Error())
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

		line, err := reader.ReadBytes('\n')
		{
			if err != nil {
				if err == io.EOF {
					break
				}
			} else {
				_, errs := fp.Seek(0, io.SeekCurrent)
				if errs != nil {
					log.Infof("Unrecoverable error reading the append only File %v: %v", FileName, err)
					ret = AOFFailed
					return ret
				}
			}

			if line[0] == '#' {
				if AOFTimeStamp != 0 && strings.HasPrefix(string(line), "#TS:") {
					var ts int64
					ts, err = strconv.ParseInt(strings.TrimPrefix(string(line[:len(line)-2]), "#TS:"), 10, 64)
					if err != nil {
						log.Panicf("Invalid timestamp annotation")
					}

					if ts > AOFTimeStamp && LastFile {
						ret = AOFTruncated
						return ret
					}
				}
				continue
			}
			if line[0] != '*' {
				log.Infof("Bad File format reading the append only File %v:make a backup of your AOF File, then use ./redis-check-AOF --fix <FileName.manifest>", FileName)
			}
			argc, _ := strconv.ParseInt(string(line[1:len(line)-2]), 10, 64)
			if argc < 1 {
				log.Infof("Bad File format reading the append only File %v:make a backup of your AOF File, then use ./redis-check-AOF --fix <FileName.manifest>", FileName)
			}
			if argc > int64(SizeMax) {
				log.Infof("Bad File format reading the append only File %v:make a backup of your AOF File, then use ./redis-check-AOF --fix <FileName.manifest>", FileName)
			}
			e := entry.NewEntry()
			argv := []string{}

			for j := 0; j < int(argc); j++ {
				line, err := reader.ReadString('\n')
				if err != nil || line[0] != '$' {
					if err == io.EOF {
						log.Infof("Unrecoverable error reading the append only File %v: %v", FileName, err)
						ret = AOFFailed
						return ret
					} else {
						log.Infof("Bad File format reading the append only File %v:make a backup of your AOF File, then use ./redis-check-AOF --fix <FileName.manifest>", FileName)
					}
				}
				len, _ := strconv.ParseInt(string(line[1:len(line)-2]), 10, 64)
				argstring := make([]byte, len+2)
				argstring, err = reader.ReadBytes('\n')
				if err != nil || argstring[len+1] != '\n' {
					log.Infof("Unrecoverable error reading the append only File %v: %v", FileName, err)
					ret = AOFFailed
					return ret
				}
				argstring = argstring[:len]
				argv = append(argv, string(argstring))
			}
			for _, value := range argv {
				e.Argv = append(e.Argv, value)
			}
			ch <- e

		}

	}
	return ret
}
