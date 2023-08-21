package rotate

import (
	"RedisShake/internal/log"
	"RedisShake/internal/utils"
	"fmt"
	"io"
	"os"
	"time"
)

type AOFReader struct {
	name     string
	dir      string
	file     *os.File
	offset   int64
	pos      int64
	filepath string
}

func NewAOFReader(name string, dir string, offset int64) *AOFReader {
	r := new(AOFReader)
	r.name = name
	r.dir = dir
	r.openFile(offset)
	return r
}

func (r *AOFReader) openFile(offset int64) {
	r.filepath = fmt.Sprintf("%s/%d.aof", r.dir, r.offset)
	var err error
	r.file, err = os.OpenFile(r.filepath, os.O_RDONLY, 0644)
	if err != nil {
		log.Panicf(err.Error())
	}
	r.offset = offset
	r.pos = 0
	log.Debugf("[%s] open file for read. filename=[%s]", r.name, r.filepath)
}

func (r *AOFReader) readNextFile(offset int64) {
	filepath := fmt.Sprintf("%s/%d.aof", r.dir, r.offset)
	if utils.IsExist(filepath) {
		r.Close()
		err := os.Remove(r.filepath)
		if err != nil {
			return
		}
		r.openFile(offset)
	}
}

func (r *AOFReader) Read(buf []byte) (n int, err error) {
	n, err = r.file.Read(buf)
	for err == io.EOF {
		if r.filepath != fmt.Sprintf("%s/%d.aof", r.dir, r.offset) {
			r.readNextFile(r.offset)
		}
		time.Sleep(time.Millisecond * 10)
		_, err = r.file.Seek(0, 1)
		if err != nil {
			log.Panicf(err.Error())
		}
		n, err = r.file.Read(buf)
	}
	if err != nil {
		log.Panicf(err.Error())
	}
	r.offset += int64(n)
	r.pos += int64(n)
	return n, nil
}

func (r *AOFReader) Offset() int64 {
	return r.offset
}

func (r *AOFReader) Close() {
	if r.file == nil {
		return
	}
	err := r.file.Close()
	if err != nil {
		log.Panicf(err.Error())
	}
	r.file = nil
	log.Infof("[%s] close file. filename=[%s]", r.name, r.filepath)
}
