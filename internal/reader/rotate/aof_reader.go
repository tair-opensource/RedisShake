package rotate

import (
	"fmt"
	"github.com/alibaba/RedisShake/internal/log"
	"github.com/alibaba/RedisShake/internal/utils"
	"io"
	"os"
	"time"
)

type AOFReader struct {
	file     *os.File
	offset   int64
	pos      int64
	filename string
}

func NewAOFReader(offset int64) *AOFReader {
	r := new(AOFReader)
	r.openFile(offset)
	return r
}

func (r *AOFReader) openFile(offset int64) {
	r.filename = fmt.Sprintf("%d.aof", offset)
	var err error
	r.file, err = os.OpenFile(r.filename, os.O_RDONLY, 0644)
	if err != nil {
		log.PanicError(err)
	}
	r.offset = offset
	r.pos = 0
	log.Infof("AOFReader open file. aof_filename=[%s]", r.filename)
}

func (r *AOFReader) readNextFile(offset int64) {
	filename := fmt.Sprintf("%d.aof", offset)
	if utils.DoesFileExist(filename) {
		r.Close()
		err := os.Remove(r.filename)
		if err != nil {
			return
		}
		r.openFile(offset)
	}
}

func (r *AOFReader) Read(buf []byte) (n int, err error) {
	n, err = r.file.Read(buf)
	for err == io.EOF {
		if r.filename != fmt.Sprintf("%d.aof", r.offset) {
			r.readNextFile(r.offset)
		}
		time.Sleep(time.Millisecond * 10)
		_, err = r.file.Seek(0, 1)
		if err != nil {
			log.PanicError(err)
		}
		n, err = r.file.Read(buf)
	}
	if err != nil {
		log.PanicError(err)
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
		log.PanicError(err)
	}
	r.file = nil
	log.Infof("AOFReader close file. aof_filename=[%s]", r.filename)
}
