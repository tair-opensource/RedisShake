package rotate

import (
	"fmt"
	"os"

	"RedisShake/internal/log"
)

const MaxFileSize = 1024 * 1024 * 1024 // 1G

type AOFWriter struct {
	name string
	dir  string

	file     *os.File
	offset   int64
	filepath string
	filesize int64
}

func NewAOFWriter(name string, dir string, offset int64) *AOFWriter {
	w := new(AOFWriter)
	w.name = name
	w.dir = dir
	w.openFile(offset)
	return w
}

func (w *AOFWriter) openFile(offset int64) {
	w.filepath = fmt.Sprintf("%s/%d.aof", w.dir, w.offset)
	var err error
	w.file, err = os.OpenFile(w.filepath, os.O_WRONLY|os.O_CREATE|os.O_APPEND, 0644)
	if err != nil {
		log.Panicf(err.Error())
	}
	w.offset = offset
	w.filesize = 0
	log.Debugf("[%s] open file for write. filename=[%s]", w.name, w.filepath)
}

func (w *AOFWriter) Write(buf []byte) {
	_, err := w.file.Write(buf)
	if err != nil {
		log.Panicf(err.Error())
	}
	w.offset += int64(len(buf))
	w.filesize += int64(len(buf))
	if w.filesize > MaxFileSize {
		w.Close()
		w.openFile(w.offset)
	}
}

func (w *AOFWriter) Close() {
	if w.file == nil {
		return
	}
	err := w.file.Sync()
	if err != nil {
		log.Panicf(err.Error())
	}
	err = w.file.Close()
	if err != nil {
		log.Panicf(err.Error())
	}
	log.Infof("[%s] close file. filename=[%s], filesize=[%d]", w.name, w.filepath, w.filesize)
}
