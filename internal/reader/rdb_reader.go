package reader

import (
	"github.com/alibaba/RedisShake/internal/entry"
	"github.com/alibaba/RedisShake/internal/log"
	"github.com/alibaba/RedisShake/internal/rdb"
	"path/filepath"
)

type rdbReader struct {
	path string
	ch   chan *entry.Entry
}

func NewRDBReader(path string) Reader {
	log.Infof("NewRDBReader: path=[%s]", path)
	absolutePath, err := filepath.Abs(path)
	if err != nil {
		log.Panicf("NewRDBReader: filepath.Abs error: %s", err.Error())
	}
	log.Infof("NewRDBReader: absolute path=[%s]", absolutePath)
	r := new(rdbReader)
	r.path = absolutePath
	return r
}

func (r *rdbReader) StartRead() chan *entry.Entry {
	r.ch = make(chan *entry.Entry, 2048)

	go func() {
		// start parse rdb
		log.Infof("start send RDB. path=[%s]", r.path)
		rdbLoader := rdb.NewLoader(r.path, r.ch)
		_ = rdbLoader.ParseRDB()
		log.Infof("send RDB finished. path=[%s]", r.path)
		close(r.ch)
	}()

	return r.ch
}

func (r *rdbReader) StartReadAOF() chan *entry.Entry {
	r.ch = make(chan *entry.Entry, 1)
	go func() {
		log.Infof("No AOF read for RDB Reader.")
		close(r.ch)
	}()

	return r.ch
}
