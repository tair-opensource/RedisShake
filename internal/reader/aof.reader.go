package reader

// this file  references  rdb_reader.go

import (
	"github.com/alibaba/RedisShake/internal/entry"
)

type aofReader struct {
	path string
	ch   chan *entry.Entry
}

// TODO:待完善参考rdb reader
func NewAOFReader(path string) Reader {

	return nil
}

func (r *aofReader) StartRead() chan *entry.Entry {
	//调用 aof中的函数
	//	aof.NewLoader()
	// aof.ParseRDB()
	return nil
}
