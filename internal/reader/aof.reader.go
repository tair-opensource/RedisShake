package reader

// this file  references  rdb_reader.go

import "github.com/alibaba/RedisShake/internal/entry"

type aofReader struct {
	path string
	ch   chan *entry.Entry
}

func NewAOFReader(path string) Reader {
	return nil
}

func (r *aofReader) StartRead() chan *entry.Entry {
	return nil
}
