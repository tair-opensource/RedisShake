package reader

import (
	"context"
	"fmt"

	"RedisShake/internal/entry"
	"RedisShake/internal/log"
	"RedisShake/internal/rdb"
	"RedisShake/internal/utils"

	"github.com/dustin/go-humanize"
)

type RdbReaderOptions struct {
	Filepath string `mapstructure:"filepath" default:""`
}

type rdbReader struct {
	ch chan *entry.Entry

	stat struct {
		Name          string `json:"name"`
		Status        string `json:"status"`
		Filepath      string `json:"filepath"`
		FileSizeBytes int64  `json:"file_size_bytes"`
		FileSizeHuman string `json:"file_size_human"`
		FileSentBytes int64  `json:"file_sent_bytes"`
		FileSentHuman string `json:"file_sent_human"`
		Percent       string `json:"percent"`
	}
}

func NewRDBReader(opts *RdbReaderOptions) Reader {
	absolutePath := utils.GetAbsPath(opts.Filepath)
	r := new(rdbReader)
	r.stat.Name = "rdb_reader"
	r.stat.Status = "init"
	r.stat.Filepath = absolutePath
	r.stat.FileSizeBytes = int64(utils.GetFileSize(absolutePath))
	r.stat.FileSizeHuman = humanize.Bytes(uint64(r.stat.FileSizeBytes))
	return r
}

func (r *rdbReader) StartRead(ctx context.Context) []chan *entry.Entry {
	log.Infof("[%s] start read", r.stat.Name)
	r.ch = make(chan *entry.Entry, 1024)
	updateFunc := func(offset int64) {
		r.stat.FileSentBytes = offset
		r.stat.FileSentHuman = humanize.Bytes(uint64(offset))
		r.stat.Percent = fmt.Sprintf("%.2f%%", float64(offset)/float64(r.stat.FileSizeBytes)*100)
		r.stat.Status = fmt.Sprintf("[%s] rdb file synced: %s", r.stat.Name, r.stat.Percent)
	}
	rdbLoader := rdb.NewLoader(r.stat.Name, updateFunc, r.stat.Filepath, r.ch)

	go func() {
		_ = rdbLoader.ParseRDB(ctx)
		log.Infof("[%s] rdb file parse done", r.stat.Name)
		close(r.ch)
	}()

	return []chan *entry.Entry{r.ch}
}

func (r *rdbReader) Status() interface{} {
	return r.stat
}

func (r *rdbReader) StatusString() string {
	return r.stat.Status
}

func (r *rdbReader) StatusConsistent() bool {
	return r.stat.FileSentBytes == r.stat.FileSizeBytes
}
