package reader

import (
	"RedisShake/internal/entry"
	"RedisShake/internal/log"
	"RedisShake/internal/utils"
	"fmt"
)

type syncClusterReader struct {
	readers  []Reader
	statusId int
}

func NewSyncClusterReader(opts *SyncReaderOptions) Reader {
	addresses, _ := utils.GetRedisClusterNodes(opts.Address, opts.Username, opts.Password, opts.Tls)
	log.Debugf("get redis cluster nodes:")
	for _, address := range addresses {
		log.Debugf("%s", address)
	}
	rd := &syncClusterReader{}
	for _, address := range addresses {
		theOpts := *opts
		theOpts.Address = address
		rd.readers = append(rd.readers, NewSyncStandaloneReader(&theOpts))
	}
	return rd
}

func (rd *syncClusterReader) StartRead() []chan *entry.Entry {
	channels := make([]chan *entry.Entry, 0)
	for _, r := range rd.readers {
		channels = append(channels, r.StartRead()...)
	}
	return channels
}

func (rd *syncClusterReader) Status() interface{} {
	stat := make([]interface{}, 0)
	for _, r := range rd.readers {
		stat = append(stat, r.Status())
	}
	return stat
}

func (rd *syncClusterReader) StatusString() string {
	rd.statusId += 1
	rd.statusId %= len(rd.readers)
	return fmt.Sprintf("src-%d, %s", rd.statusId, rd.readers[rd.statusId].StatusString())
}

func (rd *syncClusterReader) StatusConsistent() bool {
	for _, r := range rd.readers {
		if !r.StatusConsistent() {
			return false
		}
	}
	return true
}
