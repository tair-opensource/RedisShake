package reader

import (
	"context"
	"fmt"

	"RedisShake/internal/entry"
	"RedisShake/internal/log"
	"RedisShake/internal/utils"
)

type syncClusterReader struct {
	readers  []Reader
	statusId int
}

func NewSyncClusterReader(ctx context.Context, opts *SyncReaderOptions) Reader {
	addresses, _ := utils.GetRedisClusterNodes(ctx, opts.Address, opts.Username, opts.Password, opts.Tls, opts.TlsConfig, opts.PreferReplica)
	log.Debugf("get redis cluster nodes:")
	for _, address := range addresses {
		log.Debugf("%s", address)
	}
	rd := &syncClusterReader{}
	for _, address := range addresses {
		theOpts := *opts
		theOpts.Address = address
		rd.readers = append(rd.readers, NewSyncStandaloneReader(ctx, &theOpts))
	}
	return rd
}

func (rd *syncClusterReader) StartRead(ctx context.Context) []chan *entry.Entry {
	chs := make([]chan *entry.Entry, 0)
	for _, r := range rd.readers {
		ch := r.StartRead(ctx)
		chs = append(chs, ch[0])
	}
	return chs
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
