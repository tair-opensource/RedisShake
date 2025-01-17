package reader

import (
	"context"
	"fmt"

	"RedisShake/internal/entry"
	"RedisShake/internal/utils"
)

type scanClusterReader struct {
	readers  []Reader
	statusId int
}

func NewScanClusterReader(ctx context.Context, opts *ScanReaderOptions) Reader {
	addresses, _ := utils.GetRedisClusterNodes(ctx, opts.Address, opts.Username, opts.Password, opts.Tls, opts.TlsConfig, opts.PreferReplica)

	rd := &scanClusterReader{}
	for _, address := range addresses {
		theOpts := *opts
		theOpts.Address = address
		rd.readers = append(rd.readers, NewScanStandaloneReader(ctx, &theOpts))
	}
	return rd
}

func (rd *scanClusterReader) StartRead(ctx context.Context) []chan *entry.Entry {
	chs := make([]chan *entry.Entry, 0)
	for _, r := range rd.readers {
		ch := r.StartRead(ctx)
		chs = append(chs, ch[0])
	}
	return chs
}

func (rd *scanClusterReader) Status() interface{} {
	stat := make([]interface{}, 0)
	for _, r := range rd.readers {
		stat = append(stat, r.Status())
	}
	return stat
}

func (rd *scanClusterReader) StatusString() string {
	rd.statusId += 1
	rd.statusId %= len(rd.readers)
	return fmt.Sprintf("src-%d, %s", rd.statusId, rd.readers[rd.statusId].StatusString())
}

func (rd *scanClusterReader) StatusConsistent() bool {
	for _, r := range rd.readers {
		if !r.StatusConsistent() {
			return false
		}
	}
	return true
}
