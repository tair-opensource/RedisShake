package reader

import (
	"RedisShake/internal/entry"
	"RedisShake/internal/utils"
	"fmt"
	"sync"
)

type scanClusterReader struct {
	readers  []Reader
	statusId int
}

func NewScanClusterReader(opts *ScanReaderOptions) Reader {
	addresses, _ := utils.GetRedisClusterNodes(opts.Address, opts.Username, opts.Password, opts.Tls)

	rd := &scanClusterReader{}
	for _, address := range addresses {
		theOpts := *opts
		theOpts.Address = address
		rd.readers = append(rd.readers, NewScanStandaloneReader(&theOpts))
	}
	return rd
}

func (rd *scanClusterReader) StartRead() chan *entry.Entry {
	ch := make(chan *entry.Entry, 1024)
	var wg sync.WaitGroup
	for _, r := range rd.readers {
		wg.Add(1)
		go func(r Reader) {
			for e := range r.StartRead() {
				ch <- e
			}
			wg.Done()
		}(r)
	}
	go func() {
		wg.Wait()
		close(ch)
	}()
	return ch
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
