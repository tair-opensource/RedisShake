package reader

import (
	"RedisShake/internal/entry"
	"RedisShake/internal/utils"
	"sync"
)

type ScanClusterReaderOptions struct {
	Address  string `mapstructure:"address" default:""`
	Username string `mapstructure:"username" default:""`
	Password string `mapstructure:"password" default:""`
	Tls      bool   `mapstructure:"tls" default:"false"`
}

type scanClusterReader struct {
	readers []Reader
}

func NewScanClusterReader(opts *ScanClusterReaderOptions) Reader {
	addresses, _ := utils.GetRedisClusterNodes(opts.Address, opts.Username, opts.Password, opts.Tls)

	rd := &scanClusterReader{}
	for _, address := range addresses {
		rd.readers = append(rd.readers, NewScanStandaloneReader(&ScanStandaloneReaderOptions{
			Address:  address,
			Username: opts.Username,
			Password: opts.Password,
			Tls:      opts.Tls,
		}))
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
	return "scanClusterReader"
}

func (rd *scanClusterReader) StatusConsistent() bool {
	for _, r := range rd.readers {
		if !r.StatusConsistent() {
			return false
		}
	}
	return true
}
