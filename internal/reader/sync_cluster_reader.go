package reader

import (
	"RedisShake/internal/entry"
	"RedisShake/internal/log"
	"RedisShake/internal/utils"
	"sync"
)

type SyncClusterReaderOptions struct {
	Address  string `mapstructure:"address" default:""`
	Username string `mapstructure:"username" default:""`
	Password string `mapstructure:"password" default:""`
	Tls      bool   `mapstructure:"tls" default:"false"`
}

type syncClusterReader struct {
	readers []Reader
}

func NewSyncClusterReader(opts *SyncClusterReaderOptions) Reader {
	addresses, _ := utils.GetRedisClusterNodes(opts.Address, opts.Username, opts.Password, opts.Tls)
	log.Debugf("get redis cluster nodes:")
	for _, address := range addresses {
		log.Debugf("%s", address)
	}
	rd := &syncClusterReader{}
	for _, address := range addresses {
		rd.readers = append(rd.readers, NewSyncStandaloneReader(&SyncStandaloneReaderOptions{
			Address:  address,
			Username: opts.Username,
			Password: opts.Password,
			Tls:      opts.Tls,
		}))
	}
	return rd
}

func (rd *syncClusterReader) StartRead() chan *entry.Entry {
	ch := make(chan *entry.Entry, 1024)
	var wg sync.WaitGroup
	for _, r := range rd.readers {
		wg.Add(1)
		go func(r Reader) {
			defer wg.Done()
			for e := range r.StartRead() {
				ch <- e
			}
		}(r)
	}
	go func() {
		wg.Wait()
		close(ch)
	}()
	return ch
}

func (rd *syncClusterReader) Status() interface{} {
	stat := make([]interface{}, 0)
	for _, r := range rd.readers {
		stat = append(stat, r.Status())
	}
	return stat
}

func (rd *syncClusterReader) StatusString() string {
	return "syncClusterReader"
}

func (rd *syncClusterReader) StatusConsistent() bool {
	for _, r := range rd.readers {
		if !r.StatusConsistent() {
			return false
		}
	}
	return true
}
