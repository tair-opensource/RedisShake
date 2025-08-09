package reader

import (
	"RedisShake/internal/client"
	"RedisShake/internal/entry"
	"RedisShake/internal/log"
	"RedisShake/internal/utils"
	"context"
	"crypto/tls"
	"fmt"
	"github.com/redis/go-redis/v9"
)

type SyncClusterReader struct {
	readers      []Reader
	statusId     int
	OriginClient *redis.ClusterClient
}

func NewSyncClusterReader(ctx context.Context, opts *SyncReaderOptions) Reader {
	addresses, _ := utils.GetRedisClusterNodes(ctx, opts.Address, opts.Username, opts.Password, opts.Tls, opts.TlsConfig, opts.PreferReplica)
	log.Debugf("get redis cluster nodes:")
	for _, address := range addresses {
		log.Debugf("%s", address)
	}
	rd := &SyncClusterReader{}
	for _, address := range addresses {
		theOpts := *opts
		theOpts.Address = address
		rd.readers = append(rd.readers, NewSyncStandaloneReader(ctx, &theOpts))
	}

	var tlsConfig *tls.Config
	if opts.Tls {
		tlsConfig, err := client.CreateTLSConfig(opts.TlsConfig.KeyFilePath, opts.TlsConfig.CACertFilePath, opts.TlsConfig.CertFilePath)
		_ = tlsConfig
		if err != nil {
			log.Panicf("failed to load Tls config.")
		}
	}
	rd.OriginClient = redis.NewClusterClient(&redis.ClusterOptions{
		Addrs:     []string{opts.Address},
		Username:  opts.Username,
		Password:  opts.Password,
		TLSConfig: tlsConfig,
	})

	return rd
}

func (rd *SyncClusterReader) StartRead(ctx context.Context) []chan *entry.Entry {
	chs := make([]chan *entry.Entry, 0)
	for _, r := range rd.readers {
		ch := r.StartRead(ctx)
		chs = append(chs, ch[0])
	}
	return chs
}

func (rd *SyncClusterReader) Status() interface{} {
	stat := make([]interface{}, 0)
	for _, r := range rd.readers {
		stat = append(stat, r.Status())
	}
	return stat
}

func (rd *SyncClusterReader) StatusString() string {
	rd.statusId += 1
	rd.statusId %= len(rd.readers)
	return fmt.Sprintf("src-%d, %s", rd.statusId, rd.readers[rd.statusId].StatusString())
}

func (rd *SyncClusterReader) StatusConsistent() bool {
	for _, r := range rd.readers {
		if !r.StatusConsistent() {
			return false
		}
	}
	return true
}
