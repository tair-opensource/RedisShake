package reader

import (
	"RedisShake/internal/client"
	"RedisShake/internal/client/proto"
	"RedisShake/internal/config"
	"RedisShake/internal/entry"
	"RedisShake/internal/log"
	"fmt"
	"math/bits"
	"strconv"
	"strings"
)

type dbKey struct {
	db       int
	key      string
	isSelect bool
}

type scanStandaloneReader struct {
	isCluster bool
	ch        chan *entry.Entry

	// client for scan keys
	clientScan *client.Redis

	// client for dump keys
	keysNeedFetch  chan *dbKey
	clientDump     *client.Redis
	clientDumpDbid int

	stat struct {
		Name          string `json:"name"`
		Finished      bool   `json:"finished"`
		DbId          int    `json:"dbId"`
		Cursor        uint64 `json:"cursor"`
		PercentByDbId string `json:"percent"`
	}
}

type ScanReaderOptions struct {
	Cluster  bool   `mapstructure:"cluster" default:"false"`
	Address  string `mapstructure:"address" default:""`
	Username string `mapstructure:"username" default:""`
	Password string `mapstructure:"password" default:""`
	Tls      bool   `mapstructure:"tls" default:"false"`
	KSN      bool   `mapstructure:"ksn" default:"false"`
}

func NewScanStandaloneReader(opts *ScanReaderOptions) Reader {
	r := new(scanStandaloneReader)
	r.stat.Name = "reader_" + strings.Replace(opts.Address, ":", "_", -1)
	r.clientScan = client.NewRedisClient(opts.Address, opts.Username, opts.Password, opts.Tls)
	r.clientDump = client.NewRedisClient(opts.Address, opts.Username, opts.Password, opts.Tls)
	r.isCluster = r.clientScan.IsCluster() // not use opts.Cluster, because user may use standalone mode to scan a cluster node
	return r
}

func (r *scanStandaloneReader) StartRead() chan *entry.Entry {
	r.ch = make(chan *entry.Entry, 1024)
	r.keysNeedFetch = make(chan *dbKey, 1024)
	go r.scan()
	go r.fetch()
	return r.ch
}

func (r *scanStandaloneReader) scan() {
	scanDbIdUpper := 15
	if r.isCluster {
		log.Infof("scanStandaloneReader node are in cluster mode, only scan db 0")
		scanDbIdUpper = 0
	}
	for dbId := 0; dbId <= scanDbIdUpper; dbId++ {
		if !r.isCluster {
			reply := r.clientScan.DoWithStringReply("SELECT", strconv.Itoa(dbId))
			if reply != "OK" {
				log.Panicf("scanStandaloneReader select db failed. db=[%d]", dbId)
			}

			r.clientDump.Send("SELECT", strconv.Itoa(dbId))
			r.keysNeedFetch <- &dbKey{dbId, "", true}
		}

		var cursor uint64 = 0
		for {
			var keys []string
			cursor, keys = r.clientScan.Scan(cursor)
			for _, key := range keys {
				r.clientDump.Send("DUMP", key)
				r.clientDump.Send("PTTL", key)
				r.keysNeedFetch <- &dbKey{dbId, key, false}
			}

			// stat
			r.stat.Cursor = cursor
			r.stat.DbId = dbId
			r.stat.PercentByDbId = fmt.Sprintf("%.2f%%", float64(bits.Reverse64(cursor))/float64(^uint(0))*100)

			if cursor == 0 {
				break
			}
		}
	}
	r.stat.Finished = true
	close(r.keysNeedFetch)
}

func (r *scanStandaloneReader) fetch() {
	var id uint64 = 0
	for item := range r.keysNeedFetch {
		if item.isSelect {
			// select
			receive, err := client.String(r.clientDump.Receive())
			if err != nil {
				log.Panicf("scanStandaloneReader select db failed. db=[%d], err=[%v]", item.db, err)
			}
			if receive != "OK" {
				log.Panicf("scanStandaloneReader select db failed. db=[%d]", item.db)
			}
		} else {
			// dump
			receive, err := client.String(r.clientDump.Receive())
			if err != proto.Nil && err != nil { // error!
				log.Panicf(err.Error())
			}

			// pttl
			pttl, pttlErr := client.Int64(r.clientDump.Receive())
			if pttlErr != nil { // error!
				log.Panicf(pttlErr.Error())
			}
			if pttl < 0 {
				pttl = 0
			}

			if err == proto.Nil { // key not exist
				continue
			}

			id += 1
			argv := []string{"RESTORE", item.key, strconv.FormatInt(pttl, 10), receive}
			if config.Opt.Advanced.RDBRestoreCommandBehavior == "rewrite" {
				argv = append(argv, "replace")
			}
			log.Debugf("[%s] send command: [%v], dbid: [%v]", r.stat.Name, argv, item.db)
			r.ch <- &entry.Entry{
				DbId: item.db,
				Argv: argv,
			}
		}
	}
	log.Infof("[%s] scanStandaloneReader fetch finished.", r.stat.Name)
	close(r.ch)
}

func (r *scanStandaloneReader) Status() interface{} {
	return r.stat
}

func (r *scanStandaloneReader) StatusString() string {
	if r.stat.Finished {
		return fmt.Sprintf("finished")
	}
	return fmt.Sprintf("dbid=[%d], percent=[%s]", r.stat.DbId, r.stat.PercentByDbId)
}

func (r *scanStandaloneReader) StatusConsistent() bool {
	return r.stat.Finished
}
