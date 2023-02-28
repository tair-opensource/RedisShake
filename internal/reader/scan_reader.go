package reader

import (
	"strconv"
	"strings"

	"github.com/alibaba/RedisShake/internal/client"
	"github.com/alibaba/RedisShake/internal/client/proto"
	"github.com/alibaba/RedisShake/internal/entry"
	"github.com/alibaba/RedisShake/internal/log"
	"github.com/alibaba/RedisShake/internal/statistics"
)

const (
	// cluster_enabled: Indicate Redis cluster is enabled. reference from https://redis.io/commands/info/
	clusterMode = "cluster_enabled:1"
)

type dbKey struct {
	db       int
	key      string
	isSelect bool
}

type scanReader struct {
	address string

	// client for scan keys
	clientScan   *client.Redis
	innerChannel chan *dbKey
	isCluster    bool

	// client for dump keys
	clientDump     *client.Redis
	clientDumpDbid int
	ch             chan *entry.Entry
}

func NewScanReader(address string, username string, password string, isTls bool) Reader {
	r := new(scanReader)
	r.address = address
	r.clientScan = client.NewRedisClient(address, username, password, isTls)
	r.clientDump = client.NewRedisClient(address, username, password, isTls)
	log.Infof("scanReader connected to redis successful. address=[%s]", address)

	r.isCluster = r.IsCluster()
	return r
}

// IsCluster is for determining whether the server is in cluster mode.
func (r *scanReader) IsCluster() bool {
	reply := r.clientScan.DoWithStringReply("INFO", "Cluster")
	return strings.Contains(reply, clusterMode)
}

func (r *scanReader) StartRead() chan *entry.Entry {
	r.ch = make(chan *entry.Entry, 1024)
	r.innerChannel = make(chan *dbKey, 1024)
	go r.scan()
	go r.fetch()
	return r.ch
}

func (r *scanReader) scan() {
	scanDbIdUpper := 15
	if r.isCluster {
		log.Infof("scanReader node are in cluster mode, only scan db 0")
		scanDbIdUpper = 0
	}
	for dbId := 0; dbId <= scanDbIdUpper; dbId++ {
		if !r.isCluster {
			reply := r.clientScan.DoWithStringReply("SELECT", strconv.Itoa(dbId))
			if reply != "OK" {
				log.Panicf("scanReader select db failed. db=[%d]", dbId)
			}

			r.clientDump.Send("SELECT", strconv.Itoa(dbId))
			r.innerChannel <- &dbKey{dbId, "", true}
		}

		var cursor uint64 = 0
		for {
			var keys []string
			cursor, keys = r.clientScan.Scan(cursor)
			for _, key := range keys {
				r.clientDump.Send("DUMP", key)
				r.clientDump.Send("PTTL", key)
				r.innerChannel <- &dbKey{dbId, key, false}
			}

			// stat
			statistics.Metrics.ScanDbId = dbId
			statistics.Metrics.ScanCursor = cursor

			if cursor == 0 {
				break
			}
		}
	}
	close(r.innerChannel)
}

func (r *scanReader) fetch() {
	var id uint64 = 0
	for item := range r.innerChannel {
		if item.isSelect {
			// select
			receive, err := client.String(r.clientDump.Receive())
			if err != nil {
				log.Panicf("scanReader select db failed. db=[%d], err=[%v]", item.db, err)
			}
			if receive != "OK" {
				log.Panicf("scanReader select db failed. db=[%d]", item.db)
			}
		} else {
			// dump
			receive, err := client.String(r.clientDump.Receive())
			if err != proto.Nil && err != nil { // error!
				log.PanicIfError(err)
			}

			// pttl
			pttl, pttlErr := client.Int64(r.clientDump.Receive())
			log.PanicIfError(pttlErr)
			if pttl < 0 {
				pttl = 0
			}

			if err == proto.Nil { // key not exist
				continue
			}

			id += 1
			argv := []string{"RESTORE", item.key, strconv.FormatInt(pttl, 10), receive}
			r.ch <- &entry.Entry{
				Id:     id,
				IsBase: false,
				DbId:   item.db,
				Argv:   argv,
			}
		}
	}
	log.Infof("scanReader fetch finished. address=[%s]", r.address)
	close(r.ch)
}
