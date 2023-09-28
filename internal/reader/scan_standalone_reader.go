package reader

import (
	"RedisShake/internal/client"
	"RedisShake/internal/client/proto"
	"RedisShake/internal/config"
	"RedisShake/internal/entry"
	"RedisShake/internal/log"
	"RedisShake/internal/rdb/types"
	"RedisShake/internal/utils"
	"fmt"
	"math/bits"
	"regexp"
	"strconv"
	"strings"
)

type ScanReaderOptions struct {
	Cluster  bool   `mapstructure:"cluster" default:"false"`
	Address  string `mapstructure:"address" default:""`
	Username string `mapstructure:"username" default:""`
	Password string `mapstructure:"password" default:""`
	Tls      bool   `mapstructure:"tls" default:"false"`
	KSN      bool   `mapstructure:"ksn" default:"false"`
}

type dbKey struct {
	db  int
	key string
}

type scanStandaloneReader struct {
	dbs      []int
	opts     *ScanReaderOptions
	ch       chan *entry.Entry
	keyQueue *utils.UniqueQueue

	stat struct {
		Name              string `json:"name"`
		ScanFinished      bool   `json:"scan_finished"`
		ScanDbId          int    `json:"scan_dbId"`
		ScanCursor        uint64 `json:"scan_cursor"`
		ScanPercentByDbId string `json:"scan_percent"`
		NeedUpdateCount   int64  `json:"need_update_count"`
	}
}

func NewScanStandaloneReader(opts *ScanReaderOptions) Reader {
	r := new(scanStandaloneReader)
	// dbs
	c := client.NewRedisClient(opts.Address, opts.Username, opts.Password, opts.Tls)
	if c.IsCluster() { // not use opts.Cluster, because user may use standalone mode to scan a cluster node
		r.dbs = []int{0}
	} else {
		r.dbs = []int{0, 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15}
	}
	r.opts = opts
	r.ch = make(chan *entry.Entry, 1024)
	r.stat.Name = "reader_" + strings.Replace(opts.Address, ":", "_", -1)
	r.keyQueue = utils.NewUniqueQueue(100000) // cache 100000 keys
	return r
}

func (r *scanStandaloneReader) StartRead() chan *entry.Entry {
	r.subscript()
	go r.scan()
	go r.fetch()
	return r.ch
}

func (r *scanStandaloneReader) subscript() {
	if !r.opts.KSN {
		return
	}
	c := client.NewRedisClient(r.opts.Address, r.opts.Username, r.opts.Password, r.opts.Tls)
	c.Send("psubscribe", "__keyevent@*__:*")

	go func() {
		_, err := c.Receive()
		if err != nil {
			log.Panicf(err.Error())
		}
		regex := regexp.MustCompile(`\d+`)
		for {
			resp, err := c.Receive()
			if err != nil {
				log.Panicf(err.Error())
			}
			key := resp.([]interface{})[3].(string)
			dbId := regex.FindString(resp.([]interface{})[2].(string))
			dbIdInt, err := strconv.Atoi(dbId)
			if err != nil {
				log.Panicf(err.Error())
			}
			r.keyQueue.Put(dbKey{db: dbIdInt, key: key})
		}
	}()
}

func (r *scanStandaloneReader) scan() {
	c := client.NewRedisClient(r.opts.Address, r.opts.Username, r.opts.Password, r.opts.Tls)
	for dbId := range r.dbs {
		if dbId != 0 {
			reply := c.DoWithStringReply("SELECT", strconv.Itoa(dbId))
			if reply != "OK" {
				log.Panicf("scanStandaloneReader select db failed. db=[%d]", dbId)
			}
		}

		var cursor uint64 = 0
		for {
			var keys []string
			cursor, keys = c.Scan(cursor)
			for _, key := range keys {
				r.keyQueue.Put(dbKey{dbId, key}) // pass value not pointer
			}

			// stat
			r.stat.ScanCursor = cursor
			r.stat.ScanDbId = dbId
			r.stat.ScanPercentByDbId = fmt.Sprintf("%.2f%%", float64(bits.Reverse64(cursor))/float64(^uint(0))*100)

			if cursor == 0 {
				break
			}
		}
	}
	r.stat.ScanFinished = true
	if !r.opts.KSN {
		r.keyQueue.Close()
	}
}

func (r *scanStandaloneReader) fetch() {
	nowDbId := 0
	c := client.NewRedisClient(r.opts.Address, r.opts.Username, r.opts.Password, r.opts.Tls)
	for item := range r.keyQueue.Ch {
		r.stat.NeedUpdateCount = int64(r.keyQueue.Len())
		dbId := item.(dbKey).db
		key := item.(dbKey).key
		if nowDbId != dbId {
			reply := c.DoWithStringReply("SELECT", strconv.Itoa(dbId))
			if reply != "OK" {
				log.Panicf("scanStandaloneReader select db failed. db=[%d]", dbId)
			}
			nowDbId = dbId
		}
		// dump
		c.Send("DUMP", key)
		c.Send("PTTL", key)
		iDump, err1 := c.Receive()
		iPttl, err2 := c.Receive()
		if err1 == proto.Nil {
			continue // key not exist
		} else if err1 != nil {
			log.Panicf(err1.Error())
		} else if err2 != nil {
			log.Panicf(err2.Error())
		}
		dump := iDump.(string)
		pttl := int(iPttl.(int64))
		if pttl == -2 {
			continue // key not exist
		}
		if pttl == -1 {
			pttl = 0 // -1 means no expire
		}
		if uint64(len(dump)) > config.Opt.Advanced.TargetRedisProtoMaxBulkLen {
			log.Warnf("key=[%s] dump len=[%d] too large, split it. This is not a good practice in Redis.", key, len(dump))
			typeByte := dump[0]
			anotherReader := strings.NewReader(dump[1 : len(dump)-10])
			o := types.ParseObject(anotherReader, typeByte, key)
			cmds := o.Rewrite()
			for _, cmd := range cmds {
				e := entry.NewEntry()
				e.DbId = dbId
				e.Argv = cmd
				r.ch <- e
			}
			if pttl != 0 {
				e := entry.NewEntry()
				e.DbId = dbId
				e.Argv = []string{"PEXPIRE", key, strconv.Itoa(pttl)}
				r.ch <- e
			}
		} else {
			argv := []string{"RESTORE", key, strconv.Itoa(pttl), dump}
			if config.Opt.Advanced.RDBRestoreCommandBehavior == "rewrite" {
				argv = append(argv, "replace")
			}
			r.ch <- &entry.Entry{
				DbId: dbId,
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
	if r.stat.ScanFinished {
		return fmt.Sprintf("need_update_count=[%d]", r.stat.NeedUpdateCount)
	}
	return fmt.Sprintf("scan_dbid=[%d], scan_percent=[%s], need_update_count=[%d]", r.stat.ScanDbId, r.stat.ScanPercentByDbId, r.stat.NeedUpdateCount)
}

func (r *scanStandaloneReader) StatusConsistent() bool {
	return r.stat.ScanFinished && r.stat.NeedUpdateCount == 0
}
