package reader

import (
	"context"
	"errors"
	"fmt"
	"math/bits"
	"regexp"
	"strconv"
	"strings"

	"RedisShake/internal/client"
	"RedisShake/internal/client/proto"
	"RedisShake/internal/config"
	"RedisShake/internal/entry"
	"RedisShake/internal/log"
	"RedisShake/internal/rdb/types"
	"RedisShake/internal/utils"
)

type ScanReaderOptions struct {
	Cluster       bool             `mapstructure:"cluster" default:"false"`
	Address       string           `mapstructure:"address" default:""`
	Username      string           `mapstructure:"username" default:""`
	Password      string           `mapstructure:"password" default:""`
	Tls           bool             `mapstructure:"tls" default:"false"`
	TlsConfig     client.TlsConfig `mapstructure:"tls_config" default:"{}"`
	Scan          bool             `mapstructure:"scan" default:"true"`
	KSN           bool             `mapstructure:"ksn" default:"false"`
	DBS           []int            `mapstructure:"dbs"`
	PreferReplica bool             `mapstructure:"prefer_replica" default:"false"`
	Count         int              `mapstructure:"count" default:"1"`
}

type dbKey struct {
	db  int
	key string
}

type needRestoreItem struct {
	dbId int
	key  string
}

type scanStandaloneReader struct {
	ctx             context.Context
	dbs             []int
	opts            *ScanReaderOptions
	ch              chan *entry.Entry
	needDumpQueue   *utils.UniqueQueue
	needRestoreChan chan *needRestoreItem
	dumpClient      *client.Redis

	stat struct {
		Name              string `json:"name"`
		ScanFinished      bool   `json:"scan_finished"`
		ScanDbId          int    `json:"scan_dbId"`
		ScanCursor        uint64 `json:"scan_cursor"`
		ScanPercentByDbId string `json:"scan_percent"`
		NeedUpdateCount   int64  `json:"need_update_count"`
	}
}

func NewScanStandaloneReader(ctx context.Context, opts *ScanReaderOptions) Reader {
	r := new(scanStandaloneReader)
	// dbs
	c := client.NewRedisClient(ctx, opts.Address, opts.Username, opts.Password, opts.Tls, opts.TlsConfig, opts.PreferReplica)
	if len(opts.DBS) != 0 {
		r.dbs = opts.DBS
	} else if c.IsCluster() { // not use opts.Cluster, because user may use standalone mode to scan a cluster node
		r.dbs = []int{0}
	} else {
		c.Send("info", "keyspace")
		info, err := c.Receive()
		if err != nil {
			log.Panicf(err.Error())
		}
		r.dbs = utils.ParseDBs(info.(string))
	}
	r.opts = opts
	r.ch = make(chan *entry.Entry, 1024)
	r.stat.Name = "reader_" + strings.Replace(opts.Address, ":", "_", -1)
	r.needDumpQueue = utils.NewUniqueQueue(100000)        // cache 100000 keys
	r.needRestoreChan = make(chan *needRestoreItem, 1024) // inflight 1024 keys
	log.Infof("[%s] scanStandaloneReader init finished. dbs=[%v]", r.stat.Name, r.dbs)
	return r
}

func (r *scanStandaloneReader) StartRead(ctx context.Context) []chan *entry.Entry {
	r.ctx = ctx
	if r.opts.Scan {
		go r.scan()
	}
	if r.opts.KSN {
		go r.subscript()
	}
	go r.dump()
	go r.restore()
	return []chan *entry.Entry{r.ch}
}

func (r *scanStandaloneReader) subscript() {
	c := client.NewRedisClient(r.ctx, r.opts.Address, r.opts.Username, r.opts.Password, r.opts.Tls, r.opts.TlsConfig, r.opts.PreferReplica)
	if len(r.dbs) == 0 {
		c.Send("psubscribe", "__keyevent@*__:*")
	} else {
		strs := make([]string, len(r.dbs))
		for i, v := range r.dbs {
			strs[i] = strconv.Itoa(v)
		}
		s := fmt.Sprintf("__keyevent@[%v]__:*", strings.Join(strs, ","))
		c.Send("psubscribe", s)
	}
	_, err := c.Receive()
	if err != nil {
		log.Panicf(err.Error())
	}
	regex := regexp.MustCompile(`\d+`)
	for {
		select {
		case <-r.ctx.Done():
			log.Infof("[%s] scanStandaloneReader subscript finished.", r.stat.Name)
			r.needDumpQueue.Close()
			return
		default:
			resp, err := c.Receive()
			if err != nil {
				log.Panicf(err.Error())
			}
			respSlice := resp.([]interface{})
			key := respSlice[3].(string)
			dbId := regex.FindString(respSlice[2].(string))
			dbIdInt, err := strconv.Atoi(dbId)
			if err != nil {
				log.Panicf(err.Error())
			}
			// handle del action
			eventSlice := strings.Split(respSlice[2].(string), ":")
			if eventSlice[1] == "del" {
				e := entry.NewEntry()
				e.DbId = dbIdInt
				e.Argv = []string{"DEL", key}
				r.ch <- e
				continue
			}
			r.needDumpQueue.Put(dbKey{db: dbIdInt, key: key})
		}
	}
}

func (r *scanStandaloneReader) scan() {
	c := client.NewRedisClient(r.ctx, r.opts.Address, r.opts.Username, r.opts.Password, r.opts.Tls, r.opts.TlsConfig, r.opts.PreferReplica)
	defer c.Close()
	for _, dbId := range r.dbs {
		if dbId != 0 {
			reply := c.DoWithStringReply("SELECT", strconv.Itoa(dbId))
			if reply != "OK" {
				log.Panicf("scanStandaloneReader select db failed. db=[%d]", dbId)
			}
		}

		var cursor uint64 = 0
		count := r.opts.Count
		for {
			select {
			case <-r.ctx.Done():
				log.Infof("[%s] scanStandaloneReader scan finished.", r.stat.Name)
				r.needDumpQueue.Close()
				return
			default:
			}

			var keys []string
			cursor, keys = c.Scan(cursor, count)
			for _, key := range keys {
				r.needDumpQueue.Put(dbKey{dbId, key}) // pass value not pointer
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
		r.needDumpQueue.Close()
	}
}

func (r *scanStandaloneReader) dump() {
	nowDbId := 0
	r.dumpClient = client.NewRedisClient(r.ctx, r.opts.Address, r.opts.Username, r.opts.Password, r.opts.Tls, r.opts.TlsConfig, r.opts.PreferReplica)
	// Support prefer_replica=true in both Cluster and Standalone mode
	if r.opts.PreferReplica {
		r.dumpClient.Do("READONLY")
		log.Infof("running dump() in read-only mode")
	}

	for item := range r.needDumpQueue.Ch {
		r.stat.NeedUpdateCount = int64(r.needDumpQueue.Len())
		dbId := item.(dbKey).db
		key := item.(dbKey).key
		if nowDbId != dbId {
			r.dumpClient.Send("SELECT", strconv.Itoa(dbId))
			nowDbId = dbId
		}
		// dump
		r.dumpClient.Send("DUMP", key)
		r.dumpClient.Send("PTTL", key)
		r.needRestoreChan <- &needRestoreItem{dbId, key}
	}
	close(r.needRestoreChan)
	log.Infof("[%s] scanStandaloneReader dump finished.", r.stat.Name)
}

func (r *scanStandaloneReader) restore() {
	nowDbId := 0
	for item := range r.needRestoreChan {
		dbId := item.dbId
		key := item.key
		if nowDbId != dbId {
			reply, err := r.dumpClient.Receive()
			if err != nil || reply != "OK" {
				log.Panicf("scanStandaloneReader select db failed. db=[%d]", dbId)
			}
			nowDbId = dbId
		}
		iDump, err1 := r.dumpClient.Receive()
		iPttl, err2 := r.dumpClient.Receive()
		if errors.Is(err1, proto.Nil) {
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
			cmdC := o.Rewrite()
			for cmd := range cmdC {
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
	log.Infof("[%s] scanStandaloneReader restore finished.", r.stat.Name)
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
