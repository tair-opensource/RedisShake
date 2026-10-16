package reader

import (
	"context"
	"errors"
	"fmt"
	"math/bits"
	"regexp"
	"strconv"
	"strings"
	"sync"
	"time"

	"RedisShake/internal/client"
	"RedisShake/internal/client/proto"
	"RedisShake/internal/config"
	"RedisShake/internal/entry"
	"RedisShake/internal/log"
	"RedisShake/internal/rdb/types"
	"RedisShake/internal/utils"
)

type ScanReaderOptions struct {
	Cluster         bool             `mapstructure:"cluster" default:"false"`
	Address         string           `mapstructure:"address" default:""`
	Username        string           `mapstructure:"username" default:""`
	Password        string           `mapstructure:"password" default:""`
	Tls             bool             `mapstructure:"tls" default:"false"`
	TlsConfig       client.TlsConfig `mapstructure:"tls_config" default:"{}"`
	Scan            bool             `mapstructure:"scan" default:"true"`
	KSN             bool             `mapstructure:"ksn" default:"false"`
	DBS             []int            `mapstructure:"dbs"`
	PreferReplica   bool             `mapstructure:"prefer_replica" default:"false"`
	Count           int              `mapstructure:"count" default:"1"`
	SkipUnknownType []string         `mapstructure:"skip_unknown_type" default:"[]"`
	// DumpParallel is the number of independent source connections that drain
	// the shared needDumpQueue and run DUMP+PTTL. A single connection caps at
	// ~22k keys/s (round-trip latency), starving downstream writers. Each worker
	// pulls keys off the shared channel (no key partitioning needed). Default 1.
	DumpParallel int `mapstructure:"dump_parallel" default:"1"`
	// DumpQueueSize bounds the scan()->dump() handoff queue. scan() enumerates
	// key names far faster than dump() processes them; an unbounded queue buffers
	// ~the whole keyspace in RAM (OOM on large DBs) and the large live heap also
	// degrades throughput via GC. The bounded default makes Put() block,
	// backpressuring scan() to dump() throughput so memory stays flat. Set a
	// large value to restore the previous effectively-unbounded behavior.
	DumpQueueSize int `mapstructure:"dump_queue_size" default:"100000"`
}

type dbKey struct {
	db  int
	key string
}

type needRestoreItem struct {
	dbId int
	key  string
}

// dumpWorker is one source connection that drains the shared needDumpQueue.
// Each worker runs its own dump()/restore() pair on its own connection, so
// pipelined replies stay ordered per-connection. Workers share needDumpQueue
// (input) and r.ch (output); the Go channel hands each key to exactly one
// worker, so no key partitioning is needed.
type dumpWorker struct {
	client          *client.Redis
	needRestoreChan chan *needRestoreItem
	isValkey        bool
}

type scanStandaloneReader struct {
	ctx           context.Context
	dbs           []int
	opts          *ScanReaderOptions
	ch            chan *entry.Entry
	needDumpQueue *utils.UniqueQueue
	subWG         sync.WaitGroup
	restoreWG     sync.WaitGroup

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
	r.dbs = opts.DBS
	r.opts = opts
	r.ch = make(chan *entry.Entry, 1024)
	r.stat.Name = "reader_" + strings.Replace(opts.Address, ":", "_", -1)
	queueSize := opts.DumpQueueSize
	if queueSize < 1 {
		queueSize = 100000
	}
	r.needDumpQueue = utils.NewUniqueQueue(queueSize) // bounded: backpressure scan() to dump() speed (avoids buffering whole keyspace)
	log.Infof("[%s] scanStandaloneReader init finished. dbs=[%v], dump_queue_size=[%d]", r.stat.Name, r.dbs, queueSize)
	return r
}

func (r *scanStandaloneReader) StartRead(ctx context.Context) []chan *entry.Entry {
	r.ctx = ctx
	if r.opts.KSN {
		r.subWG.Add(1)
		go r.subscribe()
		r.subWG.Wait()
	}
	if r.opts.Scan {
		go r.scan()
	}
	parallel := r.opts.DumpParallel
	if parallel < 1 {
		parallel = 1
	}
	log.Infof("[%s] starting %d dump worker(s)", r.stat.Name, parallel)
	for i := 0; i < parallel; i++ {
		w := &dumpWorker{
			client:          client.NewRedisClient(r.ctx, r.opts.Address, r.opts.Username, r.opts.Password, r.opts.Tls, r.opts.TlsConfig, r.opts.PreferReplica),
			needRestoreChan: make(chan *needRestoreItem, 1024),
		}
		r.restoreWG.Add(1)
		go r.dump(w)
		go r.restore(w)
	}
	// Close the output channel once every worker's restore() has drained.
	go func() {
		r.restoreWG.Wait()
		close(r.ch)
	}()
	return []chan *entry.Entry{r.ch}
}

func (r *scanStandaloneReader) subscribe() {
	c := client.NewRedisClient(r.ctx, r.opts.Address, r.opts.Username, r.opts.Password, r.opts.Tls, r.opts.TlsConfig, r.opts.PreferReplica)
	log.Infof("[%s] scanStandaloneReader subscribe started. dbs=[%v]", r.stat.Name, r.dbs)
	if len(r.dbs) == 0 {
		c.Send("psubscribe", "__keyevent@*__:*")
		_, err := c.Receive()
		if err != nil {
			log.Panicf("%v", err)
		}
	} else {
		args := []interface{}{"psubscribe"}
		for _, db := range r.dbs {
			args = append(args, fmt.Sprintf("__keyevent@%v__:*", db))
		}
		c.Send(args...)
		for range r.dbs {
			_, err := c.Receive()
			if err != nil {
				log.Panicf("%v", err)
			}
		}
	}

	// wait
	r.subWG.Done()

	regex := regexp.MustCompile(`\d+`)
	for {
		select {
		case <-r.ctx.Done():
			log.Infof("[%s] scanStandaloneReader subscribe finished.", r.stat.Name)
			r.needDumpQueue.Close()
			return
		default:
			resp, err := c.Receive()
			if err != nil {
				log.Panicf("%v", err)
			}
			respSlice := resp.([]interface{})
			key := respSlice[3].(string)
			dbId := regex.FindString(respSlice[2].(string))
			dbIdInt, err := strconv.Atoi(dbId)
			if err != nil {
				log.Panicf("%v", err)
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
	dbs := r.dbs
	if len(r.dbs) == 0 {
		c.Send("info", "keyspace")
		info, err := c.Receive()
		if err != nil {
			log.Panicf("%v", err)
		}
		dbs = utils.ParseDBs(info.(string))
	}
	for _, dbId := range dbs {
		c.Send("SELECT", strconv.Itoa(dbId))
		reply, err := c.Receive()
		// Redis Cluster and some databases do not support SELECT.
		if dbId != 0 && (err != nil || reply != "OK") {
			log.Panicf("scanStandaloneReader select db failed. db=[%d], err=[%v], reply=[%v]", dbId, err, reply)
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

func (r *scanStandaloneReader) dump(w *dumpWorker) {
	nowDbId := 0
	w.isValkey = w.client.IsValkey()
	log.Infof("[%s] detected server type: %s", r.stat.Name, map[bool]string{true: "Valkey", false: "Redis"}[w.isValkey])
	// Support prefer_replica=true in both Cluster and Standalone mode
	if r.opts.PreferReplica {
		w.client.Do("READONLY")
		log.Infof("running dump() in read-only mode")
	}

	// Batch the DUMP/PTTL sends instead of flushing per command (Send() would do
	// a syscall per command — the scan reader's dominant cost). Flush every
	// `flushEvery` keys or every 5ms (whichever first) so restore() is never
	// starved and a lull can't strand a partial batch. flushEvery stays well
	// under the needRestoreChan capacity to avoid a send/receive deadlock.
	const flushEvery = 128
	ticker := time.NewTicker(5 * time.Millisecond)
	defer ticker.Stop()
	pending := 0
	for {
		select {
		case <-ticker.C:
			if pending > 0 {
				w.client.Flush()
				pending = 0
			}
			continue
		case item, ok := <-r.needDumpQueue.Ch:
			if !ok {
				if pending > 0 {
					w.client.Flush()
				}
				close(w.needRestoreChan)
				log.Infof("[%s] scanStandaloneReader dump finished.", r.stat.Name)
				return
			}
			r.stat.NeedUpdateCount = int64(r.needDumpQueue.Len())
			dbId := item.(dbKey).db
			key := item.(dbKey).key
			if nowDbId != dbId {
				w.client.SendNoFlush("SELECT", strconv.Itoa(dbId))
				nowDbId = dbId
			}
			// dump (buffered; flushed in batches below)
			w.client.SendNoFlush("DUMP", key)
			w.client.SendNoFlush("PTTL", key)
			if len(r.opts.SkipUnknownType) > 0 {
				w.client.SendNoFlush("TYPE", key)
			}
			w.needRestoreChan <- &needRestoreItem{dbId, key}
			pending++
			if pending >= flushEvery {
				w.client.Flush()
				pending = 0
			}
		}
	}
}

// restore sends RESTORE commands to the target Redis.
// Note: rdb_restore_command_behavior configuration only applies when RESTORE command is used.
// For large values exceeding target_redis_proto_max_bulk_len, individual commands (SET, HSET, etc.)
// are used instead, which may not respect the rdb_restore_command_behavior setting.
func (r *scanStandaloneReader) restore(w *dumpWorker) {
	defer r.restoreWG.Done()
	nowDbId := 0
	for item := range w.needRestoreChan {
		dbId := item.dbId
		key := item.key
		if nowDbId != dbId {
			reply, err := w.client.Receive()
			if err != nil || reply != "OK" {
				log.Panicf("scanStandaloneReader select db failed. db=[%d]", dbId)
			}
			nowDbId = dbId
		}
		iDump, err1 := w.client.Receive()
		iPttl, err2 := w.client.Receive()
		if len(r.opts.SkipUnknownType) > 0 {
			iType, err3 := w.client.Receive()
			if err3 != nil {
				log.Panicf("%v", err3)
			}
			typeStr := iType.(string)
			// type in SkipUnknownType
			skip := false
			for _, skipType := range r.opts.SkipUnknownType {
				if strings.EqualFold(typeStr, skipType) {
					skip = true
				}
			}
			if skip {
				log.Infof("skip restore key=[%s] type=[%s]", key, typeStr)
				continue
			}
		}
		if errors.Is(err1, proto.Nil) {
			continue // key not exist
		} else if err1 != nil {
			log.Panicf("%v", err1)
		} else if err2 != nil {
			log.Panicf("%v", err2)
		}
		dump := iDump.(string)
		pttl := 0
		switch v := iPttl.(type) {
		case int64:
			pttl = int(v)
			if pttl == 0 {
				pttl = 1
			}
		case string:
			log.Panicf("iPttl is string, this should not happen. key=[%s], pttl=[%s]", key, v)
		default:
			log.Panicf("unexpected type for pttl: %T", iPttl)
		}

		if pttl == -2 {
			continue // key not exist
		}
		if pttl == -1 {
			pttl = 0 // -1 means no expire
		}
		if uint64(len(dump)) > config.Opt.Advanced.TargetRedisProtoMaxBulkLen {
			log.Warnf("key=[%s] dump len=[%d] exceeds target_redis_proto_max_bulk_len, falling back to individual commands. "+
				"rdb_restore_command_behavior setting may not work correctly for this key.", key, len(dump))
			typeByte := dump[0]
			anotherReader := strings.NewReader(dump[1 : len(dump)-10])
			o := types.ParseObject(anotherReader, typeByte, key, w.isValkey)
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
