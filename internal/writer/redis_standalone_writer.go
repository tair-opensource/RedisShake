package writer

import (
	"context"
	"errors"
	"fmt"
	"go.uber.org/ratelimit"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"RedisShake/internal/client"
	"RedisShake/internal/client/proto"
	"RedisShake/internal/config"
	"RedisShake/internal/entry"
	"RedisShake/internal/log"
)

type RedisWriterOptions struct {
	Cluster   bool                   `mapstructure:"cluster" default:"false"`
	Address   string                 `mapstructure:"address" default:""`
	Username  string                 `mapstructure:"username" default:""`
	Password  string                 `mapstructure:"password" default:""`
	Tls       bool                   `mapstructure:"tls" default:"false"`
	TlsConfig client.TlsConfig       `mapstructure:"tls_config" default:"{}"`
	OffReply  bool                   `mapstructure:"off_reply" default:"false"`
	// Parallel is the number of independent target connections the writer fans
	// entries out across. The single shared input channel hands each entry to
	// exactly one connection worker (no key partitioning needed; RESTOREs of
	// distinct keys are order-independent). Default 1 == original behavior.
	Parallel  int                    `mapstructure:"parallel" default:"1"`
	Sentinel  client.SentinelOptions `mapstructure:"sentinel"`
}

// connWriter is one target connection plus its in-flight reply queue. Each runs
// its own processWrite/processReply pair; they share the writer's input channel.
type connWriter struct {
	client      *client.Redis
	chWaitReply chan *entry.Entry
	DbId        int
}

type redisStandaloneWriter struct {
	address  string
	conns    []*connWriter
	offReply bool
	rl       ratelimit.Limiter // shared: target_redis_max_qps is a global cap

	ch       chan *entry.Entry
	chWg     sync.WaitGroup // processWrite goroutines
	chWaitWg sync.WaitGroup // processReply goroutines

	stat struct {
		Name              string `json:"name"`
		UnansweredBytes   int64  `json:"unanswered_bytes"`
		UnansweredEntries int64  `json:"unanswered_entries"`
	}
}

func NewRedisStandaloneWriter(ctx context.Context, opts *RedisWriterOptions) Writer {
	rw := new(redisStandaloneWriter)
	rw.address = opts.Address
	rw.stat.Name = "writer_" + strings.Replace(opts.Address, ":", "_", -1)
	rw.offReply = opts.OffReply

	parallel := opts.Parallel
	if parallel < 1 {
		parallel = 1
	}
	rw.rl = ratelimit.New(config.Opt.Advanced.TargetRedisMaxQPS)
	log.Infof("set target redis max qps to %d (shared across %d writer connections)", config.Opt.Advanced.TargetRedisMaxQPS, parallel)

	rw.ch = make(chan *entry.Entry, config.Opt.Advanced.PipelineCountLimit)
	rw.conns = make([]*connWriter, parallel)
	for i := 0; i < parallel; i++ {
		cw := &connWriter{
			client: client.NewRedisClient(ctx, opts.Address, opts.Username, opts.Password, opts.Tls, opts.TlsConfig, false),
		}
		if opts.OffReply {
			cw.client.Send("CLIENT", "REPLY", "OFF")
		} else {
			cw.chWaitReply = make(chan *entry.Entry, config.Opt.Advanced.PipelineCountLimit*2)
			rw.chWaitWg.Add(1)
			go rw.processReply(cw)
		}
		rw.conns[i] = cw
	}
	if opts.OffReply {
		log.Infof("turn off the reply of write")
	}
	return rw
}

func (w *redisStandaloneWriter) Close() {
	close(w.ch)
	w.chWg.Wait()
	if !w.offReply {
		for _, cw := range w.conns {
			close(cw.chWaitReply)
		}
		w.chWaitWg.Wait()
	}
}

func (w *redisStandaloneWriter) StartWrite(ctx context.Context) chan *entry.Entry {
	for _, cw := range w.conns {
		w.chWg.Add(1)
		go w.processWrite(ctx, cw)
	}
	return w.ch
}

func (w *redisStandaloneWriter) Write(e *entry.Entry) {
	w.ch <- e
}

func (w *redisStandaloneWriter) switchDbTo(cw *connWriter, newDbId int) {
	log.Debugf("[%s] switch db to [%d]", w.stat.Name, newDbId)
	cw.client.Send("select", strconv.Itoa(newDbId))
	cw.DbId = newDbId
	if !w.offReply {
		cw.chWaitReply <- &entry.Entry{
			Argv:    []string{"select", strconv.Itoa(newDbId)},
			CmdName: "select",
		}
	}
}

func (w *redisStandaloneWriter) processWrite(ctx context.Context, cw *connWriter) {
	ticker := time.NewTicker(10 * time.Millisecond)
	defer ticker.Stop()

	for {
		select {
		case <-ctx.Done():
			// do nothing until w.ch is closed
		case <-ticker.C:
			cw.client.Flush()
		case e, ok := <-w.ch:
			if !ok {
				// clean up and exit
				cw.client.Flush()
				w.chWg.Done()
				return
			}
			// switch db if we need
			if cw.DbId != e.DbId {
				w.switchDbTo(cw, e.DbId)
			}
			// send
			bytes := e.Serialize()
			// Wait only while target has in-flight bytes; otherwise a single oversized
			// entry would spin forever (processReply can't drain what it never gets).
			for unanswered := atomic.LoadInt64(&w.stat.UnansweredBytes); unanswered > 0 && e.SerializedSize+unanswered > config.Opt.Advanced.TargetRedisClientMaxQuerybufLen; unanswered = atomic.LoadInt64(&w.stat.UnansweredBytes) {
				time.Sleep(time.Millisecond)
			}
			if e.SerializedSize > config.Opt.Advanced.TargetRedisClientMaxQuerybufLen {
				key := ""
				if len(e.Keys) > 0 {
					key = e.Keys[0]
				}
				log.Warnf("[%s] entry serialized size=%d exceeds target_redis_client_max_querybuf_len=%d, sending anyway. cmd=[%s] key=[%s]",
					w.stat.Name, e.SerializedSize, config.Opt.Advanced.TargetRedisClientMaxQuerybufLen, e.CmdName, key)
			}
			w.rl.Take()
			log.Debugf("[%s] send cmd. cmd=[%s]", w.stat.Name, e.String())
			if !w.offReply {
				select {
				case cw.chWaitReply <- e:
				default:
					cw.client.Flush()
					cw.chWaitReply <- e
				}
				atomic.AddInt64(&w.stat.UnansweredBytes, e.SerializedSize)
				atomic.AddInt64(&w.stat.UnansweredEntries, 1)
			}
			cw.client.SendBytesBuff(bytes)
		}
	}
}

func (w *redisStandaloneWriter) processReply(cw *connWriter) {
	for e := range cw.chWaitReply {
		reply, err := cw.client.Receive()
		log.Debugf("[%s] receive reply. reply=[%v], cmd=[%s]", w.stat.Name, reply, e.String())

		// It's good to skip the nil error since some write commands will return the null reply. For example,
		// the SET command with NX option will return nil if the key already exists.
		if err != nil && !errors.Is(err, proto.Nil) {
			if err.Error() == "BUSYKEY Target key name already exists." {
				if config.Opt.Advanced.RDBRestoreCommandBehavior == "skip" {
					log.Debugf("[%s] redisStandaloneWriter received BUSYKEY reply. cmd=[%s]", w.stat.Name, e.String())
				} else if config.Opt.Advanced.RDBRestoreCommandBehavior == "panic" {
					log.Panicf("[%s] redisStandaloneWriter received BUSYKEY reply. cmd=[%s]", w.stat.Name, e.String())
				}
			} else {
				log.Panicf("[%s] receive reply failed. cmd=[%s], error=[%v]", w.stat.Name, e.String(), err)
			}
		}
		if strings.EqualFold(e.CmdName, "select") { // skip select command
			continue
		}
		atomic.AddInt64(&w.stat.UnansweredBytes, -e.SerializedSize)
		atomic.AddInt64(&w.stat.UnansweredEntries, -1)
	}
	w.chWaitWg.Done()
}

func (w *redisStandaloneWriter) Status() interface{} {
	return w.stat
}

func (w *redisStandaloneWriter) StatusString() string {
	return fmt.Sprintf("[%s]: unanswered_entries=%d", w.stat.Name, atomic.LoadInt64(&w.stat.UnansweredEntries))
}

func (w *redisStandaloneWriter) StatusConsistent() bool {
	return atomic.LoadInt64(&w.stat.UnansweredBytes) == 0 && atomic.LoadInt64(&w.stat.UnansweredEntries) == 0
}
