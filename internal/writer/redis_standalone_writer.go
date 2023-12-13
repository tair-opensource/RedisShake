package writer

import (
	"RedisShake/internal/client"
	"RedisShake/internal/client/proto"
	"RedisShake/internal/config"
	"RedisShake/internal/entry"
	"RedisShake/internal/log"
	"fmt"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"time"
)

type RedisWriterOptions struct {
	Cluster  bool   `mapstructure:"cluster" default:"false"`
	Address  string `mapstructure:"address" default:""`
	Username string `mapstructure:"username" default:""`
	Password string `mapstructure:"password" default:""`
	Tls      bool   `mapstructure:"tls" default:"false"`
}

type redisStandaloneWriter struct {
	address string
	client  *client.Redis
	DbId    int

	chWaitReply chan *entry.Entry
	chWg        sync.WaitGroup

	stat struct {
		Name              string `json:"name"`
		UnansweredBytes   int64  `json:"unanswered_bytes"`
		UnansweredEntries int64  `json:"unanswered_entries"`
	}
}

func NewRedisStandaloneWriter(opts *RedisWriterOptions) Writer {
	rw := new(redisStandaloneWriter)
	rw.address = opts.Address
	rw.stat.Name = "writer_" + strings.Replace(opts.Address, ":", "_", -1)
	rw.client = client.NewRedisClient(opts.Address, opts.Username, opts.Password, opts.Tls)
	rw.chWaitReply = make(chan *entry.Entry, config.Opt.Advanced.PipelineCountLimit)
	rw.chWg.Add(1)
	go rw.processReply()
	return rw
}

func (w *redisStandaloneWriter) Flush() {
	reply := w.client.DoWithStringReply("FLUSHALL")
	if reply != "OK" {
		log.Panicf("flush failed with reply: %s", reply)
	}
}

func (w *redisStandaloneWriter) Close() {
	close(w.chWaitReply)
	w.chWg.Wait()
}

func (w *redisStandaloneWriter) Write(e *entry.Entry) {
	// switch db if we need
	if w.DbId != e.DbId {
		w.switchDbTo(e.DbId)
	}

	// send
	bytes := e.Serialize()
	for e.SerializedSize+atomic.LoadInt64(&w.stat.UnansweredBytes) > config.Opt.Advanced.TargetRedisClientMaxQuerybufLen {
		time.Sleep(1 * time.Nanosecond)
	}
	log.Debugf("[%s] send cmd. cmd=[%s]", w.stat.Name, e.String())
	w.chWaitReply <- e
	atomic.AddInt64(&w.stat.UnansweredBytes, e.SerializedSize)
	atomic.AddInt64(&w.stat.UnansweredEntries, 1)
	w.client.SendBytes(bytes)
}

func (w *redisStandaloneWriter) switchDbTo(newDbId int) {
	log.Debugf("[%s] switch db to [%d]", w.stat.Name, newDbId)
	w.client.Send("select", strconv.Itoa(newDbId))
	w.DbId = newDbId
	w.chWaitReply <- &entry.Entry{
		Argv:    []string{"select", strconv.Itoa(newDbId)},
		CmdName: "select",
	}
}

func (w *redisStandaloneWriter) processReply() {
	for e := range w.chWaitReply {
		reply, err := w.client.Receive()
		log.Debugf("[%s] receive reply. reply=[%v], cmd=[%s]", w.stat.Name, reply, e.String())
		if err == proto.Nil {
			log.Warnf("[%s] receive nil reply. cmd=[%s]", w.stat.Name, e.String())
		} else if err != nil {
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
	w.chWg.Done()
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
