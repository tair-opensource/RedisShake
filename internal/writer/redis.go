package writer

import (
	"bytes"
	"github.com/alibaba/RedisShake/internal/client"
	"github.com/alibaba/RedisShake/internal/config"
	"github.com/alibaba/RedisShake/internal/entry"
	"github.com/alibaba/RedisShake/internal/log"
	"github.com/alibaba/RedisShake/internal/statistics"
	"strconv"
	"sync/atomic"
	"time"
)

type redisWriter struct {
	client *client.Redis
	DbId   int

	cmdBuffer   *bytes.Buffer
	chWaitReply chan *entry.Entry

	UpdateUnansweredBytesCount uint64 // have sent in bytes
}

func NewRedisWriter(address string, username string, password string, isTls bool) Writer {
	rw := new(redisWriter)
	rw.client = client.NewRedisClient(address, username, password, isTls)
	log.Infof("redisWriter connected to redis successful. address=[%s]", address)
	rw.cmdBuffer = new(bytes.Buffer)
	rw.chWaitReply = make(chan *entry.Entry, config.Config.Advanced.PipelineCountLimit)
	go rw.flushInterval()
	return rw
}

func (w *redisWriter) Write(e *entry.Entry) {
	// switch db if we need
	if w.DbId != e.DbId {
		w.switchDbTo(e.DbId)
	}

	// send
	w.cmdBuffer.Reset()
	client.EncodeArgv(e.Argv, w.cmdBuffer)
	e.EncodedSize = uint64(w.cmdBuffer.Len())
	for e.EncodedSize+atomic.LoadUint64(&w.UpdateUnansweredBytesCount) > config.Config.Advanced.TargetRedisClientMaxQuerybufLen {
		time.Sleep(1 * time.Millisecond)
	}
	atomic.AddUint64(&w.UpdateUnansweredBytesCount, e.EncodedSize)
	w.client.SendBytes(w.cmdBuffer.Bytes())
	w.chWaitReply <- e
}

func (w *redisWriter) switchDbTo(newDbId int) {
	w.client.Send("select", strconv.Itoa(newDbId))
	w.DbId = newDbId
}

func (w *redisWriter) flushInterval() {
	for {
		select {
		case e := <-w.chWaitReply:
			reply, err := w.client.Receive()
			if err != nil {
				if err.Error() == "BUSYKEY Target key name already exists." {
					if config.Config.Advanced.RDBRestoreCommandBehavior == "skip" {
						log.Warnf("redisWriter received BUSYKEY reply. argv=%v", e.Argv)
					} else if config.Config.Advanced.RDBRestoreCommandBehavior == "panic" {
						log.Panicf("redisWriter received BUSYKEY reply. argv=%v", e.Argv)
					}
				} else {
					log.Panicf("redisWriter received error. error=[%v], argv=%v, slots=%v, reply=[%v]", err, e.Argv, e.Slots, reply)
				}
			}
			atomic.AddUint64(&w.UpdateUnansweredBytesCount, ^(e.EncodedSize - 1))
			statistics.UpdateEntryId(e.Id)
			statistics.UpdateAOFAppliedOffset(e.Offset)
			statistics.UpdateUnansweredBytesCount(atomic.LoadUint64(&w.UpdateUnansweredBytesCount))
		}
	}
}
