package writer

import (
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

	chWaitReply chan *entry.Entry

	UpdateUnansweredBytesCount uint64 // have sent in bytes
}

func NewRedisWriter(address string, password string, isTls bool) Writer {
	rw := new(redisWriter)
	rw.client = client.NewRedisClient(address, password, isTls)
	log.Infof("redisWriter connected to redis successful. address=[%s]", address)

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
	buf := client.EncodeArgv(e.Argv)
	e.EncodedSize = uint64(buf.Len())
	for e.EncodedSize+atomic.LoadUint64(&w.UpdateUnansweredBytesCount) > config.Config.Advanced.TargetRedisClientMaxQuerybufLen {
		time.Sleep(1 * time.Millisecond)
	}
	atomic.AddUint64(&w.UpdateUnansweredBytesCount, e.EncodedSize)
	w.client.SendBytes(buf.Bytes())
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
			log.Debugf("redisWriter received reply. argv=%v, reply=%v, error=[%v]", e.Argv, reply, err)
			if err != nil {
				if err.Error() == "BUSYKEY Target key name already exists." {
					if config.Config.Advanced.RDBRestoreCommandBehavior == "skip" {
						log.Warnf("redisWriter received BUSYKEY reply. argv=%v", e.Argv)
					} else if config.Config.Advanced.RDBRestoreCommandBehavior == "panic" {
						log.Panicf("redisWriter received BUSYKEY reply. argv=%v", e.Argv)
					}
				} else {
					log.Panicf("redisWriter received error. error=[%v], argv=%v", err, e.Argv)
				}
			}
			atomic.AddUint64(&w.UpdateUnansweredBytesCount, ^(e.EncodedSize - 1))
			statistics.UpdateEntryId(e.Id)
			statistics.UpdateAOFAppliedOffset(e.Offset)
			statistics.UpdateUnansweredBytesCount(atomic.LoadUint64(&w.UpdateUnansweredBytesCount))
		}
	}
}
