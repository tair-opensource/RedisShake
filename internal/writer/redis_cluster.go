package writer

import (
	"github.com/alibaba/RedisShake/internal/entry"
	"github.com/alibaba/RedisShake/internal/log"
	"strconv"
	"strings"
)

const KeySlots = 16384

type RedisClusterWriter struct {
	client []Writer
	router [KeySlots]Writer
}

func NewRedisClusterWriter(addresses []string, password string, isTls bool) Writer {
	rw := new(RedisClusterWriter)
	rw.client = make([]Writer, len(addresses))
	for inx, address := range addresses {
		rw.client[inx] = NewRedisWriter(address, password, isTls)
	}
	log.Infof("redisClusterWriter connected to redis cluster successful. addresses=%v", addresses)
	rw.loadClusterNodes()
	return rw
}

func (r *RedisClusterWriter) loadClusterNodes() {
	for _, writer := range r.client {
		standalone := writer.(*redisWriter)
		reply := standalone.client.DoWithStringReply("cluster", "nodes")
		reply = strings.TrimSpace(reply)
		for _, line := range strings.Split(reply, "\n") {
			line = strings.TrimSpace(line)
			words := strings.Split(line, " ")
			if strings.Contains(words[2], "myself") {
				log.Infof("redisClusterWriter load cluster nodes. line=%v", line)
				for i := 8; i < len(words); i++ {
					words[i] = strings.TrimSpace(words[i])
					var start, end int
					var err error
					if strings.Contains(words[i], "-") {
						seg := strings.Split(words[i], "-")
						start, err = strconv.Atoi(seg[0])
						if err != nil {
							log.PanicError(err)
						}
						end, err = strconv.Atoi(seg[1])
						if err != nil {
							log.PanicError(err)
						}
					} else {
						start, err = strconv.Atoi(words[i])
						if err != nil {
							log.PanicError(err)
						}
						end = start
					}
					for j := start; j <= end; j++ {
						if r.router[j] != nil {
							log.Panicf("redisClusterWriter: slot %d already occupied", j)
						}
						r.router[j] = standalone
					}
				}
			}
		}
	}
	for i := 0; i < KeySlots; i++ {
		if r.router[i] == nil {
			log.Panicf("redisClusterWriter: slot %d not occupied", i)
		}
	}
}

func (r *RedisClusterWriter) Write(entry *entry.Entry) {
	if len(entry.Slots) == 0 {
		for _, writer := range r.client {
			writer.Write(entry)
		}
		return
	}

	lastSlot := -1
	for _, slot := range entry.Slots {
		if lastSlot == -1 {
			lastSlot = slot
		}
		if slot != lastSlot {
			log.Panicf("CROSSSLOT Keys in request don't hash to the same slot. argv=%v", entry.Argv)
		}
	}
	r.router[lastSlot].Write(entry)
}
