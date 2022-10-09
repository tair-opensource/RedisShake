package writer

import (
	"fmt"
	"github.com/alibaba/RedisShake/internal/client"
	"github.com/alibaba/RedisShake/internal/entry"
	"github.com/alibaba/RedisShake/internal/log"
	"strconv"
	"strings"
)

const KeySlots = 16384

type RedisClusterWriter struct {
	addresses []string
	writers   []Writer
	router    [KeySlots]Writer
}

func NewRedisClusterWriter(address string, username string, password string, isTls bool) Writer {
	rw := new(RedisClusterWriter)

	rw.loadClusterNodes(address, username, password, isTls)

	log.Infof("redisClusterWriter connected to redis cluster successful. addresses=%v", rw.addresses)
	return rw
}

func (r *RedisClusterWriter) loadClusterNodes(address string, username string, password string, isTls bool) {
	client_ := client.NewRedisClient(address, username, password, isTls)
	reply := client_.DoWithStringReply("cluster", "nodes")
	reply = strings.TrimSpace(reply)
	for _, line := range strings.Split(reply, "\n") {
		line = strings.TrimSpace(line)
		words := strings.Split(line, " ")
		if !strings.Contains(words[2], "master") {
			continue
		}
		if len(words) < 9 {
			log.Panicf("invalid cluster nodes line: %s", line)
		}
		log.Infof("redisClusterWriter load cluster nodes. line=%v", line)
		// address
		address := strings.Split(words[1], "@")[0]

		// handle ipv6 address
		tok := strings.Split(address, ":")
		if len(tok) > 2 {
			// ipv6 address
			port := tok[len(tok)-1]

			ipv6Addr := strings.Join(tok[:len(tok)-1], ":")
			address = fmt.Sprintf("[%s]:%s", ipv6Addr, port)
		}

		r.addresses = append(r.addresses, address)
		// writers
		redisWriter := NewRedisWriter(address, username, password, isTls)
		r.writers = append(r.writers, redisWriter)
		// parse slots
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
				r.router[j] = redisWriter
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
		for _, writer := range r.writers {
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

func (r *RedisClusterWriter) Close() {
	for _, writer := range r.writers {
		writer.Close()
	}
}
