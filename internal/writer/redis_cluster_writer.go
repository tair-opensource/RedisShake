package writer

import (
	"context"

	"RedisShake/internal/entry"
	"RedisShake/internal/log"
	"RedisShake/internal/utils"
)

const KeySlots = 16384

type RedisClusterWriter struct {
	addresses []string
	writers   []Writer
	router    [KeySlots]Writer

	stat []interface{}
}

func NewRedisClusterWriter(ctx context.Context, opts *RedisWriterOptions) Writer {
	rw := new(RedisClusterWriter)
	rw.loadClusterNodes(ctx, opts)
	log.Infof("redisClusterWriter connected to redis cluster successful. addresses=%v", rw.addresses)
	return rw
}

func (r *RedisClusterWriter) Close() {
	for _, writer := range r.writers {
		writer.Close()
	}
}

func (r *RedisClusterWriter) loadClusterNodes(ctx context.Context, opts *RedisWriterOptions) {
	addresses, slots := utils.GetRedisClusterNodes(ctx, opts.Address, opts.Username, opts.Password, opts.Tls, false)
	r.addresses = addresses
	for i, address := range addresses {
		theOpts := *opts
		theOpts.Address = address
		redisWriter := NewRedisStandaloneWriter(ctx, &theOpts)
		r.writers = append(r.writers, redisWriter)
		for _, s := range slots[i] {
			if r.router[s] != nil {
				log.Panicf("redisClusterWriter: slot %d already occupied", s)
			}
			r.router[s] = redisWriter
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

func (r *RedisClusterWriter) Consistent() bool {
	for _, writer := range r.writers {
		if !writer.StatusConsistent() {
			return false
		}
	}
	return true
}

func (r *RedisClusterWriter) Status() interface{} {
	r.stat = make([]interface{}, 0)
	for _, writer := range r.writers {
		r.stat = append(r.stat, writer.Status())
	}
	return r.stat
}

func (r *RedisClusterWriter) StatusString() string {
	return "[redis_cluster_writer] writing to redis cluster"
}

func (r *RedisClusterWriter) StatusConsistent() bool {
	for _, writer := range r.writers {
		if !writer.StatusConsistent() {
			return false
		}
	}
	return true
}
