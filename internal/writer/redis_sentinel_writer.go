package writer

import (
	"RedisShake/internal/client"
	"RedisShake/internal/log"
	"context"
	"fmt"
)

func NewRedisSentinelWriter(ctx context.Context, opts *RedisWriterOptions) Writer {
	sentinel := client.NewSentinelClient(ctx, opts.Address, opts.Tls)
	sentinel.Send("SENTINEL", "GET-MASTER-ADDR-BY-NAME", opts.Master)
	addr, err := sentinel.Receive()
	if err != nil {
		log.Panicf(err.Error())
	}
	hostport := addr.([]interface{})
	address := fmt.Sprintf("%s:%s", hostport[0].(string), hostport[1].(string))
	sentinel.Close()

	redisOpt := &RedisWriterOptions{
		Address:  address,
		Username: opts.Username,
		Password: opts.Password,
		Tls:      opts.Tls,
		OffReply: opts.OffReply,
	}
	log.Infof("connecting to master node at %s", redisOpt.Address)
	return NewRedisStandaloneWriter(ctx, redisOpt)
}
