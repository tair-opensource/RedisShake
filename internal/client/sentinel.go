package client

import (
	"RedisShake/internal/log"
	"context"
	"fmt"
)

type SentinelOptions struct {
	MasterName string    `mapstructure:"master_name" default:""`
	Address    string    `mapstructure:"address" default:""`
	Username   string    `mapstructure:"username" default:""`
	Password   string    `mapstructure:"password" default:""`
	Tls        bool      `mapstructure:"tls" default:"false"`
	TlsConfig  TlsConfig `mapstructure:"tls_config" default:"{}"`
}

func FetchAddressFromSentinel(opts *SentinelOptions) string {
	log.Infof("fetching master address from sentinel. sentinel address: %s, master name: %s", opts.Address, opts.MasterName)

	ctx := context.Background()
	c := NewRedisClient(ctx, opts.Address, opts.Username, opts.Password, opts.Tls, opts.TlsConfig, false)
	defer c.Close()
	c.Send("SENTINEL", "GET-MASTER-ADDR-BY-NAME", opts.MasterName)
	hostport := ArrayString(c.Receive())
	address := fmt.Sprintf("%s:%s", hostport[0], hostport[1])
	log.Infof("fetched master address: %s", address)
	return address
}
