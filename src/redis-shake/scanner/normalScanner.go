package scanner

import (
	"fmt"

	"redis-shake/configure"

	"github.com/garyburd/redigo/redis"
)

type NormalScanner struct {
	client redis.Conn
	cursor int64
}

func (ns *NormalScanner) ScanKey() ([]string, error) {
	var keys []string
	values, err := redis.Values(ns.client.Do("SCAN", ns.cursor, "COUNT",
		conf.Options.ScanKeyNumber))
	if err != nil && err != redis.ErrNil {
		return nil, fmt.Errorf("NormalScanner: scan with cursor[%v] failed[%v]", ns.cursor, err)
	}

	values, err = redis.Scan(values, &ns.cursor, &keys)
	if err != nil && err != redis.ErrNil {
		return nil, fmt.Errorf("NormalScanner: do scan with cursor[%v] failed[%v]", ns.cursor, err)
	}

	return keys, nil
}

func (ns *NormalScanner) EndNode() bool {
	return ns.cursor == 0
}

func (ns *NormalScanner) Close() {
	ns.client.Close()
}
