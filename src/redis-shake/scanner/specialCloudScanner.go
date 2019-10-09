package scanner

import (
	"fmt"
	"redis-shake/common"
	"redis-shake/configure"

	"github.com/garyburd/redigo/redis"
)

type SpecialCloudScanner struct {
	client redis.Conn
	cursor int64

	tencentNodeId string
	aliyunNodeId int
}

func (scs *SpecialCloudScanner) ScanKey() ([]string, error) {
	var (
		values []interface{}
		err    error
		keys   []string
	)

	switch conf.Options.ScanSpecialCloud {
	case utils.TencentCluster:
		values, err = redis.Values(scs.client.Do("SCAN", scs.cursor, "COUNT",
			conf.Options.ScanKeyNumber, scs.tencentNodeId))
	case utils.AliyunCluster:
		values, err = redis.Values(scs.client.Do("ISCAN", scs.aliyunNodeId, scs.cursor, "COUNT",
			conf.Options.ScanKeyNumber))
	}
	if err != nil && err != redis.ErrNil {
		return nil, fmt.Errorf("SpecialCloudScanner: scan with cursor[%v] failed[%v]", scs.cursor, err)
	}

	values, err = redis.Scan(values, &scs.cursor, &keys)
	if err != nil && err != redis.ErrNil {
		return nil, fmt.Errorf("SpecialCloudScanner: do scan with cursor[%v] failed[%v]", scs.cursor, err)
	}

	return keys, nil
}

func (scs *SpecialCloudScanner) EndNode() bool {
	return scs.cursor == 0
}

func (scs *SpecialCloudScanner) Close() {
	scs.client.Close()
}
