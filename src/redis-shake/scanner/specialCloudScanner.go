package scanner

import (
	"strconv"
	"fmt"
	"redis-shake/common"
	"redis-shake/configure"

	"github.com/garyburd/redigo/redis"
)

type SpecialCloudScanner struct {
	client redis.Conn
	cursor int64

	tencentNodes []string
}

func (scs *SpecialCloudScanner) NodeCount() (int, error) {
	switch conf.Options.ScanSpecialCloud {
	case utils.AliyunCluster:
		info, err := redis.Bytes(scs.client.Do("info", "Cluster"))
		if err != nil {
			return -1, err
		}

		result := utils.ParseInfo(info)
		if count, err := strconv.ParseInt(result["nodecount"], 10, 0); err != nil {
			return -1, err
		} else if count <= 0 {
			return -1, fmt.Errorf("source node count[%v] illegal", count)
		} else {
			return int(count), nil
		}
	case utils.TencentCluster:
		var err error
		scs.tencentNodes, err = utils.GetAllClusterNode(scs.client, conf.StandAloneRoleMaster, "id")
		if err != nil {
			return -1, err
		}

		return len(scs.tencentNodes), nil
	default:
		return -1, nil
	}
}

func (scs *SpecialCloudScanner) ScanKey(node interface{}) ([]string, error) {
	var (
		values []interface{}
		err    error
		keys   []string
	)

	switch conf.Options.ScanSpecialCloud {
	case utils.TencentCluster:
		values, err = redis.Values(scs.client.Do("SCAN", scs.cursor, "COUNT",
			conf.Options.ScanKeyNumber, scs.tencentNodes[node.(int)]))
	case utils.AliyunCluster:
		values, err = redis.Values(scs.client.Do("ISCAN", node, scs.cursor, "COUNT",
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
