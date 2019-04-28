package scanner

import (
	"strconv"
	"fmt"
	"bytes"

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
	case AliyunCluster:
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
	case TencentCluster:
		/*
		 * tencent cluster return:
		 * 10.1.1.1:2000> cluster nodes
		 * 25b21f1836026bd49c52b2d10e09fbf8c6aa1fdc 10.0.0.15:6379@11896 slave 36034e645951464098f40d339386e9d51a9d7e77 0 1531471918205 1 connected
		 * da6041781b5d7fe21404811d430cdffea2bf84de 10.0.0.15:6379@11170 master - 0 1531471916000 2 connected 10923-16383
		 * 36034e645951464098f40d339386e9d51a9d7e77 10.0.0.15:6379@11541 myself,master - 0 1531471915000 1 connected 0-5460
		 * 53f552fd8e43112ae68b10dada69d3af77c33649 10.0.0.15:6379@11681 slave da6041781b5d7fe21404811d430cdffea2bf84de 0 1531471917204 3 connected
		 * 18090a0e57cf359f9f8c8c516aa62a811c0f0f0a 10.0.0.15:6379@11428 slave ef3cf5e20e1a7cf5f9cc259ed488c82c4aa17171 0 1531471917000 2 connected
		 * ef3cf5e20e1a7cf5f9cc259ed488c82c4aa17171 10.0.0.15:6379@11324 master - 0 1531471916204 0 connected 5461-10922
		 */
		info, err := redis.Bytes(scs.client.Do("cluster", "nodes"))
		if err != nil {
			return -1, err
		}

		lines := bytes.Split(info, []byte("\r\n"))
		ret := make([]string, 0, len(lines)/2)
		master := []byte("master")
		for _, row := range lines {
			col := bytes.Split(row, []byte(" "))
			if len(col) >= 3 && bytes.Contains(col[2], master) {
				ret = append(ret, string(col[0]))
			}
		}
		scs.tencentNodes = ret
		return len(ret), nil
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
	case TencentCluster:
		values, err = redis.Values(scs.client.Do("SCAN", scs.cursor, "COUNT",
			conf.Options.ScanKeyNumber, scs.tencentNodes[node.(int)]))
	case AliyunCluster:
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
