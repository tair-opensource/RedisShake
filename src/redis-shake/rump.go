package run

import (
	"pkg/libs/log"
	"strconv"

	"redis-shake/common"
	"redis-shake/configure"

	"github.com/garyburd/redigo/redis"
	"fmt"
	"bytes"
)

const (
	TencentCluster = "tencent_cluster"
	AliyunCluster  = "aliyun_cluster"
)

type CmdRump struct {
	sourceConn redis.Conn
	targetConn redis.Conn

	keyChan    chan *KeyNode // keyChan is used to communicated between routine1 and routine2
	resultChan chan *KeyNode // resultChan is used to communicated between routine2 and routine3
}

type KeyNode struct {
	key   string
	value string
	pttl  int64
}

func (cr *CmdRump) GetDetailedInfo() []interface{} {
	return nil
}

func (cr *CmdRump) Main() {
	// build connection
	cr.sourceConn = utils.OpenRedisConn(conf.Options.SourceAddress, conf.Options.SourceAuthType,
		conf.Options.SourcePasswordRaw)
	cr.targetConn = utils.OpenRedisConn(conf.Options.TargetAddress, conf.Options.TargetAuthType,
		conf.Options.TargetPasswordRaw)

	// init two channels
	cr.keyChan = make(chan *KeyNode, conf.Options.ScanKeyNumber)
	cr.resultChan = make(chan *KeyNode, conf.Options.ScanKeyNumber)

	/*
	 * we start 3 routines to run:
	 * 1. fetch keys from the source redis
	 * 2. write keys into the target redis
	 * 3. read result from the target redis
	 */
	// routine1
	go cr.fetcher()
	// routine2
	go cr.writer()
	// routine3
	cr.receiver()
}

/*
 * get db node info when conf.Options.ScanSpecialCloud != "".
 * return:
 *     int: node number. Used in aliyun_cluster
 *     []string: node lists. Used in tencent_cluster
 */
func (cr *CmdRump) getDbNode() (int, []string, error) {
	switch conf.Options.ScanSpecialCloud {
	case AliyunCluster:
		info, err := redis.Bytes(cr.sourceConn.Do("info", "Cluster"))
		if err != nil {
			return -1, nil, err
		}

		result := utils.ParseInfo(info)
		if count, err := strconv.ParseInt(result["nodecount"], 10, 0); err != nil {
			return -1, nil, err
		} else if count <= 0 {
			return -1, nil, fmt.Errorf("source node count[%v] illegal", count)
		} else {
			return int(count), nil, nil
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
		info, err := redis.Bytes(cr.sourceConn.Do("cluster", "nodes"))
		if err != nil {
			return -1, nil, err
		}

		lines := bytes.Split(info, []byte("\r\n"))
		ret := make([]string, 0, len(lines) / 2)
		master := []byte("master")
		for _, row := range lines {
			col := bytes.Split(row, []byte(" "))
			if len(col) >= 3 && bytes.Contains(col[2], master) {
				ret = append(ret, string(col[0]))
			}
		}
		return len(ret), ret, nil
	default:
		return 1, nil, nil
	}
}

func (cr *CmdRump) fetcher() {
	length, list, err := cr.getDbNode()
	if err != nil || length <= 0 {
		log.Panicf("fetch db node failed: length[%v], list[%v], error[%v]", length, list, err)
	}

	log.Infof("start fetcher with special-cloud[%v], length[%v], list[%v]", conf.Options.ScanSpecialCloud,
		length, list)

	// iterate all source nodes
	for i := 0; i < length; i++ {
		var (
			cursor int64
			keys   []string
			values []interface{}
			err    error
		)

		// fetch data from on node
		for {
			switch conf.Options.ScanSpecialCloud {
			case "":
				values, err = redis.Values(cr.sourceConn.Do("SCAN", cursor, "COUNT",
					conf.Options.ScanKeyNumber))
			case TencentCluster:
				values, err = redis.Values(cr.sourceConn.Do("SCAN", cursor, "COUNT",
					conf.Options.ScanKeyNumber, list[i]))
			case AliyunCluster:
				values, err = redis.Values(cr.sourceConn.Do("ISCAN", i, cursor, "COUNT",
					conf.Options.ScanKeyNumber))
			}
			if err != nil && err != redis.ErrNil {
				log.Panicf("scan with cursor[%v] failed[%v]", cursor, err)
			}

			values, err = redis.Scan(values, &cursor, &keys)
			if err != nil && err != redis.ErrNil {
				log.Panicf("do scan with cursor[%v] failed[%v]", cursor, err)
			}

			log.Info("scaned keys: ", len(keys))

			// pipeline dump
			for _, key := range keys {
				log.Debug("scan key: ", key)
				cr.sourceConn.Send("DUMP", key)
			}
			dumps, err := redis.Strings(cr.sourceConn.Do(""))
			if err != nil && err != redis.ErrNil {
				log.Panicf("do dump with cursor[%v] failed[%v]", cursor, err)
			}

			// pipeline ttl
			for _, key := range keys {
				cr.sourceConn.Send("PTTL", key)
			}
			pttls, err := redis.Int64s(cr.sourceConn.Do(""))
			if err != nil && err != redis.ErrNil {
				log.Panicf("do ttl with cursor[%v] failed[%v]", cursor, err)
			}

			for i, k := range keys {
				cr.keyChan <- &KeyNode{k, dumps[i], pttls[i]}
			}

			// Last iteration of scan.
			if cursor == 0 {
				break
			}
		}
	}

	close(cr.keyChan)
}

func (cr *CmdRump) writer() {
	var count uint32
	for ele := range cr.keyChan {
		if ele.pttl == -1 { // not set ttl
			ele.pttl = 0
		}
		if ele.pttl == -2 {
			log.Debugf("skip key %s for expired", ele.key)
			continue
		}

		log.Debugf("restore %s", ele.key)
		if conf.Options.Rewrite {
			cr.targetConn.Send("RESTORE", ele.key, ele.pttl, ele.value, "REPLACE")
		} else {
			cr.targetConn.Send("RESTORE", ele.key, ele.pttl, ele.value)
		}

		cr.resultChan <- ele
		count++
		if count == conf.Options.ScanKeyNumber {
			// batch
			log.Debugf("send keys %d\n", count)
			cr.targetConn.Flush()
			count = 0
		}
	}
	cr.targetConn.Flush()
	close(cr.resultChan)
}

func (cr *CmdRump) receiver() {
	for ele := range cr.resultChan {
		if _, err := cr.targetConn.Receive(); err != nil && err != redis.ErrNil {
			log.Panicf("restore key[%v] with pttl[%v] error[%v]", ele.key, strconv.FormatInt(ele.pttl, 10),
				err)
		}
	}
}