package run

import (
	"pkg/libs/log"
	"strconv"

	"redis-shake/common"
	"redis-shake/configure"

	"github.com/garyburd/redigo/redis"
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

func (cr *CmdRump) fetcher() {
	length := 1
	if conf.Options.ScanSpecialCloud == TencentCluster {
		length = len(conf.Options.ScanSpecialCloudTencentUrls)
	} else if conf.Options.ScanSpecialCloud == AliyunCluster {
		length = int(conf.Options.ScanSpecialCloudAliyunNodeNumber)
	}

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
					conf.Options.ScanKeyNumber, conf.Options.ScanSpecialCloudTencentUrls[i]))
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