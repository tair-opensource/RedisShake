package run

import (
	"pkg/libs/log"
	"strconv"

	"redis-shake/common"
	"redis-shake/configure"

	"github.com/garyburd/redigo/redis"
	"redis-shake/scanner"
)

type CmdRump struct {
	sourceConn redis.Conn
	targetConn redis.Conn

	keyChan    chan *KeyNode // keyChan is used to communicated between routine1 and routine2
	resultChan chan *KeyNode // resultChan is used to communicated between routine2 and routine3

	scanner scanner.Scanner
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

	cr.scanner = scanner.NewScanner(cr.sourceConn)

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
	length, err := cr.scanner.NodeCount()
	if err != nil || length <= 0 {
		log.Panicf("fetch db node failed: length[%v], error[%v]", length, err)
	}

	log.Infof("start fetcher with special-cloud[%v], length[%v]", conf.Options.ScanSpecialCloud, length)

	// iterate all source nodes
	for i := 0; i < length; i++ {
		// fetch data from on node
		for {
			keys, err := cr.scanner.ScanKey(i)
			if err != nil {
				log.Panic(err)
			}

			log.Info("scaned keys: ", len(keys))

			if len(keys) != 0 {
				// pipeline dump
				for _, key := range keys {
					log.Debug("scan key: ", key)
					cr.sourceConn.Send("DUMP", key)
				}
				dumps, err := redis.Strings(cr.sourceConn.Do(""))
				if err != nil && err != redis.ErrNil {
					log.Panicf("do dump with failed[%v]", err)
				}

				// pipeline ttl
				for _, key := range keys {
					cr.sourceConn.Send("PTTL", key)
				}
				pttls, err := redis.Int64s(cr.sourceConn.Do(""))
				if err != nil && err != redis.ErrNil {
					log.Panicf("do ttl with failed[%v]", err)
				}

				for i, k := range keys {
					cr.keyChan <- &KeyNode{k, dumps[i], pttls[i]}
				}
			}

			// Last iteration of scan.
			if cr.scanner.EndNode() {
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
