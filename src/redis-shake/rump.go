package run

import (
	"pkg/libs/log"
	"strconv"

	"redis-shake/common"
	"redis-shake/configure"

	"github.com/garyburd/redigo/redis"
	"redis-shake/scanner"
	"sync"
)

type CmdRump struct {
	sourceConn []redis.Conn
	targetConn redis.Conn

	keyChan    chan *KeyNode // keyChan is used to communicated between routine1 and routine2
	resultChan chan *KeyNode // resultChan is used to communicated between routine2 and routine3

	scanners  []scanner.Scanner // one scanner match one db/proxy
	fetcherWg sync.WaitGroup
}

type KeyNode struct {
	key   string
	value string
	pttl  int64
}

func (cr *CmdRump) GetDetailedInfo() interface{} {
	return nil
}

func (cr *CmdRump) Main() {
	// build connection

	cr.sourceConn = make([]redis.Conn, len(conf.Options.SourceAddress))
	for i, address := range conf.Options.SourceAddress {
		cr.sourceConn[i] = utils.OpenRedisConn(address, conf.Options.SourceAuthType, conf.Options.SourcePasswordRaw)
	}
	cr.targetConn = utils.OpenRedisConn(conf.Options.TargetAddress, conf.Options.TargetAuthType,
		conf.Options.TargetPasswordRaw)

	// init two channels
	chanSize := int(conf.Options.ScanKeyNumber) * len(conf.Options.SourceAddress)
	cr.keyChan = make(chan *KeyNode, chanSize)
	cr.resultChan = make(chan *KeyNode, chanSize)

	cr.scanners = scanner.NewScanner(cr.sourceConn)
	if cr.scanners == nil || len(cr.scanners) == 0 {
		log.Panic("create scanner failed")
		return
	}

	/*
	 * we start 4 routines to run:
	 * 1. fetch keys from the source redis
	 * 2. wait fetcher all exit
	 * 3. write keys into the target redis
	 * 4. read result from the target redis
	 */
	// routine1
	cr.fetcherWg.Add(len(cr.scanners))
	for i := range cr.scanners {
		go cr.fetcher(i)
	}

	// routine2
	go func() {
		cr.fetcherWg.Wait()
		close(cr.keyChan)
	}()

	// routine3
	go cr.writer()

	// routine4
	cr.receiver()
}

func (cr *CmdRump) fetcher(idx int) {
	length, err := cr.scanners[idx].NodeCount()
	if err != nil || length <= 0 {
		log.Panicf("fetch db node failed: length[%v], error[%v]", length, err)
	}

	log.Infof("start fetcher with special-cloud[%v], length[%v]", conf.Options.ScanSpecialCloud, length)

	// iterate all source nodes
	for i := 0; i < length; i++ {
		// fetch data from on node
		for {
			keys, err := cr.scanners[idx].ScanKey(i)
			if err != nil {
				log.Panic(err)
			}

			log.Info("scaned keys: ", len(keys))

			if len(keys) != 0 {
				// pipeline dump
				for _, key := range keys {
					log.Debug("scan key: ", key)
					cr.sourceConn[idx].Send("DUMP", key)
				}
				dumps, err := redis.Strings(cr.sourceConn[idx].Do(""))
				if err != nil && err != redis.ErrNil {
					log.Panicf("do dump with failed[%v]", err)
				}

				// pipeline ttl
				for _, key := range keys {
					cr.sourceConn[idx].Send("PTTL", key)
				}
				pttls, err := redis.Int64s(cr.sourceConn[idx].Do(""))
				if err != nil && err != redis.ErrNil {
					log.Panicf("do ttl with failed[%v]", err)
				}

				for i, k := range keys {
					cr.keyChan <- &KeyNode{k, dumps[i], pttls[i]}
				}
			}

			// Last iteration of scan.
			if cr.scanners[idx].EndNode() {
				break
			}
		}
	}

	cr.fetcherWg.Done()
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
