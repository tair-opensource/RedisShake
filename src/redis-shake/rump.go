package run

import (
	"pkg/libs/log"
	"strconv"
	"sync"
	"fmt"

	"redis-shake/common"
	"redis-shake/configure"
	"redis-shake/scanner"

	"github.com/garyburd/redigo/redis"
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

	cr.sourceConn = make([]redis.Conn, len(conf.Options.SourceAddressList))
	for i, address := range conf.Options.SourceAddressList {
		cr.sourceConn[i] = utils.OpenRedisConn([]string{address}, conf.Options.SourceAuthType,
		conf.Options.SourcePasswordRaw, false, conf.Options.SourceTLSEnable)
	}
	// TODO, current only support write data into 1 db or proxy
	cr.targetConn = utils.OpenRedisConn(conf.Options.TargetAddressList, conf.Options.TargetAuthType,
		conf.Options.TargetPasswordRaw, false, conf.Options.SourceTLSEnable)

	// init two channels
	chanSize := int(conf.Options.ScanKeyNumber) * len(conf.Options.SourceAddressList)
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

	log.Infof("start fetcher with special-cloud[%v], nodes[%v]", conf.Options.ScanSpecialCloud, length)

	// iterate all source nodes
	for i := 0; i < length; i++ {
		// fetch db number from 'info Keyspace'
		dbNumber, err := cr.getSourceDbList(i)
		if err != nil {
			log.Panic(err)
		}

		log.Infof("fetch node[%v] with db list: %v", i, dbNumber)
		// iterate all db
		for _, db := range dbNumber {
			log.Infof("fetch node[%v] db[%v]", i, db)
			if err := cr.doFetch(int(db), i); err != nil {
				log.Panic(err)
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

func (cr *CmdRump) getSourceDbList(id int) ([]int32, error) {
	conn := cr.sourceConn[id]
	if ret, err := conn.Do("info", "Keyspace"); err != nil {
		return nil, err
	} else if mp, err := utils.ParseKeyspace(ret.([]byte)); err != nil {
		return nil, err
	} else {
		list := make([]int32, 0, len(mp))
		for key, val := range mp {
			if val > 0 {
				list = append(list, key)
			}
		}
		return list, nil
	}
}

func (cr *CmdRump) doFetch(db, idx int) error {
	// send 'select' command to both source and target
	log.Infof("send source select db")
	if _, err := cr.sourceConn[idx].Do("select", db); err != nil {
		return err
	}

	log.Infof("send target select db")
	cr.targetConn.Flush()
	if err := cr.targetConn.Send("select", db); err != nil {
		return err
	}
	cr.targetConn.Flush()

	log.Infof("finish select db, start fetching node[%v] db[%v]", idx, db)

	for {
		keys, err := cr.scanners[idx].ScanKey(idx)
		if err != nil {
			return err
		}

		log.Info("scanned keys: ", len(keys))

		if len(keys) != 0 {
			// pipeline dump
			for _, key := range keys {
				log.Debug("scan key: ", key)
				cr.sourceConn[idx].Send("DUMP", key)
			}
			dumps, err := redis.Strings(cr.sourceConn[idx].Do(""))
			if err != nil && err != redis.ErrNil {
				return fmt.Errorf("do dump with failed[%v]", err)
			}

			// pipeline ttl
			for _, key := range keys {
				cr.sourceConn[idx].Send("PTTL", key)
			}
			pttls, err := redis.Int64s(cr.sourceConn[idx].Do(""))
			if err != nil && err != redis.ErrNil {
				return fmt.Errorf("do ttl with failed[%v]", err)
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

	log.Infof("finish fetching node[%v] db[%v]", idx, db)

	return nil
}