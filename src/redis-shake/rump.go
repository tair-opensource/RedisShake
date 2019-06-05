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
}

func (cr *CmdRump) GetDetailedInfo() interface{} {
	return nil
}

func (cr *CmdRump) Main() {
	var wg sync.WaitGroup
	wg.Add(len(conf.Options.SourceAddressList))
	// build dbRumper
	for i, address := range conf.Options.SourceAddressList {
		dr := &dbRumper{
			id:      i,
			address: address,
		}
		log.Infof("start dbRumper[%v]", i)
		go func() {
			defer wg.Done()
			dr.run()
		}()
	}
	wg.Wait()

	log.Info("all rumpers finish!")
}

/*------------------------------------------------------*/
// one rump(1 db or 1 proxy) link corresponding to one dbRumper
type dbRumper struct {
	id      int // id
	address string

	client       redis.Conn // source client
	tencentNodes []string   // for tencent cluster only
}

func (dr *dbRumper) run() {
	// single connection
	dr.client = utils.OpenRedisConn([]string{dr.address}, conf.Options.SourceAuthType,
			conf.Options.SourcePasswordRaw, false, conf.Options.SourceTLSEnable)

	// some clouds may have several db under proxy
	count, err := dr.getNode()
	if err != nil {
		log.Panicf("dbRumper[%v] get node failed[%v]", dr.id, err)
	}

	log.Infof("dbRumper[%v] get node count: %v", dr.id, count)

	var wg sync.WaitGroup
	wg.Add(count)
	for i := 0; i < count; i++ {
		var target []string
		if conf.Options.TargetType == conf.RedisTypeCluster {
			target = conf.Options.TargetAddressList
		} else {
			// round-robin pick
			pick := utils.PickTargetRoundRobin(len(conf.Options.TargetAddressList))
			target = []string{conf.Options.TargetAddressList[pick]}
		}

		var tencentNodeId string
		if len(dr.tencentNodes) > 0 {
			tencentNodeId = dr.tencentNodes[i]
		}

		executor := &dbRumperExecutor{
			rumperId:   dr.id,
			executorId: i,
			sourceClient: utils.OpenRedisConn([]string{dr.address}, conf.Options.SourceAuthType,
				conf.Options.SourcePasswordRaw, false, conf.Options.SourceTLSEnable),
			targetClient: utils.OpenRedisConn(target, conf.Options.TargetAuthType,
				conf.Options.TargetPasswordRaw, conf.Options.TargetType == conf.RedisTypeCluster,
				conf.Options.TargetTLSEnable),
			tencentNodeId: tencentNodeId,
		}

		go func() {
			defer wg.Done()
			executor.exec()
		}()
	}

	wg.Wait()

	log.Infof("dbRumper[%v] finished!", dr.id)
}

func (dr *dbRumper) getNode() (int, error) {
	switch conf.Options.ScanSpecialCloud {
	case utils.AliyunCluster:
		info, err := redis.Bytes(dr.client.Do("info", "Cluster"))
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
		dr.tencentNodes, err = utils.GetAllClusterNode(dr.client, conf.StandAloneRoleMaster, "id")
		if err != nil {
			return -1, err
		}

		return len(dr.tencentNodes), nil
	default:
		return 1, nil
	}
}

/*------------------------------------------------------*/
// one executor(1 db only) link corresponding to one dbRumperExecutor
type dbRumperExecutor struct {
	rumperId      int // father id
	executorId    int // current id, also == aliyun cluster node id
	sourceClient  redis.Conn
	targetClient  redis.Conn
	tencentNodeId string // tencent cluster node id

	keyChan    chan *KeyNode // keyChan is used to communicated between routine1 and routine2
	resultChan chan *KeyNode // resultChan is used to communicated between routine2 and routine3

	scanner   scanner.Scanner // one scanner match one db/proxy
	fetcherWg sync.WaitGroup
}

type KeyNode struct {
	key   string
	value string
	pttl  int64
}

func (dre *dbRumperExecutor) exec() {
	// create scanner
	dre.scanner = scanner.NewScanner(dre.sourceClient, dre.tencentNodeId, dre.executorId)
	if dre.scanner == nil {
		log.Panicf("dbRumper[%v] executor[%v] create scanner failed", dre.rumperId, dre.executorId)
		return
	}

	// init two channels
	chanSize := int(conf.Options.ScanKeyNumber * 2)
	dre.keyChan = make(chan *KeyNode, chanSize)
	dre.resultChan = make(chan *KeyNode, chanSize)

	/*
	 * we start 4 routines to run:
	 * 1. fetch keys from the source redis
	 * 2. write keys into the target redis
	 * 3. read result from the target redis
	 */
	// routine1
	go dre.fetcher()

	// routine3
	go dre.writer()

	// routine4
	dre.receiver()

	log.Infof("dbRumper[%v] executor[%v] finish!", dre.rumperId, dre.executorId)
}

func (dre *dbRumperExecutor) fetcher() {
	log.Infof("dbRumper[%v] executor[%v] start fetcher with special-cloud[%v]", dre.rumperId, dre.executorId,
		conf.Options.ScanSpecialCloud)

	// fetch db number from 'info keyspace'
	dbNumber, err := dre.getSourceDbList()
	if err != nil {
		log.Panic(err)
	}

	log.Infof("dbRumper[%v] executor[%v] fetch db list: %v", dre.rumperId, dre.executorId, dbNumber)
	// iterate all db
	for _, db := range dbNumber {
		log.Infof("dbRumper[%v] executor[%v] fetch logical db: %v", dre.rumperId, dre.executorId, db)
		if err := dre.doFetch(int(db)); err != nil {
			log.Panic(err)
		}
	}

	close(dre.keyChan)
}

func (dre *dbRumperExecutor) writer() {
	var count uint32
	for ele := range dre.keyChan {
		if ele.pttl == -1 { // not set ttl
			ele.pttl = 0
		}
		if ele.pttl == -2 {
			log.Debugf("dbRumper[%v] executor[%v] skip key %s for expired", dre.rumperId, dre.executorId, ele.key)
			continue
		}

		// TODO, big key split
		log.Debugf("dbRumper[%v] executor[%v] restore %s", dre.rumperId, dre.executorId, ele.key)
		if conf.Options.Rewrite {
			dre.targetClient.Send("RESTORE", ele.key, ele.pttl, ele.value, "REPLACE")
		} else {
			dre.targetClient.Send("RESTORE", ele.key, ele.pttl, ele.value)
		}

		dre.resultChan <- ele
		count++
		if count == conf.Options.ScanKeyNumber {
			// batch
			log.Debugf("dbRumper[%v] executor[%v] send keys %d", dre.rumperId, dre.executorId, count)
			dre.targetClient.Flush()
			count = 0
		}
	}
	dre.targetClient.Flush()
	close(dre.resultChan)
}

func (dre *dbRumperExecutor) receiver() {
	for ele := range dre.resultChan {
		if _, err := dre.targetClient.Receive(); err != nil && err != redis.ErrNil {
			log.Panicf("dbRumper[%v] executor[%v] restore key[%v] with pttl[%v] error[%v]", dre.rumperId,
				dre.executorId, ele.key, strconv.FormatInt(ele.pttl, 10), err)
		}
	}
}

func (dre *dbRumperExecutor) getSourceDbList() ([]int32, error) {
	conn := dre.sourceClient
	if ret, err := conn.Do("info", "keyspace"); err != nil {
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

func (dre *dbRumperExecutor) doFetch(db int) error {
	// send 'select' command to both source and target
	log.Infof("dbRumper[%v] executor[%v] send source select db", dre.rumperId, dre.executorId)
	if _, err := dre.sourceClient.Do("select", db); err != nil {
		return err
	}

	// it's ok to send select directly because the message order can be guaranteed.
	log.Infof("dbRumper[%v] executor[%v] send target select db", dre.rumperId, dre.executorId)
	dre.targetClient.Flush()
	if err := dre.targetClient.Send("select", db); err != nil {
		return err
	}
	dre.targetClient.Flush()

	log.Infof("dbRumper[%v] executor[%v] start fetching node db[%v]", dre.rumperId, dre.executorId, db)

	for {
		keys, err := dre.scanner.ScanKey()
		if err != nil {
			return err
		}

		log.Infof("dbRumper[%v] executor[%v] scanned keys number: %v", dre.rumperId, dre.executorId, len(keys))

		if len(keys) != 0 {
			// pipeline dump
			for _, key := range keys {
				log.Debug("dbRumper[%v] executor[%v] scan key: %v", dre.rumperId, dre.executorId, key)
				dre.sourceClient.Send("DUMP", key)
			}
			dumps, err := redis.Strings(dre.sourceClient.Do(""))
			if err != nil && err != redis.ErrNil {
				return fmt.Errorf("do dump with failed[%v]", err)
			}

			// pipeline ttl
			for _, key := range keys {
				dre.sourceClient.Send("PTTL", key)
			}
			pttls, err := redis.Int64s(dre.sourceClient.Do(""))
			if err != nil && err != redis.ErrNil {
				return fmt.Errorf("do ttl with failed[%v]", err)
			}

			for i, k := range keys {
				dre.keyChan <- &KeyNode{k, dumps[i], pttls[i]}
			}
		}

		// Last iteration of scan.
		if dre.scanner.EndNode() {
			break
		}
	}

	log.Infof("dbRumper[%v] executor[%v] finish fetching db[%v]", dre.rumperId, dre.executorId, db)

	return nil
}