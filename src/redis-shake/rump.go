package run

import (
	"fmt"
	"math"
	"reflect"
	"strconv"
	"sync"

	"pkg/libs/atomic2"
	"pkg/libs/log"
	"redis-shake/common"
	"redis-shake/configure"
	"redis-shake/metric"
	"redis-shake/scanner"
	"redis-shake/filter"

	"github.com/garyburd/redigo/redis"
	"time"
	"bytes"
)

type CmdRump struct {
	dumpers []*dbRumper
}

func (cr *CmdRump) GetDetailedInfo() interface{} {
	ret := make(map[string]interface{}, len(cr.dumpers))
	for _, dumper := range cr.dumpers {
		if dumper == nil {
			continue
		}
		ret[dumper.address] = dumper.getStats()
	}

	// TODO, better to move to the next level
	metric.AddMetric(0)

	return []map[string]interface{}{
		{
			"Details": ret,
		},
	}
}

func (cr *CmdRump) Main() {
	cr.dumpers = make([]*dbRumper, len(conf.Options.SourceAddressList))

	var wg sync.WaitGroup
	wg.Add(len(conf.Options.SourceAddressList))
	// build dbRumper
	for i, address := range conf.Options.SourceAddressList {
		dr := &dbRumper{
			id:      i,
			address: address,
		}

		cr.dumpers[i] = dr

		log.Infof("start dbRumper[%v]", i)
		go func() {
			defer wg.Done()
			dr.run()
		}()
	}
	wg.Wait()

	log.Infof("all rumpers finish!, total data: %v", cr.GetDetailedInfo())
}

/*------------------------------------------------------*/
// one rump(1 db or 1 proxy) link corresponding to one dbRumper
type dbRumper struct {
	id      int    // id
	address string // source address

	client       redis.Conn // source client
	tencentNodes []string   // for tencent cluster only

	executors []*dbRumperExecutor
}

func (dr *dbRumper) getStats() map[string]interface{} {
	ret := make(map[string]interface{}, len(dr.executors))
	for _, exe := range dr.executors {
		if exe == nil {
			continue
		}

		id := fmt.Sprintf("%v", exe.executorId)
		ret[id] = exe.getStats()
	}

	return ret
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

	dr.executors = make([]*dbRumperExecutor, count)

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

		sourceClient := utils.OpenRedisConn([]string{dr.address}, conf.Options.SourceAuthType,
			conf.Options.SourcePasswordRaw, false, conf.Options.SourceTLSEnable)
		targetClient := utils.OpenRedisConn(target, conf.Options.TargetAuthType,
			conf.Options.TargetPasswordRaw, conf.Options.TargetType == conf.RedisTypeCluster,
			conf.Options.TargetTLSEnable)
		targetBigKeyClient := utils.OpenRedisConn(target, conf.Options.TargetAuthType,
			conf.Options.TargetPasswordRaw, conf.Options.TargetType == conf.RedisTypeCluster,
			conf.Options.TargetTLSEnable)
		executor := NewDbRumperExecutor(dr.id, i, sourceClient, targetClient, targetBigKeyClient, tencentNodeId)
		dr.executors[i] = executor

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
	rumperId           int        // father id
	executorId         int        // current id, also == aliyun cluster node id
	sourceClient       redis.Conn // source client
	targetClient       redis.Conn // target client
	tencentNodeId      string     // tencent cluster node id
	targetBigKeyClient redis.Conn // target client only used in big key, this is a bit ugly
	previousDb         int        // store previous db

	keyChan    chan *KeyNode // keyChan is used to communicated between routine1 and routine2
	resultChan chan *KeyNode // resultChan is used to communicated between routine2 and routine3

	scanner scanner.Scanner // one scanner match one db/proxy

	fetcherWg sync.WaitGroup
	stat      dbRumperExexutorStats

	dbList    []int32 // db list
	keyNumber int64   // key in this db number
	close     bool    // is finish?
}

func NewDbRumperExecutor(rumperId, executorId int, sourceClient, targetClient, targetBigKeyClient redis.Conn,
	tencentNodeId string) *dbRumperExecutor {
	executor := &dbRumperExecutor{
		rumperId:           rumperId,
		executorId:         executorId,
		sourceClient:       sourceClient,
		targetClient:       targetClient,
		tencentNodeId:      tencentNodeId,
		targetBigKeyClient: targetBigKeyClient,
		previousDb:         0,
		stat: dbRumperExexutorStats{
			minSize: 1 << 30,
			maxSize: 0,
			sumSize: 0,
		},
	}

	return executor
}

type KeyNode struct {
	key   string
	value string
	pttl  int64
	db    int
}

type dbRumperExexutorStats struct {
	rBytes    atomic2.Int64 // read bytes
	rCommands atomic2.Int64 // read commands
	wBytes    atomic2.Int64 // write bytes
	wCommands atomic2.Int64 // write commands
	cCommands atomic2.Int64 // confirmed commands
	minSize   int64         // min package size
	maxSize   int64         // max package size
	sumSize   int64         // total package size
}

func (dre *dbRumperExecutor) getStats() map[string]interface{} {
	kv := make(map[string]interface{})
	// stats -> map
	v := reflect.ValueOf(dre.stat)
	for i := 0; i < v.NumField(); i++ {
		f := v.Field(i)
		name := v.Type().Field(i).Name
		switch f.Kind() {
		case reflect.Struct:
			// todo
			kv[name] = f.Field(0).Int()
			// kv[name] = f.Interface()
		case reflect.Int64:
			if name == "sumSize" {
				continue
			}
			kv[name] = f.Int()
		}
	}

	kv["keyChan"] = len(dre.keyChan)
	kv["resultChan"] = len(dre.resultChan)
	kv["avgSize"] = float64(dre.stat.sumSize) / float64(dre.stat.rCommands.Get())

	return kv
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

	// fetch db number from 'info keyspace'
	var err error
	dre.dbList, dre.keyNumber, err = dre.getSourceDbList()
	if err != nil {
		log.Panic(err)
	}

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
	go dre.receiver()

	// start metric
	for range time.NewTicker(1 * time.Second).C {
		if dre.close {
			break
		}

		var b bytes.Buffer
		fmt.Fprintf(&b, "dbRumper[%v] total = %v(keys) - %10v(keys) [%3d%%]  entry=%-12d",
			dre.rumperId, dre.keyNumber, dre.stat.cCommands.Get(),
			100 * dre.stat.cCommands.Get() / dre.keyNumber, dre.stat.wCommands.Get())
		log.Info(b.String())
	}

	log.Infof("dbRumper[%v] executor[%v] finish!", dre.rumperId, dre.executorId)
}

func (dre *dbRumperExecutor) fetcher() {
	log.Infof("dbRumper[%v] executor[%v] start fetcher with special-cloud[%v]", dre.rumperId, dre.executorId,
		conf.Options.ScanSpecialCloud)

	log.Infof("dbRumper[%v] executor[%v] fetch db list: %v", dre.rumperId, dre.executorId, dre.dbList)
	// iterate all db nodes
	for _, db := range dre.dbList {
		if filter.FilterDB(int(db)) {
			log.Infof("dbRumper[%v] executor[%v] db[%v] filtered", dre.rumperId, dre.executorId, db)
			continue
		}

		log.Infof("dbRumper[%v] executor[%v] fetch logical db: %v", dre.rumperId, dre.executorId, db)
		if err := dre.doFetch(int(db)); err != nil {
			log.Panic(err)
		}
	}

	close(dre.keyChan)
}

func (dre *dbRumperExecutor) writer() {
	var count uint32
	var wBytes int64
	var err error
	batch := make([]*KeyNode, 0, conf.Options.ScanKeyNumber)

	// used in QoS
	bucket := utils.StartQoS(conf.Options.Qps)
	preDb := 0
	preBigKeyDb := 0
	for ele := range dre.keyChan {
		/*if filter.FilterKey(ele.key) {
			continue
		}*/
		// QoS, limit the qps
		<-bucket

		if ele.pttl == -1 { // not set ttl
			ele.pttl = 0
		}
		if ele.pttl == -2 {
			log.Debugf("dbRumper[%v] executor[%v] skip key %s for expired", dre.rumperId, dre.executorId, ele.key)
			continue
		}
		if conf.Options.TargetDB != -1 {
			ele.db = conf.Options.TargetDB
		}

		log.Debugf("dbRumper[%v] executor[%v] restore[%s], length[%v]", dre.rumperId, dre.executorId, ele.key,
			len(ele.value))
		if uint64(len(ele.value)) >= conf.Options.BigKeyThreshold {
			log.Infof("dbRumper[%v] executor[%v] restore big key[%v] with length[%v], pttl[%v], db[%v]",
				dre.rumperId, dre.executorId, ele.key, len(ele.value), ele.pttl, ele.db)
			// flush previous cache
			batch = dre.writeSend(batch, &count, &wBytes)

			// handle big key
			utils.RestoreBigkey(dre.targetBigKeyClient, ele.key, ele.value, ele.pttl, ele.db, &preBigKeyDb)
			// all the reply has been handled in RestoreBigkey
			// dre.resultChan <- ele
			continue
		}

		// send "select" command if db is different
		if ele.db != preDb {
			dre.targetClient.Send("select", ele.db)
			preDb = ele.db
		}

		if conf.Options.KeyExists == "rewrite" {
			err = dre.targetClient.Send("RESTORE", ele.key, ele.pttl, ele.value, "REPLACE")
		} else {
			err = dre.targetClient.Send("RESTORE", ele.key, ele.pttl, ele.value)
		}
		if err != nil {
			log.Panicf("dbRumper[%v] executor[%v] send key[%v] failed[%v]", dre.rumperId, dre.executorId,
				ele.key, err)
		}

		wBytes += int64(len(ele.value))
		batch = append(batch, ele)
		// move to real send
		// dre.resultChan <- ele
		count++

		if count >= conf.Options.ScanKeyNumber {
			// batch
			log.Debugf("dbRumper[%v] executor[%v] send keys %d", dre.rumperId, dre.executorId, count)

			batch = dre.writeSend(batch, &count, &wBytes)
		}
	}
	dre.writeSend(batch, &count, &wBytes)

	close(dre.resultChan)
}

func (dre *dbRumperExecutor) writeSend(batch []*KeyNode, count *uint32, wBytes *int64) []*KeyNode {
	newBatch := make([]*KeyNode, 0, conf.Options.ScanKeyNumber)
	if len(batch) == 0 {
		return newBatch
	}

	if err := dre.targetClient.Flush(); err != nil {
		log.Panicf("dbRumper[%v] executor[%v] flush failed[%v]", dre.rumperId, dre.executorId, err)
	}

	// real send
	for _, ele := range batch {
		dre.resultChan <- ele
	}

	dre.stat.wCommands.Add(int64(*count))
	dre.stat.wBytes.Add(*wBytes)

	*count = 0
	*wBytes = 0

	return newBatch
}

func (dre *dbRumperExecutor) receiver() {
	for ele := range dre.resultChan {
		if _, err := dre.targetClient.Receive(); err != nil && err != redis.ErrNil {
			rdbVersion, checksum, checkErr := utils.CheckVersionChecksum(utils.String2Bytes(ele.value))
			log.Panicf("dbRumper[%v] executor[%v] restore key[%v] error[%v]: pttl[%v], value length[%v], "+
				"rdb version[%v], checksum[%v], check error[%v]",
				dre.rumperId, dre.executorId, ele.key, err, strconv.FormatInt(ele.pttl, 10), len(ele.value),
				rdbVersion, checksum, checkErr)
		}
		dre.stat.cCommands.Incr()
	}

	dre.close = true
}

func (dre *dbRumperExecutor) getSourceDbList() ([]int32, int64, error) {
	// tencent cluster only has 1 logical db
	if conf.Options.ScanSpecialCloud == utils.TencentCluster {
		return []int32{0}, 1, nil
	}

	conn := dre.sourceClient
	if ret, err := conn.Do("info", "keyspace"); err != nil {
		return nil, 0, err
	} else if mp, err := utils.ParseKeyspace(ret.([]byte)); err != nil {
		return nil, 0, err
	} else {
		list := make([]int32, 0, len(mp))
		var total int64
		for db, number := range mp {
			if number > 0 && !filter.FilterDB(int(db)) {
				list = append(list, db)
				total += number
			}
		}
		return list, total, nil
	}
}

func (dre *dbRumperExecutor) doFetch(db int) error {
	// some redis type only has db0, so we add this judge
	if db != dre.previousDb {
		dre.previousDb = db
		// send 'select' command to both source and target
		log.Debugf("dbRumper[%v] executor[%v] send source select db", dre.rumperId, dre.executorId)
		if _, err := dre.sourceClient.Do("select", db); err != nil {
			return err
		}
	}

	// selecting target db is moving into writer

	log.Infof("dbRumper[%v] executor[%v] start fetching node db[%v]", dre.rumperId, dre.executorId, db)

	for {
		rawKeys, err := dre.scanner.ScanKey()
		if err != nil {
			return err
		}

		var keys []string
		if len(conf.Options.FilterKeyBlacklist) != 0 || len(conf.Options.FilterKeyWhitelist) != 0 {
			// filter keys
			keys = make([]string, 0, len(rawKeys))
			for _, key := range rawKeys {
				if filter.FilterKey(key) {
					log.Infof("dbRumper[%v] executor[%v] key[%v] filter", dre.rumperId, dre.executorId, key)
					continue
				}
				keys = append(keys, key)
			}
		} else {
			keys = rawKeys
		}

		log.Debugf("dbRumper[%v] executor[%v] scanned keys number: %v", dre.rumperId, dre.executorId, len(keys))

		if len(keys) != 0 {
			// pipeline dump
			for _, key := range keys {
				log.Debugf("dbRumper[%v] executor[%v] scan key: %v", dre.rumperId, dre.executorId, key)
				dre.sourceClient.Send("DUMP", key)
			}

			reply, err := dre.sourceClient.Do("")
			dumps, err := redis.Strings(reply, err)
			if err != nil && err != redis.ErrNil {
				return fmt.Errorf("do dump with failed[%v], reply[%v]", err, reply)
			}

			// pipeline ttl
			for _, key := range keys {
				dre.sourceClient.Send("PTTL", key)
			}
			reply, err = dre.sourceClient.Do("")
			pttls, err := redis.Int64s(reply, err)
			if err != nil && err != redis.ErrNil {
				return fmt.Errorf("do ttl with failed[%v], reply[%v]", err, reply)
			}

			dre.stat.rCommands.Add(int64(len(keys)))
			for i, k := range keys {
				length := len(dumps[i])
				dre.stat.rBytes.Add(int64(length)) // length of value
				dre.stat.minSize = int64(math.Min(float64(dre.stat.minSize), float64(length)))
				dre.stat.maxSize = int64(math.Max(float64(dre.stat.maxSize), float64(length)))
				dre.stat.sumSize += int64(length)
				dre.keyChan <- &KeyNode{k, dumps[i], pttls[i], db}
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
