package dbSync

import (
	"bufio"
	"redis-shake/configure"
	"redis-shake/common"
	"pkg/libs/atomic2"
	"pkg/libs/log"
	"time"
	"pkg/redis"
	"strings"
	"strconv"
	"redis-shake/filter"
	"bytes"
	"fmt"
	"io"
	"net"

	"redis-shake/metric"

	redigo "github.com/garyburd/redigo/redis"
	"unsafe"
)

func (ds *DbSyncer) syncCommand(reader *bufio.Reader, target []string, authType, passwd string, tlsEnable bool, dbid int) {
	isCluster := conf.Options.TargetType == conf.RedisTypeCluster
	c := utils.OpenRedisConnWithTimeout(target, authType, passwd, incrSyncReadeTimeout, incrSyncReadeTimeout, isCluster, tlsEnable)
	defer c.Close()

	ds.sendBuf = make(chan cmdDetail, conf.Options.SenderCount)
	ds.delayChannel = make(chan *delayNode, conf.Options.SenderDelayChannelSize)

	// fetch source redis offset
	go ds.fetchOffset()

	// receiver target reply
	go ds.receiveTargetReply(c)

	// parse command from source redis
	go ds.parseSourceCommand(reader)

	// do send to target
	go ds.sendTargetCommand(c)

	// print stat
	for lStat := ds.stat.Stat(); ; {
		time.Sleep(time.Second)
		nStat := ds.stat.Stat()
		var b bytes.Buffer
		fmt.Fprintf(&b, "DbSyncer[%d] sync: ", ds.id)
		fmt.Fprintf(&b, " +forwardCommands=%-6d", nStat.wCommands - lStat.wCommands)
		fmt.Fprintf(&b, " +filterCommands=%-6d", nStat.incrSyncFilter - lStat.incrSyncFilter)
		fmt.Fprintf(&b, " +writeBytes=%d", nStat.wBytes - lStat.wBytes)
		log.Info(b.String())
		lStat = nStat
	}
}

func (ds *DbSyncer) fetchOffset() {
	if conf.Options.Psync == false {
		log.Warnf("DbSyncer[%d] GetFakeSlaveOffset not enable when psync == false", ds.id)
		return
	}

	srcConn := utils.OpenRedisConnWithTimeout([]string{ds.source}, conf.Options.SourceAuthType, ds.sourcePassword,
		incrSyncReadeTimeout, incrSyncReadeTimeout, false, conf.Options.SourceTLSEnable)
	ticker := time.NewTicker(10 * time.Second)
	for range ticker.C {
		offset, err := utils.GetFakeSlaveOffset(srcConn)
		if err != nil {
			// log.PurePrintf("%s\n", NewLogItem("GetFakeSlaveOffsetFail", "WARN", NewErrorLogDetail("", err.Error())))
			log.Warnf("DbSyncer[%d] Event:GetFakeSlaveOffsetFail\tId:%s\tWarn:%s",
				ds.id, conf.Options.Id, err.Error())

			// Reconnect while network error happen
			if err == io.EOF {
				srcConn = utils.OpenRedisConnWithTimeout([]string{ds.source}, conf.Options.SourceAuthType,
					ds.sourcePassword, incrSyncReadeTimeout, incrSyncReadeTimeout, false, conf.Options.SourceTLSEnable)
			} else if _, ok := err.(net.Error); ok {
				srcConn = utils.OpenRedisConnWithTimeout([]string{ds.source}, conf.Options.SourceAuthType,
					ds.sourcePassword, incrSyncReadeTimeout, incrSyncReadeTimeout, false, conf.Options.SourceTLSEnable)
			}
		} else {
			// ds.SyncStat.SetOffset(offset)
			if ds.stat.sourceOffset, err = strconv.ParseInt(offset, 10, 64); err != nil {
				log.Errorf("DbSyncer[%d] Event:GetFakeSlaveOffsetFail\tId:%s\tError:%s",
					ds.id, conf.Options.Id, err.Error())
			}
		}
	}

	log.Panicf("DbSyncer[%d] something wrong if you see me", ds.id)
}

func (ds *DbSyncer) receiveTargetReply(c redigo.Conn) {
	var node *delayNode
	var recvId atomic2.Int64

	for {
		reply, err := c.Receive()

		recvId.Incr()
		id := recvId.Get() // receive id

		// print debug log of receive reply
		log.Debugf("DbSyncer[%d] receive reply-id[%v]: [%v], error:[%v]", ds.id, id, reply, err)

		if conf.Options.Metric == false {
			continue
		}

		if err == nil {
			metric.GetMetric(ds.id).AddSuccessCmdCount(ds.id, 1)
		} else {
			metric.GetMetric(ds.id).AddFailCmdCount(ds.id, 1)
			if utils.CheckHandleNetError(err) {
				log.Panicf("DbSyncer[%d] Event:NetErrorWhileReceive\tId:%s\tError:%s",
					ds.id, conf.Options.Id, err.Error())
			} else {
				log.Panicf("DbSyncer[%d] Event:ErrorReply\tId:%s\tCommand: [unknown]\tError: %s",
					ds.id, conf.Options.Id, err.Error())
			}
		}

		if node == nil {
			// non-blocking read from delay channel
			select {
			case node = <-ds.delayChannel:
			default:
				// it's ok, channel is empty
			}
		}

		// TODO, how to calculate the delay in transaction mode?
		/*if node != nil {
			if node.id == id {
				metric.GetMetric(ds.id).AddDelay(uint64(time.Now().Sub(node.t).Nanoseconds()) / 1000000) // ms
				node = nil
			} else if node.id < id {
				log.Panicf("DbSyncer[%d] receive id invalid: node-id[%v] < receive-id[%v]",
					ds.id, node.id, id)
			}
		}*/
	}

	log.Panicf("DbSyncer[%d] something wrong if you see me", ds.id)
}

func (ds *DbSyncer) parseSourceCommand(reader *bufio.Reader) {
	var (
		lastDb              = -1
		bypass              = false
		isSelect            = false
		sCmd          string
		argv, newArgv [][]byte
		err           error
		reject        bool
		// sendMarkId atomic2.Int64 // sendMarkId is also used as mark the sendId in sender routine
	)

	// if the start db id != 0, send dbid to the target at first
	if ds.startDbId != 0 {
		log.Infof("last dbid[%v] != 0, send 'select' first", ds.startDbId)
		dbS := fmt.Sprintf("%d", ds.startDbId)
		ds.sendBuf <- cmdDetail{
			Cmd:    "select",
			Args:   []interface{}{utils.String2Bytes(dbS)},
			Offset: ds.fullSyncOffset,
			Db:     ds.startDbId,
		}
	}

	decoder := redis.NewDecoder(reader)

	log.Infof("DbSyncer[%d] FlushEvent:IncrSyncStart\tId:%s\t", ds.id, conf.Options.Id)

	for {
		ignoresentinel:= false
		ignoreCmd := false
		isSelect = false
		// incrOffset is used to do resume from break-point job
		resp, incrOffset := redis.MustDecodeOpt(decoder)

		if sCmd, argv, err = redis.ParseArgs(resp); err != nil {
			log.PanicErrorf(err, "DbSyncer[%d] parse command arguments failed[%v]", ds.id, err)
		} else {
			metric.GetMetric(ds.id).AddPullCmdCount(ds.id, 1)

			if sCmd != "ping" {
				if strings.EqualFold(sCmd, "select") {
					if len(argv) != 1 {
						log.Panicf("DbSyncer[%d] select command len(args) = %d", ds.id, len(argv))
					}
					s := string(argv[0])
					n, err := strconv.Atoi(s)
					if err != nil {
						log.PanicErrorf(err, "DbSyncer[%d] parse db = %s failed", ds.id, s)
					}
					bypass = filter.FilterDB(n)
					isSelect = true
					lastDb = n
				} else if filter.FilterCommands(sCmd) {
					ignoreCmd = true
				} else if strings.EqualFold(sCmd, "publish") && strings.EqualFold(string(argv[0]), "__sentinel__:hello"){
					ignoresentinel = true
				}

				if bypass || ignoreCmd || ignoresentinel {
					ds.stat.incrSyncFilter.Incr()
					// ds.SyncStat.BypassCmdCount.Incr()
					metric.GetMetric(ds.id).AddBypassCmdCount(ds.id, 1)
					log.Debugf("DbSyncer[%d] ignore command[%v]", ds.id, sCmd)
					continue
				}
			}

			newArgv, reject = filter.HandleFilterKeyWithCommand(sCmd, argv)
			if bypass || ignoreCmd || reject {
				ds.stat.incrSyncFilter.Incr()
				metric.GetMetric(ds.id).AddBypassCmdCount(ds.id, 1)
				log.Debugf("DbSyncer[%d] filter command[%v]", ds.id, sCmd)
				continue
			}
		}

		if isSelect && conf.Options.TargetDB != -1 {
			if conf.Options.TargetDB != lastDb {
				lastDb = conf.Options.TargetDB
				/* send select command. */
				ds.sendBuf <- cmdDetail{
					Cmd:    "SELECT",
					Args:   []interface{}{[]byte(strconv.FormatInt(int64(lastDb), 10))},
					Offset: ds.fullSyncOffset + incrOffset,
					Db:     lastDb,
				}
			} else {
				ds.stat.incrSyncFilter.Incr()
				metric.GetMetric(ds.id).AddBypassCmdCount(ds.id, 1)
			}
			continue
		}

		data := make([]interface{}, 0, len(newArgv))
		for _, item := range newArgv {
			data = append(data, item)
		}
		ds.sendBuf <- cmdDetail{
			Cmd:    sCmd,
			Args:   data,
			Offset: ds.fullSyncOffset + incrOffset,
			Db:     lastDb,
		}
	}

	log.Panicf("DbSyncer[%d] something wrong if you see me", ds.id)
}

func (ds *DbSyncer) sendTargetCommand(c redigo.Conn) {
	var cachedCount uint
	var cachedSize uint64
	var sendId atomic2.Int64
	var bs string       // barrier status
	var flushStatus int // need a barrier?

	// cache the batch oplog
	cachedTunnel := make([]cmdDetail, 0, conf.Options.SenderCount + 1)
	checkpointRunId := fmt.Sprintf("%s-%s", ds.source, utils.CheckpointRunId)
	checkpointVersion := fmt.Sprintf("%s-%s", ds.source, utils.CheckpointVersion)
	checkpointOffset := fmt.Sprintf("%s-%s", ds.source, utils.CheckpointOffset)
	ticker := time.NewTicker(500 * time.Millisecond)
	// mark whether the given db has already send runId, no need to send run-id each time.
	runIdMap := make(map[int]struct{})

	// do send
	sendFunc := func() {
		length := len(cachedTunnel)
		if length == 0 {
			// do nothing
			return
		}

		lastOplog := cachedTunnel[len(cachedTunnel) - 1]
		needBatch := true
		if !ds.enableResumeFromBreakPoint || (cachedCount == 1 && lastOplog.Cmd == "ping") {
			needBatch = false
		}

		var offset int64
		// enable resume from break point
		if needBatch {
			ds.addSendId(&sendId, 1)

			// the last offset
			offset = lastOplog.Offset
			if err := c.Send("multi"); err != nil {
				log.Panicf("DbSyncer[%d] Event:SendToTargetFail\tId:%s\tError:%s\t",
					ds.id, conf.Options.Id, err.Error())
			}
		}

		ds.addSendId(&sendId, len(cachedTunnel))
		for _, cacheItem := range cachedTunnel {
			if err := c.Send(cacheItem.Cmd, cacheItem.Args...); err != nil {
				log.Panicf("DbSyncer[%d] Event:SendToTargetFail\tId:%s\tError:%s\t",
					ds.id, conf.Options.Id, err.Error())
			}

			// print debug log of send command
			if conf.Options.LogLevel == utils.LogLevelDebug {
				strArgv := make([]string, len(cacheItem.Args))
				for i, ele := range cacheItem.Args {
					eleB := ele.([]byte)
					strArgv[i] = *(*string)(unsafe.Pointer(&eleB))
					// strArgv[i] = string(ele.([]byte))
				}
				log.Debugf("DbSyncer[%d] send command[%v]: [%s %v]", ds.id, sendId.Get(), cacheItem.Cmd,
					strArgv)
			}
		}

		if needBatch {
			// need send run-id?
			if _, ok := runIdMap[lastOplog.Db]; !ok {
				runIdMap[lastOplog.Db] = struct{}{}
				ds.addSendId(&sendId, 2)
				// run id
				if err := c.Send("hset", ds.checkpointName, checkpointRunId, ds.runId); err != nil {
					log.Panicf("DbSyncer[%d] Event:SendToTargetFail\tId:%s\tError:%s\t",
						ds.id, conf.Options.Id, err.Error())
				}
				// version
				if err := c.Send("hset", ds.checkpointName, checkpointVersion, utils.FcvCheckpoint.CurrentVersion); err != nil {
					log.Panicf("DbSyncer[%d] Event:SendToTargetFail\tId:%s\tError:%s\t",
						ds.id, conf.Options.Id, err.Error())
				}
			}

			// add checkpoint
			ds.addSendId(&sendId, 2)
			if err := c.Send("hset", ds.checkpointName, checkpointOffset, offset); err != nil {
				log.Panicf("DbSyncer[%d] Event:SendToTargetFail\tId:%s\tError:%s\t",
					ds.id, conf.Options.Id, err.Error())
			}
			if err := c.Send("exec"); err != nil {
				log.Panicf("DbSyncer[%d] Event:SendToTargetFail\tId:%s\tError:%s\t",
					ds.id, conf.Options.Id, err.Error())
			}
		}

		if err := c.Flush(); err != nil {
			log.Panicf("DbSyncer[%d] Event:FlushFail\tId:%s\tError:%s\t",
				ds.id, conf.Options.Id, err.Error())
		}

		// clear
		cachedTunnel = cachedTunnel[:0]
		cachedCount = 0
		cachedSize = 0
	}

	for {
		select {
		case item := <-ds.sendBuf:
			length := len(item.Cmd)
			for i := range item.Args {
				length += len(item.Args[i].([]byte))
			}

			bs, flushStatus = barrierStatus(item.Cmd, bs)
			log.Debugf("DbSyncer[%d] command[%s] with barrier status[%v] and flush status[%v]",
				ds.id, item.Cmd, bs, flushStatus)
			if flushStatus == flushStatusYes {
				// flush previous data
				sendFunc()
				flushStatus = flushStatusNo
			}

			// remove command when bs == barrierStatusHoldStart or barrierStatusHoldEnd
			if bs != barrierStatusHoldStart && bs != barrierStatusHoldEnd {
				cachedTunnel = append(cachedTunnel, item)
				cachedCount++
				cachedSize += uint64(length)

				// update metric
				ds.stat.wCommands.Incr()
				ds.stat.wBytes.Add(int64(length))
				metric.GetMetric(ds.id).AddPushCmdCount(ds.id, 1)
				metric.GetMetric(ds.id).AddNetworkFlow(ds.id, uint64(length))
			}

		case <-ticker.C:
			if len(ds.sendBuf) == 0 && len(cachedTunnel) > 0 {
				flushStatus = flushStatusYes
			} else {
				flushStatus = flushStatusNo
			}
		}

		if cachedCount < conf.Options.SenderCount && cachedSize < conf.Options.SenderSize && flushStatus == flushStatusNo {
			// do not flush
			continue
		}

		// flush cache
		sendFunc()
	}

	log.Warnf("DbSyncer[%d] sender exit", ds.id)
}
