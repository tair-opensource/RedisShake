package dbSync

import (
	"bufio"
	"redis-shake/configure"
	"redis-shake/common"
	"pkg/libs/atomic2"
	"pkg/libs/log"
	"time"
	"pkg/redis"
	"unsafe"
	"strings"
	"strconv"
	"redis-shake/filter"
	"bytes"
	"fmt"
	"io"
	"net"

	"redis-shake/metric"
)

func (ds *DbSyncer) syncCommand(reader *bufio.Reader, target []string, authType, passwd string, tlsEnable bool) {
	isCluster := conf.Options.TargetType == conf.RedisTypeCluster
	c := utils.OpenRedisConnWithTimeout(target, authType, passwd, incrSyncReadeTimeout, incrSyncReadeTimeout, isCluster, tlsEnable)
	defer c.Close()

	ds.sendBuf = make(chan cmdDetail, conf.Options.SenderCount)
	ds.delayChannel = make(chan *delayNode, conf.Options.SenderDelayChannelSize)
	var sendId, recvId, sendMarkId atomic2.Int64 // sendMarkId is also used as mark the sendId in sender routine

	// fetch source redis offset
	go ds.fetchOffset()

	go func() {
		var node *delayNode
		for {
			reply, err := c.Receive()

			recvId.Incr()
			id := recvId.Get() // receive id

			// print debug log of receive reply
			log.Debugf("receive reply-id[%v]: [%v], error:[%v]", id, reply, err)

			if conf.Options.Metric == false {
				continue
			}

			if err == nil {
				metric.GetMetric(ds.id).AddSuccessCmdCount(ds.id, 1)
			} else {
				metric.GetMetric(ds.id).AddFailCmdCount(ds.id, 1)
				if utils.CheckHandleNetError(err) {
					log.Panicf("DbSyncer[%v] Event:NetErrorWhileReceive\tId:%s\tError:%s",
						ds.id, conf.Options.Id, err.Error())
				} else {
					log.Panicf("DbSyncer[%v] Event:ErrorReply\tId:%s\tCommand: [unknown]\tError: %s",
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

			if node != nil {
				if node.id == id {
					metric.GetMetric(ds.id).AddDelay(uint64(time.Now().Sub(node.t).Nanoseconds()) / 1000000) // ms
					node = nil
				} else if node.id < id {
					log.Panicf("DbSyncer[%v] receive id invalid: node-id[%v] < receive-id[%v]",
						ds.id, node.id, id)
				}
			}
		}
	}()

	go func() {
		var (
			lastdb        int32 = 0
			bypass              = false
			isselect            = false
			scmd          string
			argv, newArgv [][]byte
			err           error
			reject        bool
		)

		decoder := redis.NewDecoder(reader)

		log.Infof("DbSyncer[%v] Event:IncrSyncStart\tId:%s\t", ds.id, conf.Options.Id)

		for {
			ignorecmd := false
			isselect = false
			resp := redis.MustDecodeOpt(decoder)

			if scmd, argv, err = redis.ParseArgs(resp); err != nil {
				log.PanicErrorf(err, "DbSyncer[%v] parse command arguments failed", ds.id)
			} else {
				metric.GetMetric(ds.id).AddPullCmdCount(ds.id, 1)

				// print debug log of send command
				if conf.Options.LogLevel == utils.LogLevelDebug {
					strArgv := make([]string, len(argv))
					for i, ele := range argv {
						strArgv[i] = *(*string)(unsafe.Pointer(&ele))
					}
					sendMarkId.Incr()
					log.Debugf("DbSyncer[%v] send command[%v]: [%s %v]", ds.id, sendMarkId.Get(), scmd, strArgv)
				}

				if scmd != "ping" {
					if strings.EqualFold(scmd, "select") {
						if len(argv) != 1 {
							log.Panicf("DbSyncer[%v] select command len(args) = %d", ds.id, len(argv))
						}
						s := string(argv[0])
						n, err := strconv.Atoi(s)
						if err != nil {
							log.PanicErrorf(err, "DbSyncer[%v] parse db = %s failed", ds.id, s)
						}
						bypass = filter.FilterDB(n)
						isselect = true
					} else if filter.FilterCommands(scmd) {
						ignorecmd = true
					}
					if bypass || ignorecmd {
						ds.stat.incrSyncFilter.Incr()
						// ds.SyncStat.BypassCmdCount.Incr()
						metric.GetMetric(ds.id).AddBypassCmdCount(ds.id, 1)
						log.Debugf("DbSyncer[%v] ignore command[%v]", ds.id, scmd)
						continue
					}
				}

				newArgv, reject = filter.HandleFilterKeyWithCommand(scmd, argv)
				if bypass || ignorecmd || reject {
					ds.stat.incrSyncFilter.Incr()
					metric.GetMetric(ds.id).AddBypassCmdCount(ds.id, 1)
					log.Debugf("DbSyncer[%v] filter command[%v]", ds.id, scmd)
					continue
				}
			}

			if isselect && conf.Options.TargetDB != -1 {
				if conf.Options.TargetDB != int(lastdb) {
					lastdb = int32(conf.Options.TargetDB)
					//sendBuf <- cmdDetail{Cmd: scmd, Args: argv, Timestamp: time.Now()}
					/* send select command. */
					ds.sendBuf <- cmdDetail{Cmd: "SELECT", Args: [][]byte{[]byte(strconv.FormatInt(int64(lastdb), 10))}}
				} else {
					ds.stat.incrSyncFilter.Incr()
					metric.GetMetric(ds.id).AddBypassCmdCount(ds.id, 1)
				}
				continue
			}
			ds.sendBuf <- cmdDetail{Cmd: scmd, Args: newArgv}
		}
	}()

	go func() {
		var noFlushCount uint
		var cachedSize uint64

		for item := range ds.sendBuf {
			length := len(item.Cmd)
			data := make([]interface{}, len(item.Args))
			for i := range item.Args {
				data[i] = item.Args[i]
				length += len(item.Args[i])
			}
			err := c.Send(item.Cmd, data...)
			if err != nil {
				log.Panicf("DbSyncer[%v] Event:SendToTargetFail\tId:%s\tError:%s\t",
					ds.id, conf.Options.Id, err.Error())
			}
			noFlushCount += 1

			ds.stat.wCommands.Incr()
			ds.stat.wBytes.Add(int64(length))
			metric.GetMetric(ds.id).AddPushCmdCount(ds.id, 1)
			metric.GetMetric(ds.id).AddNetworkFlow(ds.id, uint64(length))
			sendId.Incr()

			if conf.Options.Metric {
				// delay channel
				ds.addDelayChan(sendId.Get())
			}

			if noFlushCount > conf.Options.SenderCount || cachedSize > conf.Options.SenderSize ||
				len(ds.sendBuf) == 0 { // 5000 ds in a batch
				err := c.Flush()
				noFlushCount = 0
				cachedSize = 0
				if utils.CheckHandleNetError(err) {
					log.Panicf("DbSyncer[%v] Event:NetErrorWhileFlush\tId:%s\tError:%s\t",
						ds.id, conf.Options.Id, err.Error())
				}
			}
		}
	}()

	// print stat
	for lStat := ds.stat.Stat(); ; {
		time.Sleep(time.Second)
		nStat := ds.stat.Stat()
		var b bytes.Buffer
		fmt.Fprintf(&b, "DbSyncer[%v] sync: ", ds.id)
		fmt.Fprintf(&b, " +forwardCommands=%-6d", nStat.wCommands - lStat.wCommands)
		fmt.Fprintf(&b, " +filterCommands=%-6d", nStat.incrSyncFilter - lStat.incrSyncFilter)
		fmt.Fprintf(&b, " +writeBytes=%d", nStat.wBytes - lStat.wBytes)
		log.Info(b.String())
		lStat = nStat
	}
}

func (ds *DbSyncer) fetchOffset() {
	if conf.Options.Psync == false {
		log.Warnf("DbSyncer[%v] GetFakeSlaveOffset not enable when psync == false", ds.id)
		return
	}

	srcConn := utils.OpenRedisConnWithTimeout([]string{ds.source}, conf.Options.SourceAuthType, ds.sourcePassword,
		incrSyncReadeTimeout, incrSyncReadeTimeout, false, conf.Options.SourceTLSEnable)
	ticker := time.NewTicker(10 * time.Second)
	for range ticker.C {
		offset, err := utils.GetFakeSlaveOffset(srcConn)
		if err != nil {
			// log.PurePrintf("%s\n", NewLogItem("GetFakeSlaveOffsetFail", "WARN", NewErrorLogDetail("", err.Error())))
			log.Warnf("DbSyncer[%v] Event:GetFakeSlaveOffsetFail\tId:%s\tWarn:%s",
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
				log.Errorf("DbSyncer[%v] Event:GetFakeSlaveOffsetFail\tId:%s\tError:%s",
					ds.id, conf.Options.Id, err.Error())
			}
		}
	}

	log.Panicf("something wrong if you see me")
}

func (ds *DbSyncer) addDelayChan(id int64) {
	// send
	/*
	 * available >=4096: 1:1 sampling
	 * available >=1024: 1:10 sampling
	 * available >=128: 1:100 sampling
	 * else: 1:1000 sampling
	 */
	used := cap(ds.delayChannel) - len(ds.delayChannel)
	if used >= 4096 ||
		used >= 1024 && id%10 == 0 ||
		used >= 128 && id%100 == 0 ||
		id%1000 == 0 {
		// non-blocking add
		select {
		case ds.delayChannel <- &delayNode{t: time.Now(), id: id}:
		default:
			// do nothing but print when channel is full
			log.Warnf("DbSyncer[%v] delayChannel is full", ds.id)
		}
	}
}
