// Copyright 2016 CodisLabs. All Rights Reserved.
// Licensed under the MIT (MIT-LICENSE.txt) license.

package run

import (
	"bufio"
	"bytes"
	"fmt"
	"io"
	"net"
	"os"
	"strconv"
	"strings"
	"time"

	"pkg/libs/atomic2"
	"pkg/libs/io/pipe"
	"pkg/libs/log"
	"pkg/redis"
	"redis-shake/common"
	"redis-shake/configure"
	"redis-shake/command"
	"redis-shake/base"
	"redis-shake/heartbeat"
	"redis-shake/metric"
)

type delayNode struct {
	t  time.Time // timestamp
	id int64     // id
}

type CmdSync struct {
	rbytes, wbytes, nentry, ignore atomic2.Int64

	forward, nbypass atomic2.Int64

	targetOffset atomic2.Int64
	sourceOffset int64

	/*
	 * this channel is used to calculate delay between redis-shake and target redis.
	 * Once oplog sent, the corresponding delayNode push back into this queue. Next time
	 * receive reply from target redis, the front node poped and then delay calculated.
	 */
	delayChannel chan *delayNode

	// sending queue
	sendBuf chan cmdDetail
	
	wait_full chan struct{}

	status string
}

type cmdSyncStat struct {
	rbytes, wbytes, nentry, ignore int64

	forward, nbypass int64
}

type cmdDetail struct {
	Cmd       string
	Args      [][]byte
}

func (c *cmdDetail) String() string {
	str := c.Cmd
	for _, s := range c.Args {
		str += " " + string(s)
	}
	return str
}

func (cmd *CmdSync) Stat() *cmdSyncStat {
	return &cmdSyncStat{
		rbytes: cmd.rbytes.Get(),
		wbytes: cmd.wbytes.Get(),
		nentry: cmd.nentry.Get(),
		ignore: cmd.ignore.Get(),

		forward: cmd.forward.Get(),
		nbypass: cmd.nbypass.Get(),
	}
}

// return send buffer length, delay channel length, target db offset
func (cmd *CmdSync) GetDetailedInfo() []interface{} {

	return []interface{}{len(cmd.sendBuf), len(cmd.delayChannel), cmd.targetOffset.Get(), cmd.sourceOffset}
}

func (cmd *CmdSync) Main() {
	from, target := conf.Options.SourceAddress, conf.Options.TargetAddress
	if len(from) == 0 {
		log.Panic("invalid argument: from")
	}
	if len(target) == 0 {
		log.Panic("invalid argument: target")
	}

	log.Infof("sync from '%s' to '%s' with http-port[%d]\n", from, target, conf.Options.HttpProfile)
	cmd.wait_full = make(chan struct{})

	var sockfile *os.File
	if len(conf.Options.SockFileName) != 0 {
		sockfile = utils.OpenReadWriteFile(conf.Options.SockFileName)
		defer sockfile.Close()
	}

	base.Status = "waitfull"
	var input io.ReadCloser
	var nsize int64
	if conf.Options.Psync {
		input, nsize = cmd.SendPSyncCmd(from, conf.Options.SourceAuthType, conf.Options.SourcePasswordRaw)
	} else {
		input, nsize = cmd.SendSyncCmd(from, conf.Options.SourceAuthType, conf.Options.SourcePasswordRaw)
	}
	defer input.Close()

	log.Infof("rdb file = %d\n", nsize)

	if sockfile != nil {
		r, w := pipe.NewFilePipe(int(conf.Options.SockFileSize), sockfile)
		defer r.Close()
		go func(r io.Reader) {
			defer w.Close()
			p := make([]byte, utils.ReaderBufferSize)
			for {
				utils.Iocopy(r, w, p, len(p))
			}
		}(input)
		input = r
	}

	if len(conf.Options.HeartbeatUrl) > 0 {
		heartbeatCtl := heartbeat.HeartbeatController{
			ServerUrl: conf.Options.HeartbeatUrl,
			Interval:  int32(conf.Options.HeartbeatInterval),
		}
		go heartbeatCtl.Start()
	}

	reader := bufio.NewReaderSize(input, utils.ReaderBufferSize)

	base.Status = "full"
	cmd.SyncRDBFile(reader, target, conf.Options.TargetAuthType, conf.Options.TargetPasswordRaw, nsize)

	base.Status = "incr"
	close(cmd.wait_full)
	cmd.SyncCommand(reader, target, conf.Options.TargetAuthType, conf.Options.TargetPasswordRaw)
}

func (cmd *CmdSync) SendSyncCmd(master, auth_type, passwd string) (net.Conn, int64) {
	c, wait := utils.OpenSyncConn(master, auth_type, passwd)
	for {
		select {
		case nsize := <-wait:
			if nsize == 0 {
				log.Info("+")
			} else {
				return c, nsize
			}
		case <-time.After(time.Second):
			log.Info("-")
		}
	}
}

func (cmd *CmdSync) SendPSyncCmd(master, auth_type, passwd string) (pipe.Reader, int64) {
	c := utils.OpenNetConn(master, auth_type, passwd)
	log.Infof("psync connect '%v' with auth type[%v] OK!", master, auth_type)

	utils.SendPSyncListeningPort(c, conf.Options.HttpProfile)
	log.Infof("psync send listening port[%v] OK!", conf.Options.HttpProfile)

	br := bufio.NewReaderSize(c, utils.ReaderBufferSize)
	bw := bufio.NewWriterSize(c, utils.WriterBufferSize)

	log.Infof("try to send 'psync' command")
	runid, offset, wait := utils.SendPSyncFullsync(br, bw)
	cmd.targetOffset.Set(offset)
	log.Infof("psync runid = %s offset = %d, fullsync", runid, offset)

	var nsize int64
	for nsize == 0 {
		select {
		case nsize = <-wait:
			if nsize == 0 {
				log.Info("+")
			}
		case <-time.After(time.Second):
			log.Info("-")
		}
	}

	piper, pipew := pipe.NewSize(utils.ReaderBufferSize)

	go func() {
		defer pipew.Close()
		p := make([]byte, 8192)
		for rdbsize := int(nsize); rdbsize != 0; {
			rdbsize -= utils.Iocopy(br, pipew, p, rdbsize)
		}
		for {
			n, err := cmd.PSyncPipeCopy(c, br, bw, offset, pipew)
			if err != nil {
				log.PanicErrorf(err, "psync runid = %s, offset = %d, pipe is broken", runid, offset)
			}
			offset += n
			cmd.targetOffset.Set(offset)
			for {
				// cmd.SyncStat.SetStatus("reopen")
				base.Status = "reopen"
				time.Sleep(time.Second)
				c = utils.OpenNetConnSoft(master, auth_type, passwd)
				if c != nil {
					// log.PurePrintf("%s\n", NewLogItem("SourceConnReopenSuccess", "INFO", LogDetail{Info: strconv.FormatInt(offset, 10)}))
					log.Infof("Event:SourceConnReopenSuccess\tId: %s\toffset = %d", conf.Options.Id, offset)
					// cmd.SyncStat.SetStatus("incr")
					base.Status = "incr"
					break
				} else {
					// log.PurePrintf("%s\n", NewLogItem("SourceConnReopenFail", "WARN", NewErrorLogDetail("", "")))
					log.Errorf("Event:SourceConnReopenFail\tId: %s", conf.Options.Id)
				}
			}
			utils.AuthPassword(c, auth_type, passwd)
			utils.SendPSyncListeningPort(c, conf.Options.HttpProfile)
			br = bufio.NewReaderSize(c, utils.ReaderBufferSize)
			bw = bufio.NewWriterSize(c, utils.WriterBufferSize)
			utils.SendPSyncContinue(br, bw, runid, offset)
		}
	}()
	return piper, nsize
}

func (cmd *CmdSync) PSyncPipeCopy(c net.Conn, br *bufio.Reader, bw *bufio.Writer, offset int64, copyto io.Writer) (int64, error) {
	defer c.Close()
	var nread atomic2.Int64
	go func() {
		defer c.Close()
		for {
			time.Sleep(time.Second * 1)
			select {
			case <-cmd.wait_full:
				if err := utils.SendPSyncAck(bw, offset+nread.Get()); err != nil {
					return
				}
			default:
				if err := utils.SendPSyncAck(bw, 0); err != nil {
					return
				}
			}
		}
	}()

	var p = make([]byte, 8192)
	for {
		n, err := br.Read(p)
		if err != nil {
			return nread.Get(), nil
		}
		if _, err := copyto.Write(p[:n]); err != nil {
			return nread.Get(), err
		}
		nread.Add(int64(n))
	}
}

func (cmd *CmdSync) SyncRDBFile(reader *bufio.Reader, target, auth_type, passwd string, nsize int64) {
	pipe := utils.NewRDBLoader(reader, &cmd.rbytes, conf.Options.Parallel * 32)
	wait := make(chan struct{})
	go func() {
		defer close(wait)
		group := make(chan int, conf.Options.Parallel)
		for i := 0; i < cap(group); i++ {
			go func() {
				defer func() {
					group <- 0
				}()
				c := utils.OpenRedisConn(target, auth_type, passwd)
				defer c.Close()
				var lastdb uint32 = 0
				for e := range pipe {
					if !base.AcceptDB(e.DB) {
						cmd.ignore.Incr()
					} else {
						cmd.nentry.Incr()
						if conf.Options.TargetDB != -1 {
							if conf.Options.TargetDB != int(lastdb) {
								lastdb = uint32(conf.Options.TargetDB)
								utils.SelectDB(c, uint32(conf.Options.TargetDB))
							}
						} else {
							if e.DB != lastdb {
								lastdb = e.DB
								utils.SelectDB(c, lastdb)
							}
						}

						if len(conf.Options.FilterKey) != 0 {
							for i := 0; i < len(conf.Options.FilterKey); i++ {
								if strings.HasPrefix(string(e.Key), conf.Options.FilterKey[i]) {
									utils.RestoreRdbEntry(c, e)
									break
								}
							}
						} else if len(conf.Options.FilterSlot) > 0 {
							for _, slot := range conf.Options.FilterSlot {
								slotInt, _ := strconv.Atoi(slot)
								if int(utils.KeyToSlot(string(e.Key))) == slotInt {
									utils.RestoreRdbEntry(c, e)
									break
								}
							}
						} else {
							utils.RestoreRdbEntry(c, e)
						}
					}
				}
			}()
		}
		for i := 0; i < cap(group); i++ {
			<-group
		}
	}()

	var stat *cmdSyncStat

	for done := false; !done; {
		select {
		case <-wait:
			done = true
		case <-time.After(time.Second):
		}
		stat = cmd.Stat()
		var b bytes.Buffer
		fmt.Fprintf(&b, "total=%d - %12d [%3d%%]", nsize, stat.rbytes, 100*stat.rbytes/nsize)
		fmt.Fprintf(&b, "  entry=%-12d", stat.nentry)
		if stat.ignore != 0 {
			fmt.Fprintf(&b, "  ignore=%-12d", stat.ignore)
		}
		log.Info(b.String())
		metric.MetricVar.SetFullSyncProgress(uint64(100 * stat.rbytes / nsize))
	}
	log.Info("sync rdb done")
}

func (cmd *CmdSync) SyncCommand(reader *bufio.Reader, target, auth_type, passwd string) {
	c := utils.OpenRedisConnWithTimeout(target, auth_type, passwd, time.Duration(10)*time.Minute, time.Duration(10)*time.Minute)
	defer c.Close()

	cmd.sendBuf = make(chan cmdDetail, conf.Options.SenderCount)
	cmd.delayChannel = make(chan *delayNode, conf.Options.SenderDelayChannelSize)
	var sendId, recvId atomic2.Int64

	go func() {
		srcConn := utils.OpenRedisConnWithTimeout(conf.Options.SourceAddress, conf.Options.SourceAuthType,
			conf.Options.SourcePasswordRaw, time.Duration(10)*time.Minute, time.Duration(10)*time.Minute)
		ticker := time.NewTicker(10 * time.Second)
		for range ticker.C {
			offset, err := utils.GetFakeSlaveOffset(srcConn)
			if err != nil {
				// log.PurePrintf("%s\n", NewLogItem("GetFakeSlaveOffsetFail", "WARN", NewErrorLogDetail("", err.Error())))
				log.Warnf("Event:GetFakeSlaveOffsetFail\tId:%s\tWarn:%s", conf.Options.Id, err.Error())

				// Reconnect while network error happen
				if err == io.EOF {
					srcConn = utils.OpenRedisConnWithTimeout(conf.Options.SourceAddress, conf.Options.SourceAuthType,
						conf.Options.SourcePasswordRaw, time.Duration(10)*time.Minute, time.Duration(10)*time.Minute)
				} else if _, ok := err.(net.Error); ok {
					srcConn = utils.OpenRedisConnWithTimeout(conf.Options.SourceAddress, conf.Options.SourceAuthType,
						conf.Options.SourcePasswordRaw, time.Duration(10)*time.Minute, time.Duration(10)*time.Minute)
				}
			} else {
				// cmd.SyncStat.SetOffset(offset)
				if cmd.sourceOffset, err = strconv.ParseInt(offset, 10, 64); err != nil {
					log.Errorf("Event:GetFakeSlaveOffsetFail\tId:%s\tError:%s", conf.Options.Id, err.Error())
				}
			}
			// cmd.SyncStat.SendBufCount = int64(len(sendBuf))
			// cmd.SyncStat.ProcessingCmdCount = int64(len(cmd.delayChannel))
			//log.Infof("%s", cmd.SyncStat.Roll())
			// cmd.SyncStat.Roll()
			// log.PurePrintf("%s\n", NewLogItem("Metric", "INFO", cmd.SyncStat.Snapshot()))
		}
	}()

	go func() {
		var node *delayNode
		for {
			_, err := c.Receive()
			if conf.Options.Metric == false {
				continue
			}
			recvId.Incr()

			if err == nil {
				// cmd.SyncStat.SuccessCmdCount.Incr()
				metric.MetricVar.AddSuccessCmdCount(1)
			} else {
				// cmd.SyncStat.FailCmdCount.Incr()
				metric.MetricVar.AddFailCmdCount(1)
				if utils.CheckHandleNetError(err) {
					// log.PurePrintf("%s\n", NewLogItem("NetErrorWhileReceive", "ERROR", NewErrorLogDetail("", err.Error())))
					log.Panicf("Event:NetErrorWhileReceive\tId:%s\tError:%s", conf.Options.Id, err.Error())
				} else {
					// log.PurePrintf("%s\n", NewLogItem("ErrorReply", "ERROR", NewErrorLogDetail("", err.Error())))
					log.Panicf("Event:ErrorReply\tId:%s\tCommand: [unknown]\tError: %s",
						conf.Options.Id, err.Error())
				}
			}

			if node == nil {
				// non-blocking read from delay channel
				select {
				case node = <-cmd.delayChannel:
				default:
					// it's ok, channel is empty
				}
			}

			if node != nil {
				id := recvId.Get() // receive id
				if node.id == id {
					// cmd.SyncStat.Delay.Add(time.Now().Sub(node.t).Nanoseconds())
					metric.MetricVar.AddDelay(uint64(time.Now().Sub(node.t).Nanoseconds()) / 1000000) // ms
					node = nil
				} else if node.id < id {
					log.Panicf("receive id invalid: node-id[%v] < receive-id[%v]", node.id, id)
				}
			}
		}
	}()

	go func() {
		var lastdb int32 = 0
		var bypass bool = false
		var isselect bool = false

        var scmd string
        var argv, new_argv [][]byte
        var err error

		decoder := redis.NewDecoder(reader)

		// log.PurePrintf("%s\n", NewLogItem("IncrSyncStart", "INFO", LogDetail{}))
		log.Infof("Event:IncrSyncStart\tId:%s\t", conf.Options.Id)

		for {
			ignorecmd := false
			isselect = false
			resp := redis.MustDecodeOpt(decoder)

			if scmd, argv, err = redis.ParseArgs(resp); err != nil {
				log.PanicError(err, "parse command arguments failed")
			} else {
				// cmd.SyncStat.PullCmdCount.Incr()
				metric.MetricVar.AddPullCmdCount(1)
				if scmd != "ping" {
					if strings.EqualFold(scmd, "select") {
						if len(argv) != 1 {
							log.Panicf("select command len(args) = %d", len(argv))
						}
						s := string(argv[0])
						n, err := strconv.Atoi(s)
						if err != nil {
							log.PanicErrorf(err, "parse db = %s failed", s)
						}
						bypass = !base.AcceptDB(uint32(n))
						isselect = true
					} else if strings.EqualFold(scmd, "opinfo") {
						ignorecmd = true
					}
					if bypass || ignorecmd {
						cmd.nbypass.Incr()
						// cmd.SyncStat.BypassCmdCount.Incr()
						metric.MetricVar.AddBypassCmdCount(1)
						continue
					}
				}

				is_filter := false
				if len(conf.Options.FilterKey) != 0 {
					cmd, ok := command.RedisCommands[scmd]
					if ok && len(argv) > 0 {
						new_argv, is_filter = command.GetMatchKeys(cmd, argv, conf.Options.FilterKey)
					} else {
						is_filter = true
						new_argv = argv
					}
				} else {
					is_filter = true
					new_argv = argv
				}
				if bypass || ignorecmd || !is_filter {
					cmd.nbypass.Incr()
					continue
				}
			}

			if isselect && conf.Options.TargetDB != -1 {
				if conf.Options.TargetDB != int(lastdb) {
					lastdb = int32(conf.Options.TargetDB)
					//sendBuf <- cmdDetail{Cmd: scmd, Args: argv, Timestamp: time.Now()}
					/* send select command. */
					cmd.sendBuf <- cmdDetail{Cmd: "SELECT", Args: [][]byte{[]byte(strconv.FormatInt(int64(lastdb), 10))}}
				} else {
					cmd.nbypass.Incr()
					// cmd.SyncStat.BypassCmdCount.Incr()
					metric.MetricVar.AddBypassCmdCount(1)
				}
				continue
			}
			cmd.sendBuf <- cmdDetail{Cmd: scmd, Args: new_argv}
		}
	}()

	go func() {
		var noFlushCount uint
		var cachedSize uint64

		for item := range cmd.sendBuf {
			length := len(item.Cmd)
			data := make([]interface{}, len(item.Args))
			for i := range item.Args {
				data[i] = item.Args[i]
				length += len(item.Args[i])
			}
			err := c.Send(item.Cmd, data...)
			if err != nil {
				// log.PurePrintf("%s\n", NewLogItem("SendToTargetFail", "ERROR", NewErrorLogDetail("", err.Error())))
				log.Panicf("Event:SendToTargetFail\tId:%s\tError:%s\t", conf.Options.Id, err.Error())
			}
			noFlushCount += 1

			cmd.forward.Incr()
			// cmd.SyncStat.PushCmdCount.Incr()
			metric.MetricVar.AddPushCmdCount(1)
			// cmd.SyncStat.NetworkFlow.Add(int64(length)) // 发送流量
			metric.MetricVar.AddNetworkFlow(uint64(length))
			sendId.Incr()

			if conf.Options.Metric {
				// delay channel
				cmd.addDelayChan(sendId.Get())
			}

			if noFlushCount > conf.Options.SenderCount || cachedSize > conf.Options.SenderSize ||
					len(cmd.sendBuf) == 0 { // 5000 cmd in a batch
				err := c.Flush()
				noFlushCount = 0
				cachedSize = 0
				if utils.CheckHandleNetError(err) {
					// log.PurePrintf("%s\n", NewLogItem("NetErrorWhileFlush", "ERROR", NewErrorLogDetail("", err.Error())))
					log.Panicf("Event:NetErrorWhileFlush\tId:%s\tError:%s\t", conf.Options.Id, err.Error())
				}
			}
		}
	}()

	for lstat := cmd.Stat(); ; {
		time.Sleep(time.Second)
		nstat := cmd.Stat()
		var b bytes.Buffer
		fmt.Fprintf(&b, "sync: ")
		fmt.Fprintf(&b, " +forward=%-6d", nstat.forward-lstat.forward)
		fmt.Fprintf(&b, " +nbypass=%-6d", nstat.nbypass-lstat.nbypass)
		fmt.Fprintf(&b, " +nbytes=%d", nstat.wbytes-lstat.wbytes)
		log.Info(b.String())
		lstat = nstat
	}
}

func (cmd *CmdSync) addDelayChan(id int64) {
	// send
	/*
	 * available >=4096: 1:1 sampling
	 * available >=1024: 1:10 sampling
	 * available >=128: 1:100 sampling
	 * else: 1:1000 sampling
	 */
	used := cap(cmd.delayChannel) - len(cmd.delayChannel)
	if used >= 4096 ||
			used >= 1024 && id % 10 == 0 ||
			used >= 128 && id % 100 == 0 ||
			id % 1000 == 0 {
		// non-blocking add
		select {
		case cmd.delayChannel <- &delayNode{t: time.Now(), id: id}:
		default:
			// do nothing but print when channel is full
			log.Warn("delayChannel is full")
		}
	}
}
