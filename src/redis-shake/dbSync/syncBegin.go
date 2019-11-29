package dbSync

import (
	"net"
	"redis-shake/common"
	"pkg/libs/log"
	"time"
	"pkg/libs/io/pipe"
	"bufio"
	"redis-shake/base"
	"io"
	"pkg/libs/atomic2"

	"redis-shake/configure"
)

// send command to source redis

func (ds *DbSyncer) sendSyncCmd(master, authType, passwd string, tlsEnable bool) (net.Conn, int64) {
	c, wait := utils.OpenSyncConn(master, authType, passwd, tlsEnable)
	for {
		select {
		case nsize := <-wait:
			if nsize == 0 {
				log.Infof("DbSyncer[%2d] + waiting source rdb", ds.id)
			} else {
				return c, nsize
			}
		case <-time.After(time.Second):
			log.Infof("DbSyncer[%2d] - waiting source rdb", ds.id)
		}
	}
}

func (ds *DbSyncer) sendPSyncCmd(master, authType, passwd string, tlsEnable bool) (pipe.Reader, int64) {
	c := utils.OpenNetConn(master, authType, passwd, tlsEnable)
	log.Infof("DbSyncer[%2d] psync connect '%v' with auth type[%v] OK!", ds.id, master, authType)

	utils.SendPSyncListeningPort(c, conf.Options.HttpProfile)
	log.Infof("DbSyncer[%2d] psync send listening port[%v] OK!", ds.id, conf.Options.HttpProfile)

	// reader buffer bind to client
	br := bufio.NewReaderSize(c, utils.ReaderBufferSize)
	// writer buffer bind to client
	bw := bufio.NewWriterSize(c, utils.WriterBufferSize)

	log.Infof("DbSyncer[%2d] try to send 'psync' command", ds.id)
	// send psync command and decode the result
	runid, offset, wait := utils.SendPSyncFullsync(br, bw)
	ds.stat.targetOffset.Set(offset)
	log.Infof("DbSyncer[%2d] psync runid = %s offset = %d, fullsync", ds.id, runid, offset)

	// get rdb file size
	var nsize int64
	for nsize == 0 {
		select {
		case nsize = <-wait:
			if nsize == 0 {
				log.Infof("DbSyncer[%2d] +", ds.id)
			}
		case <-time.After(time.Second):
			log.Infof("DbSyncer[%2d] -", ds.id)
		}
	}

	// write -> pipew -> piper -> read
	piper, pipew := pipe.NewSize(utils.ReaderBufferSize)

	go func() {
		defer pipew.Close()
		p := make([]byte, 8192)
		// read rdb in for loop
		for rdbsize := int(nsize); rdbsize != 0; {
			// br -> pipew
			rdbsize -= utils.Iocopy(br, pipew, p, rdbsize)
		}

		for {
			/*
			 * read from br(source redis) and write into pipew.
			 * Generally speaking, this function is forever run.
			 */
			n, err := ds.pSyncPipeCopy(c, br, bw, offset, pipew)
			if err != nil {
				log.PanicErrorf(err, "DbSyncer[%2d] psync runid = %s, offset = %d, pipe is broken",
					ds.id, runid, offset)
			}
			// the 'c' is closed every loop

			offset += n
			ds.stat.targetOffset.Set(offset)

			// reopen 'c' every time
			for {
				// ds.SyncStat.SetStatus("reopen")
				base.Status = "reopen"
				time.Sleep(time.Second)
				c = utils.OpenNetConnSoft(master, authType, passwd, tlsEnable)
				if c != nil {
					// log.PurePrintf("%s\n", NewLogItem("SourceConnReopenSuccess", "INFO", LogDetail{Info: strconv.FormatInt(offset, 10)}))
					log.Infof("DbSyncer[%2d] Event:SourceConnReopenSuccess\tId: %s\toffset = %d",
						ds.id, conf.Options.Id, offset)
					// ds.SyncStat.SetStatus("incr")
					base.Status = "incr"
					break
				} else {
					// log.PurePrintf("%s\n", NewLogItem("SourceConnReopenFail", "WARN", NewErrorLogDetail("", "")))
					log.Errorf("DbSyncer[%2d] Event:SourceConnReopenFail\tId: %s", ds.id, conf.Options.Id)
				}
			}
			utils.AuthPassword(c, authType, passwd)
			utils.SendPSyncListeningPort(c, conf.Options.HttpProfile)
			br = bufio.NewReaderSize(c, utils.ReaderBufferSize)
			bw = bufio.NewWriterSize(c, utils.WriterBufferSize)
			utils.SendPSyncContinue(br, bw, runid, offset)
		}
	}()
	return piper, nsize
}

func (ds *DbSyncer) pSyncPipeCopy(c net.Conn, br *bufio.Reader, bw *bufio.Writer, offset int64, copyto io.Writer) (int64, error) {
	// TODO, two times call c.Close() ? maybe a bug
	defer c.Close()
	var nread atomic2.Int64
	go func() {
		defer c.Close()
		for {
			time.Sleep(time.Second * 1)
			select {
			case <-ds.WaitFull:
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