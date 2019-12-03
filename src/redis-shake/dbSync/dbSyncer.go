package dbSync

import (
	"redis-shake/metric"
	"os"
	"redis-shake/common"
	"redis-shake/base"
	"io"
	"pkg/libs/log"
	"pkg/libs/io/pipe"
	"redis-shake/heartbeat"
	"bufio"

	"redis-shake/configure"
)

// one sync link corresponding to one DbSyncer
func NewDbSyncer(id int, source, sourcePassword string, target []string, targetPassword string, httpPort int) *DbSyncer {
	ds := &DbSyncer{
		id:              id,
		source:          source,
		sourcePassword:  sourcePassword,
		target:          target,
		targetPassword:  targetPassword,
		httpProfilePort: httpPort,
		WaitFull:        make(chan struct{}),
	}

	// add metric
	metric.AddMetric(id)

	return ds
}

type DbSyncer struct {
	id int // current id in all syncer

	source         string   // source address
	sourcePassword string   // source password
	target         []string // target address
	targetPassword string   // target password

	httpProfilePort int // http profile port

	// stat info
	stat Status

	/*
	 * this channel is used to calculate delay between redis-shake and target redis.
	 * Once oplog sent, the corresponding delayNode push back into this queue. Next time
	 * receive reply from target redis, the front node poped and then delay calculated.
	 */
	delayChannel chan *delayNode

	fullSyncOffset uint64         // full sync offset value
	sendBuf        chan cmdDetail // sending queue
	WaitFull       chan struct{}  // wait full sync done
}

func (ds *DbSyncer) GetExtraInfo() map[string]interface{} {
	return map[string]interface{}{
		"SourceAddress":      ds.source,
		"TargetAddress":      ds.target,
		"SenderBufCount":     len(ds.sendBuf),
		"ProcessingCmdCount": len(ds.delayChannel),
		"TargetDBOffset":     ds.stat.targetOffset.Get(),
		"SourceDBOffset":     ds.stat.sourceOffset,
	}
}

// main
func (ds *DbSyncer) Sync() {
	log.Infof("DbSyncer[%2d] starts syncing data from %v to %v with http[%v]",
		ds.id, ds.source, ds.target, ds.httpProfilePort)

	var sockfile *os.File
	if len(conf.Options.SockFileName) != 0 {
		sockfile = utils.OpenReadWriteFile(conf.Options.SockFileName)
		defer sockfile.Close()
	}

	base.Status = "waitfull"
	var input io.ReadCloser
	var nsize int64
	if conf.Options.Psync {
		input, nsize = ds.sendPSyncCmd(ds.source, conf.Options.SourceAuthType, ds.sourcePassword, conf.Options.SourceTLSEnable)
	} else {
		input, nsize = ds.sendSyncCmd(ds.source, conf.Options.SourceAuthType, ds.sourcePassword, conf.Options.SourceTLSEnable)
	}
	defer input.Close()

	log.Infof("DbSyncer[%2d] rdb file size = %d\n", ds.id, nsize)

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

	// start heartbeat
	if len(conf.Options.HeartbeatUrl) > 0 {
		heartbeatCtl := heartbeat.HeartbeatController{
			ServerUrl: conf.Options.HeartbeatUrl,
			Interval:  int32(conf.Options.HeartbeatInterval),
		}
		go heartbeatCtl.Start()
	}

	reader := bufio.NewReaderSize(input, utils.ReaderBufferSize)

	// sync rdb
	base.Status = "full"
	ds.syncRDBFile(reader, ds.target, conf.Options.TargetAuthType, ds.targetPassword, nsize, conf.Options.TargetTLSEnable)

	// sync increment
	base.Status = "incr"
	close(ds.WaitFull)
	ds.syncCommand(reader, ds.target, conf.Options.TargetAuthType, ds.targetPassword, conf.Options.TargetTLSEnable)
}
