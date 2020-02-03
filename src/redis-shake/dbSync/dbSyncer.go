package dbSync

import (
	"redis-shake/metric"
	"redis-shake/common"
	"redis-shake/base"
	"io"
	"pkg/libs/log"
	"redis-shake/heartbeat"
	"bufio"

	"redis-shake/configure"
	"redis-shake/checkpoint"
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
	runId          string   // source runId

	httpProfilePort int // http profile port

	// stat info
	stat Status

	startDbId int // use in break resume from break-point

	/*
	 * this channel is used to calculate delay between redis-shake and target redis.
	 * Once oplog sent, the corresponding delayNode push back into this queue. Next time
	 * receive reply from target redis, the front node poped and then delay calculated.
	 */
	delayChannel chan *delayNode

	fullSyncOffset int64          // full sync offset value
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
	log.Infof("DbSyncer[%d] starts syncing data from %v to %v with http[%v]",
		ds.id, ds.source, ds.target, ds.httpProfilePort)

	// checkpoint reload if has
	runId, offset, dbid, err := checkpoint.LoadCheckpoint(ds.id, ds.source, ds.target, conf.Options.TargetAuthType,
		ds.targetPassword, conf.Options.TargetType == conf.RedisTypeCluster, conf.Options.SourceTLSEnable)
	if err != nil {
		log.Panicf("DbSyncer[%d] load checkpoint from %v failed[%v]", ds.id, ds.target, err)
		return
	}
	log.Infof("DbSyncer[%d] checkpoint info: runId[%v], offset[%v] dbid[%v]", ds.id, runId, offset, dbid)

	base.Status = "waitfull"
	var input io.ReadCloser
	var nsize int64
	var isFullSync bool
	if conf.Options.Psync {
		input, nsize, isFullSync, runId = ds.sendPSyncCmd(ds.source, conf.Options.SourceAuthType, ds.sourcePassword,
			conf.Options.SourceTLSEnable, runId, offset)
		ds.runId = runId
	} else {
		// sync
		input, nsize = ds.sendSyncCmd(ds.source, conf.Options.SourceAuthType, ds.sourcePassword,
			conf.Options.SourceTLSEnable)
	}
	defer input.Close()

	log.Infof("DbSyncer[%d] rdb file size = %d\n", ds.id, nsize)

	// start heartbeat
	if len(conf.Options.HeartbeatUrl) > 0 {
		heartbeatCtl := heartbeat.HeartbeatController{
			ServerUrl: conf.Options.HeartbeatUrl,
			Interval:  int32(conf.Options.HeartbeatInterval),
		}
		go heartbeatCtl.Start()
	}

	reader := bufio.NewReaderSize(input, utils.ReaderBufferSize)

	if isFullSync {
		// sync rdb
		base.Status = "full"
		ds.syncRDBFile(reader, ds.target, conf.Options.TargetAuthType, ds.targetPassword, nsize, conf.Options.TargetTLSEnable)
		ds.startDbId = 0
	} else {
		ds.startDbId = dbid
		// set fullSyncProgress to 100 when skip full sync stage
		metric.GetMetric(ds.id).SetFullSyncProgress(ds.id, 100)
	}

	// sync increment
	base.Status = "incr"
	close(ds.WaitFull)
	ds.syncCommand(reader, ds.target, conf.Options.TargetAuthType, ds.targetPassword, conf.Options.TargetTLSEnable, dbid)
}
