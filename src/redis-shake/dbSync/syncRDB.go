package dbSync

import (
	"bufio"
	"redis-shake/common"
	"redis-shake/base"
	"sync"
	"redis-shake/configure"
	"redis-shake/filter"
	"pkg/libs/log"
	"time"
	"bytes"
	"fmt"

	"redis-shake/metric"
)

func (ds *DbSyncer) syncRDBFile(reader *bufio.Reader, target []string, authType, passwd string, nsize int64, tlsEnable bool) {
	pipe := utils.NewRDBLoader(reader, &ds.stat.rBytes, base.RDBPipeSize)
	wait := make(chan struct{})
	go func() {
		defer close(wait)
		var wg sync.WaitGroup
		wg.Add(conf.Options.Parallel)
		for i := 0; i < conf.Options.Parallel; i++ {
			go func() {
				defer wg.Done()
				c := utils.OpenRedisConn(target, authType, passwd, conf.Options.TargetType == conf.RedisTypeCluster,
					tlsEnable)
				defer c.Close()
				var lastdb uint32 = 0
				for e := range pipe {
					if filter.FilterDB(int(e.DB)) {
						// db filter
						ds.stat.fullSyncFilter.Incr()
					} else {
						ds.stat.keys.Incr()

						log.Debugf("DbSyncer[%d] try restore key[%s] with value length[%v]", ds.id, e.Key, len(e.Value))

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

						if filter.FilterKey(string(e.Key)) == true {
							// 1. judge if not pass filter key
							ds.stat.fullSyncFilter.Incr()
							continue
						} else {
							slot := int(utils.KeyToSlot(string(e.Key)))
							if filter.FilterSlot(slot) == true {
								// 2. judge if not pass filter slot
								ds.stat.fullSyncFilter.Incr()
								continue
							}
						}

						log.Debugf("DbSyncer[%d] start restoring key[%s] with value length[%v]", ds.id, e.Key, len(e.Value))

						utils.RestoreRdbEntry(c, e)
						log.Debugf("DbSyncer[%d] restore key[%s] ok", ds.id, e.Key)
					}
				}
			}()
		}

		wg.Wait()
	}()

	var stat *syncerStat

	// print stat
	for done := false; !done; {
		select {
		case <-wait:
			done = true
		case <-time.After(time.Second):
		}

		stat = ds.stat.Stat()
		var b bytes.Buffer
		fmt.Fprintf(&b, "DbSyncer[%d] total = %s - %12s [%3d%%]  entry=%-12d",
			ds.id, utils.GetMetric(nsize), utils.GetMetric(stat.rBytes), 100*stat.rBytes/nsize, stat.keys)
		if stat.fullSyncFilter != 0 {
			fmt.Fprintf(&b, "  filter=%-12d", stat.fullSyncFilter)
		}
		log.Info(b.String())
		metric.GetMetric(ds.id).SetFullSyncProgress(ds.id, uint64(100*stat.rBytes/nsize))
	}
	log.Infof("DbSyncer[%d] sync rdb done", ds.id)
}
