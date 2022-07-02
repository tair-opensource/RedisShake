package statistics

import (
	"github.com/alibaba/RedisShake/internal/config"
	"github.com/alibaba/RedisShake/internal/log"
	"time"
)

var (
	// ID
	entryId uint64
	// rdb
	rdbFileSize     int64
	rdbReceivedSize int64
	rdbSendSize     int64
	// aof
	aofReceivedOffset int64
	aofAppliedOffset  int64
	// ops
	allowEntriesCount    int64
	disallowEntriesCount int64
	unansweredBytesCount uint64
)

func Init() {
	go func() {
		seconds := config.Config.Advanced.LogInterval
		if seconds <= 0 {
			log.Infof("statistics disabled. seconds=[%d]", seconds)
		}

		for range time.Tick(time.Duration(seconds) * time.Second) {
			if rdbFileSize == 0 {
				continue
			}
			if rdbFileSize > rdbReceivedSize {
				log.Infof("receiving rdb. percent=[%.2f]%%, rdbFileSize=[%.3f]G, rdbReceivedSize=[%.3f]G",
					float64(rdbReceivedSize)/float64(rdbFileSize)*100,
					float64(rdbFileSize)/1024/1024/1024,
					float64(rdbReceivedSize)/1024/1024/1024)
			} else if rdbFileSize > rdbSendSize {
				log.Infof("syncing rdb. percent=[%.2f]%%, allowOps=[%.2f], disallowOps=[%.2f], entryId=[%d], unansweredBytesCount=[%d]bytes, rdbFileSize=[%.3f]G, rdbSendSize=[%.3f]G",
					float64(rdbSendSize)*100/float64(rdbFileSize),
					float32(allowEntriesCount)/float32(seconds),
					float32(disallowEntriesCount)/float32(seconds),
					entryId,
					unansweredBytesCount,
					float64(rdbFileSize)/1024/1024/1024,
					float64(rdbSendSize)/1024/1024/1024)
			} else {
				log.Infof("syncing aof. allowOps=[%.2f], disallowOps=[%.2f], entryId=[%d], unansweredBytesCount=[%d]bytes, diff=[%d], aofReceivedOffset=[%d], aofAppliedOffset=[%d]",
					float32(allowEntriesCount)/float32(seconds),
					float32(disallowEntriesCount)/float32(seconds),
					entryId,
					unansweredBytesCount,
					aofReceivedOffset-aofAppliedOffset,
					aofReceivedOffset,
					aofAppliedOffset)
			}

			allowEntriesCount = 0
			disallowEntriesCount = 0
		}
	}()
}
func UpdateEntryId(id uint64) {
	entryId = id
}
func AddAllowEntriesCount() {
	allowEntriesCount++
}
func AddDisallowEntriesCount() {
	disallowEntriesCount++
}
func SetRDBFileSize(size int64) {
	rdbFileSize = size
}
func UpdateRDBReceivedSize(size int64) {
	rdbReceivedSize = size
}
func UpdateRDBSentSize(offset int64) {
	rdbSendSize = offset
}
func UpdateAOFReceivedOffset(offset int64) {
	aofReceivedOffset = offset
}
func UpdateAOFAppliedOffset(offset int64) {
	aofAppliedOffset = offset
}
func UpdateUnansweredBytesCount(count uint64) {
	unansweredBytesCount = count
}
