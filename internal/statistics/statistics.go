package statistics

import (
	"encoding/json"
	"fmt"
	"math/bits"
	"net/http"
	"strings"
	"time"

	"github.com/alibaba/RedisShake/internal/config"
	"github.com/alibaba/RedisShake/internal/log"
)

type metrics struct {
	// info
	Address string `json:"address"`

	// entries
	EntryId              uint64 `json:"entry_id"`
	AllowEntriesCount    uint64 `json:"allow_entries_count"`
	DisallowEntriesCount uint64 `json:"disallow_entries_count"`

	// rdb
	IsDoingBgsave   bool   `json:"is_doing_bgsave"`
	RdbFileSize     uint64 `json:"rdb_file_size"`
	RdbReceivedSize uint64 `json:"rdb_received_size"`
	RdbSendSize     uint64 `json:"rdb_send_size"`

	//loading aof
	Loading            bool  `json:"loading"`
	AsyncLoading       bool  `json:"async_loading"`
	LoadingStartTime   int64 `json:"loading_start_time"`
	LoadingLoadedBytes int64 `json:"loading_loaded_bytes"`
	LoadingTotalBytes  int64 `json:"loading_total_bytes"`

	// aof
	AofReceivedOffset uint64 `json:"aof_received_offset"`
	AofAppliedOffset  uint64 `json:"aof_applied_offset"`
	AofFileSize       uint64 `json:"aof_file_size"`
	AofReceivedSize   uint64 `json:"aof_received_size"`
	// for performance debug
	InQueueEntriesCount  uint64 `json:"in_queue_entries_count"`
	UnansweredBytesCount uint64 `json:"unanswered_bytes_count"`

	// scan cursor
	ScanDbId   int    `json:"scan_db_id"`
	ScanCursor uint64 `json:"scan_cursor"`

	// for log
	Msg string `json:"msg"`
}

var Metrics = &metrics{}

func Handler(w http.ResponseWriter, _ *http.Request) {
	w.Header().Add("Content-Type", "application/json")
	err := json.NewEncoder(w).Encode(Metrics)
	if err != nil {
		log.PanicError(err)
	}
}

func Init() {
	go func() {
		seconds := config.Config.Advanced.LogInterval
		if seconds <= 0 {
			log.Infof("statistics disabled. seconds=[%d]", seconds)
		}

		lastAllowEntriesCount := Metrics.AllowEntriesCount
		lastDisallowEntriesCount := Metrics.DisallowEntriesCount

		for range time.Tick(time.Duration(seconds) * time.Second) {
			// scan
			if config.Config.Type == "scan" {
				Metrics.Msg = fmt.Sprintf("syncing. dbId=[%d], percent=[%.2f]%%, allowOps=[%.2f], disallowOps=[%.2f], entryId=[%d], InQueueEntriesCount=[%d], unansweredBytesCount=[%d]bytes",
					Metrics.ScanDbId,
					float64(bits.Reverse64(Metrics.ScanCursor))/float64(^uint(0))*100,
					float32(Metrics.AllowEntriesCount-lastAllowEntriesCount)/float32(seconds),
					float32(Metrics.DisallowEntriesCount-lastDisallowEntriesCount)/float32(seconds),
					Metrics.EntryId,
					Metrics.InQueueEntriesCount,
					Metrics.UnansweredBytesCount)
				log.Infof(strings.Replace(Metrics.Msg, "%", "%%", -1))
				lastAllowEntriesCount = Metrics.AllowEntriesCount
				lastDisallowEntriesCount = Metrics.DisallowEntriesCount
				continue
			}
			// sync or restore
			if Metrics.RdbFileSize == 0 {
				Metrics.Msg = "source db is doing bgsave"
			} else if Metrics.RdbSendSize > Metrics.RdbReceivedSize {
				Metrics.Msg = fmt.Sprintf("receiving rdb. percent=[%.2f]%%, rdbFileSize=[%.3f]G, rdbReceivedSize=[%.3f]G",
					float64(Metrics.RdbReceivedSize)/float64(Metrics.RdbFileSize)*100,
					float64(Metrics.RdbFileSize)/1024/1024/1024,
					float64(Metrics.RdbReceivedSize)/1024/1024/1024)
			} else if Metrics.RdbFileSize > Metrics.RdbSendSize {
				Metrics.Msg = fmt.Sprintf("syncing rdb. percent=[%.2f]%%, allowOps=[%.2f], disallowOps=[%.2f], entryId=[%d], InQueueEntriesCount=[%d], unansweredBytesCount=[%d]bytes, rdbFileSize=[%.3f]G, rdbSendSize=[%.3f]G",
					float64(Metrics.RdbSendSize)*100/float64(Metrics.RdbFileSize),
					float32(Metrics.AllowEntriesCount-lastAllowEntriesCount)/float32(seconds),
					float32(Metrics.DisallowEntriesCount-lastDisallowEntriesCount)/float32(seconds),
					Metrics.EntryId,
					Metrics.InQueueEntriesCount,
					Metrics.UnansweredBytesCount,
					float64(Metrics.RdbFileSize)/1024/1024/1024,
					float64(Metrics.RdbSendSize)/1024/1024/1024)
			} else {
				Metrics.Msg = fmt.Sprintf("syncing aof. allowOps=[%.2f], disallowOps=[%.2f], entryId=[%d], InQueueEntriesCount=[%d], unansweredBytesCount=[%d]bytes, diff=[%d], aofReceivedOffset=[%d], aofAppliedOffset=[%d]",
					float32(Metrics.AllowEntriesCount-lastAllowEntriesCount)/float32(seconds),
					float32(Metrics.DisallowEntriesCount-lastDisallowEntriesCount)/float32(seconds),
					Metrics.EntryId,
					Metrics.InQueueEntriesCount,
					Metrics.UnansweredBytesCount,
					Metrics.AofReceivedOffset-Metrics.AofAppliedOffset,
					Metrics.AofReceivedOffset,
					Metrics.AofAppliedOffset)
			}
			log.Infof(strings.Replace(Metrics.Msg, "%", "%%", -1))
			lastAllowEntriesCount = Metrics.AllowEntriesCount
			lastDisallowEntriesCount = Metrics.DisallowEntriesCount
		}
	}()
}

// entry id

func UpdateEntryId(id uint64) {
	Metrics.EntryId = id
}
func AddAllowEntriesCount() {
	Metrics.AllowEntriesCount++
}
func AddDisallowEntriesCount() {
	Metrics.DisallowEntriesCount++
}

// rdb

func SetRDBFileSize(size uint64) {
	Metrics.RdbFileSize = size
}
func UpdateRDBReceivedSize(size uint64) {
	Metrics.RdbReceivedSize = size
}
func UpdateRDBSentSize(offset uint64) {
	Metrics.RdbSendSize = offset
}

// aof

func UpdateAOFReceivedOffset(offset uint64) {
	Metrics.AofReceivedOffset = offset
}
func UpdateAOFAppliedOffset(offset uint64) {
	Metrics.AofAppliedOffset = offset
}

// for debug

func UpdateInQueueEntriesCount(count uint64) {
	Metrics.InQueueEntriesCount = count
}
func UpdateUnansweredBytesCount(count uint64) {
	Metrics.UnansweredBytesCount = count
}
