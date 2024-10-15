package status

import (
	"fmt"
	"time"
)

type EntryCount struct {
	ReadCount  uint64  `json:"read_count"`
	ReadOps    float64 `json:"read_ops"`
	WriteCount uint64  `json:"write_count"`
	WriteOps   float64 `json:"write_ops"`

	// update ops
	lastReadCount          uint64
	lastWriteCount         uint64
	lastUpdateTimestampSec float64
}

// call this function every second
func (e *EntryCount) UpdateOPS() {
	nowTimestampSec := float64(time.Now().UnixNano()) / 1e9
	if e.lastUpdateTimestampSec != 0 {
		timeIntervalSec := nowTimestampSec - e.lastUpdateTimestampSec
		e.ReadOps = float64(e.ReadCount-e.lastReadCount) / timeIntervalSec
		e.WriteOps = float64(e.WriteCount-e.lastWriteCount) / timeIntervalSec
		e.lastReadCount = e.ReadCount
		e.lastWriteCount = e.WriteCount
	}
	e.lastUpdateTimestampSec = nowTimestampSec
}

func (e *EntryCount) String() string {
	return fmt.Sprintf("read_count=[%d], read_ops=[%.2f], write_count=[%d], write_ops=[%.2f]", e.ReadCount, e.ReadOps, e.WriteCount, e.WriteOps)
}
