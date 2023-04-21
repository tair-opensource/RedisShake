package status

import (
	"fmt"
	"time"
)

type EntryCount struct {
	Allow       uint64  `json:"allow"`
	Disallow    uint64  `json:"disallow"`
	AllowOps    float64 `json:"allow_ops"`
	DisallowOps float64 `json:"disallow_ops"`

	// update ops
	lastAllow              uint64
	lastDisallow           uint64
	lastUpdateTimestampSec float64
}

// call this function every second
func (e *EntryCount) updateOPS() {
	nowTimestampSec := float64(time.Now().UnixNano()) / 1e9
	if e.lastUpdateTimestampSec != 0 {
		timeIntervalSec := nowTimestampSec - e.lastUpdateTimestampSec
		e.AllowOps = float64(e.Allow-e.lastAllow) / timeIntervalSec
		e.DisallowOps = float64(e.Disallow-e.lastDisallow) / timeIntervalSec
		e.lastAllow = e.Allow
		e.lastDisallow = e.Disallow
	}
	e.lastUpdateTimestampSec = nowTimestampSec
}

func (e *EntryCount) String() string {
	return fmt.Sprintf("allow: %.2fops/s, disallow: %.2fops/s", e.AllowOps, e.DisallowOps)
}
