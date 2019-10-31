package dbSync

import "pkg/libs/atomic2"

type Status struct {
	rBytes         atomic2.Int64 // read bytes
	wBytes         atomic2.Int64 // write bytes
	wCommands      atomic2.Int64 // write commands (forward)
	keys          atomic2.Int64 // total key number (nentry)
	fullSyncFilter atomic2.Int64 // filtered keys in full sync (ignore)
	incrSyncFilter atomic2.Int64 // filtered keys in increase sync (nbypass)
	targetOffset   atomic2.Int64 // target offset
	sourceOffset   int64         // source offset
}

func (s *Status) Stat() *syncerStat {
	return &syncerStat{
		rBytes:         s.rBytes.Get(),
		wBytes:         s.wBytes.Get(),
		wCommands:      s.wCommands.Get(),
		keys:          s.keys.Get(),
		fullSyncFilter: s.fullSyncFilter.Get(),
		incrSyncFilter: s.incrSyncFilter.Get(),
	}
}

// corresponding to Status struct
type syncerStat struct {
	rBytes         int64
	wBytes         int64
	wCommands      int64
	keys          int64
	fullSyncFilter int64
	incrSyncFilter int64
}
