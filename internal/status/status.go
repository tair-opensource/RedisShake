package status

import (
	"time"
)

type Statusable interface {
	Status() interface{}
	StatusString() string
	StatusConsistent() bool
}

type Stat struct {
	Time       string `json:"start_time"`
	Consistent bool   `json:"consistent"`
	// function
	TotalEntriesCount  EntryCount            `json:"total_entries_count"`
	PerCmdEntriesCount map[string]EntryCount `json:"per_cmd_entries_count"`
	// reader
	Reader interface{} `json:"reader"`
	// writer
	Writer interface{} `json:"writer"`
}

var ch = make(chan func(), 1000)
var stat = new(Stat)
var theReader Statusable
var theWriter Statusable

func AddReadCount(cmd string) {
	ch <- func() {
		cmdEntryCount, ok := stat.PerCmdEntriesCount[cmd]
		if !ok {
			cmdEntryCount = EntryCount{}
			stat.PerCmdEntriesCount[cmd] = cmdEntryCount
		}
		stat.TotalEntriesCount.ReadCount += 1
		cmdEntryCount.ReadCount += 1
		stat.PerCmdEntriesCount[cmd] = cmdEntryCount
	}
}

func AddWriteCount(cmd string) {
	ch <- func() {
		cmdEntryCount, ok := stat.PerCmdEntriesCount[cmd]
		if !ok {
			cmdEntryCount = EntryCount{}
			stat.PerCmdEntriesCount[cmd] = cmdEntryCount
		}
		stat.TotalEntriesCount.WriteCount += 1
		cmdEntryCount.WriteCount += 1
		stat.PerCmdEntriesCount[cmd] = cmdEntryCount
	}
}

func Init(r Statusable, w Statusable) {
	theReader = r
	theWriter = w
	setStatusPort()
	stat.Time = time.Now().Format("2006-01-02 15:04:05")

	// init per cmd entries count
	if stat.PerCmdEntriesCount == nil {
		stat.PerCmdEntriesCount = make(map[string]EntryCount)
	}

	// for update reader/writer stat
	go func() {
		ticker := time.NewTicker(1 * time.Second)
		defer ticker.Stop()
		lastConsistent := false
		for range ticker.C {
			ch <- func() {
				// update reader/writer stat
				stat.Reader = theReader.Status()
				stat.Writer = theWriter.Status()
				stat.Consistent = lastConsistent && theReader.StatusConsistent() && theWriter.StatusConsistent()
				lastConsistent = stat.Consistent
				// update OPS
				stat.TotalEntriesCount.UpdateOPS()
				for _, cmdEntryCount := range stat.PerCmdEntriesCount {
					cmdEntryCount.UpdateOPS()
				}
			}
		}
	}()

	// run all func in ch
	go func() {
		for f := range ch {
			f()
		}
	}()
}
