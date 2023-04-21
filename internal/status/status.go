package status

import (
	"RedisShake/internal/config"
	"RedisShake/internal/log"
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
	// transform
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

func AddEntryCount(cmd string, allow bool) {
	ch <- func() {
		if stat.PerCmdEntriesCount == nil {
			stat.PerCmdEntriesCount = make(map[string]EntryCount)
		}
		cmdEntryCount, ok := stat.PerCmdEntriesCount[cmd]
		if !ok {
			cmdEntryCount = EntryCount{}
			stat.PerCmdEntriesCount[cmd] = cmdEntryCount
		}
		if allow {
			stat.TotalEntriesCount.Allow += 1
			cmdEntryCount.Allow += 1
		} else {
			stat.TotalEntriesCount.Disallow += 1
			cmdEntryCount.Disallow += 1
		}
		stat.PerCmdEntriesCount[cmd] = cmdEntryCount
	}
}

func Init(r Statusable, w Statusable) {
	theReader = r
	theWriter = w
	setStatusPort()
	stat.Time = time.Now().Format("2006-01-02 15:04:05")

	// for update reader/writer stat
	go func() {
		ticker := time.NewTicker(1 * time.Second)
		defer ticker.Stop()
		lastConsistent := false
		for {
			select {
			case <-ticker.C:
				ch <- func() {
					// update reader/writer stat
					stat.Reader = theReader.Status()
					stat.Writer = theWriter.Status()
					stat.Consistent = lastConsistent && theReader.StatusConsistent() && theWriter.StatusConsistent()
					lastConsistent = stat.Consistent
					// update OPS
					stat.TotalEntriesCount.updateOPS()
					for _, cmdEntryCount := range stat.PerCmdEntriesCount {
						cmdEntryCount.updateOPS()
					}
				}
			}
		}
	}()

	// for log to screen
	go func() {
		if config.Opt.Advanced.LogInterval <= 0 {
			log.Infof("log interval is 0, will not log to screen")
			return
		}
		ticker := time.NewTicker(time.Duration(config.Opt.Advanced.LogInterval) * time.Second)
		defer ticker.Stop()
		for {
			select {
			case <-ticker.C:
				ch <- func() {
					log.Infof("%s, %s, %s",
						stat.TotalEntriesCount.String(),
						theReader.StatusString(),
						theWriter.StatusString())
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
