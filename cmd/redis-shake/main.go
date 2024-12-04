package main

import (
	"RedisShake/internal/client"
	"context"
	_ "net/http/pprof"
	"os"
	"os/signal"
	"strings"
	"sync/atomic"
	"syscall"
	"time"

	"RedisShake/internal/config"
	"RedisShake/internal/entry"
	"RedisShake/internal/filter"
	"RedisShake/internal/log"
	"RedisShake/internal/reader"
	"RedisShake/internal/status"
	"RedisShake/internal/utils"
	"RedisShake/internal/writer"

	"fmt"
	"runtime"

	"github.com/mcuadros/go-defaults"
)

var (
	// These variables will be set during build time
	Version   = "unknown"
	GitCommit = "unknown"
)

func getVersionString() string {
	return fmt.Sprintf("%s %s/%s (Git SHA: %s)", Version, runtime.GOOS, runtime.GOARCH, GitCommit)
}

func main() {
	// Add version flag check before config loading
	if len(os.Args) == 2 && (os.Args[1] == "-v" || os.Args[1] == "--version" || os.Args[1] == "version") {
		fmt.Printf("redis-shake version %s\n", getVersionString())
		os.Exit(0)
	}

	// Add version info at startup
	log.Infof("redis-shake version %s", getVersionString())

	v := config.LoadConfig()

	log.Init(config.Opt.Advanced.LogLevel, config.Opt.Advanced.LogFile, config.Opt.Advanced.Dir)
	utils.ChdirAndAcquireFileLock()
	utils.SetNcpu()
	utils.SetPprofPort()
	luaRuntime := filter.NewFunctionFilter(config.Opt.Filter.Function)

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	// create reader
	var theReader reader.Reader
	switch {
	case v.IsSet("sync_reader"):
		opts := new(reader.SyncReaderOptions)
		defaults.SetDefaults(opts)
		err := v.UnmarshalKey("sync_reader", opts)
		if err != nil {
			log.Panicf("failed to read the SyncReader config entry. err: %v", err)
		}
		if opts.Cluster {
			log.Infof("create SyncClusterReader")
			log.Infof("* address (should be the address of one node in the Redis cluster): %s", opts.Address)
			log.Infof("* username: %s", opts.Username)
			log.Infof("* password: %s", strings.Repeat("*", len(opts.Password)))
			log.Infof("* tls: %v", opts.Tls)
			theReader = reader.NewSyncClusterReader(ctx, opts)
		} else {
			if opts.Sentinel.Address != "" {
				address := client.FetchAddressFromSentinel(&opts.Sentinel)
				opts.Address = address
			}
			log.Infof("create SyncStandaloneReader")
			log.Infof("* address: %s", opts.Address)
			log.Infof("* username: %s", opts.Username)
			log.Infof("* password: %s", strings.Repeat("*", len(opts.Password)))
			log.Infof("* tls: %v", opts.Tls)
			theReader = reader.NewSyncStandaloneReader(ctx, opts)
		}
	case v.IsSet("scan_reader"):
		opts := new(reader.ScanReaderOptions)
		defaults.SetDefaults(opts)
		err := v.UnmarshalKey("scan_reader", opts)
		if err != nil {
			log.Panicf("failed to read the ScanReader config entry. err: %v", err)
		}
		if opts.Cluster {
			log.Infof("create ScanClusterReader")
			log.Infof("* address (should be the address of one node in the Redis cluster): %s", opts.Address)
			log.Infof("* username: %s", opts.Username)
			log.Infof("* password: %s", strings.Repeat("*", len(opts.Password)))
			log.Infof("* tls: %v", opts.Tls)
			theReader = reader.NewScanClusterReader(ctx, opts)
		} else {
			log.Infof("create ScanStandaloneReader")
			log.Infof("* address: %s", opts.Address)
			log.Infof("* username: %s", opts.Username)
			log.Infof("* password: %s", strings.Repeat("*", len(opts.Password)))
			log.Infof("* tls: %v", opts.Tls)
			theReader = reader.NewScanStandaloneReader(ctx, opts)
		}
	case v.IsSet("rdb_reader"):
		opts := new(reader.RdbReaderOptions)
		defaults.SetDefaults(opts)
		err := v.UnmarshalKey("rdb_reader", opts)
		if err != nil {
			log.Panicf("failed to read the RdbReader config entry. err: %v", err)
		}
		theReader = reader.NewRDBReader(opts)
		log.Infof("create RdbReader: %v", opts.Filepath)
	case v.IsSet("aof_reader"):
		opts := new(reader.AOFReaderOptions)
		defaults.SetDefaults(opts)
		err := v.UnmarshalKey("aof_reader", opts)
		if err != nil {
			log.Panicf("failed to read the AOFReader config entry. err: %v", err)
		}
		theReader = reader.NewAOFReader(opts)
		log.Infof("create AOFReader: %v", opts.Filepath)
	default:
		log.Panicf("no reader config entry found")
	}
	// create writer
	var theWriter writer.Writer
	switch {
	case v.IsSet("redis_writer"):
		opts := new(writer.RedisWriterOptions)
		defaults.SetDefaults(opts)
		err := v.UnmarshalKey("redis_writer", opts)
		if err != nil {
			log.Panicf("failed to read the RedisStandaloneWriter config entry. err: %v", err)
		}
		if opts.OffReply && config.Opt.Advanced.RDBRestoreCommandBehavior == "panic" {
			log.Panicf("the RDBRestoreCommandBehavior can't be 'panic' when the server not reply to commands")
		}
		if opts.Cluster {
			log.Infof("create RedisClusterWriter")
			log.Infof("* address (should be the address of one node in the Redis cluster): %s", opts.Address)
			log.Infof("* username: %s", opts.Username)
			log.Infof("* password: %s", strings.Repeat("*", len(opts.Password)))
			log.Infof("* tls: %v", opts.Tls)
			theWriter = writer.NewRedisClusterWriter(ctx, opts)
		} else {
			if opts.Sentinel.Address != "" {
				address := client.FetchAddressFromSentinel(&opts.Sentinel)
				opts.Address = address
			}
			log.Infof("create RedisStandaloneWriter")
			log.Infof("* address: %s", opts.Address)
			log.Infof("* username: %s", opts.Username)
			log.Infof("* password: %s", strings.Repeat("*", len(opts.Password)))
			log.Infof("* tls: %v", opts.Tls)
			theWriter = writer.NewRedisStandaloneWriter(ctx, opts)
		}
		if config.Opt.Advanced.EmptyDBBeforeSync {
			// exec FLUSHALL command to flush db
			entry := entry.NewEntry()
			entry.Argv = []string{"FLUSHALL"}
			theWriter.Write(entry)
		}
	default:
		log.Panicf("no writer config entry found")
	}

	// create status
	if config.Opt.Advanced.StatusPort != 0 {
		status.Init(theReader, theWriter)
	}
	// create log entry count
	logEntryCount := status.EntryCount{
		ReadCount:  0,
		WriteCount: 0,
	}

	log.Infof("start syncing...")

	go waitShutdown(cancel)

	chrs := theReader.StartRead(ctx)

	theWriter.StartWrite(ctx)

	readerDone := make(chan bool)

	for _, chr := range chrs {
		go func(ch chan *entry.Entry) {
			for e := range ch {
				// calc arguments
				e.Parse()

				// update reader status
				if config.Opt.Advanced.StatusPort != 0 {
					status.AddReadCount(e.CmdName)
				}
				// update log entry count
				atomic.AddUint64(&logEntryCount.ReadCount, 1)

				// filter
				if !filter.Filter(e) {
					log.Debugf("skip command: %v", e)
					continue
				}

				// run lua function
				log.Debugf("function before: %v", e)
				entries := luaRuntime.RunFunction(e)
				log.Debugf("function after: %v", entries)

				// write
				for _, theEntry := range entries {
					theEntry.Parse()
					theWriter.Write(theEntry)

					// update writer status
					if config.Opt.Advanced.StatusPort != 0 {
						status.AddWriteCount(theEntry.CmdName)
					}
					// update log entry count
					atomic.AddUint64(&logEntryCount.WriteCount, 1)
				}
			}
			readerDone <- true
		}(chr)
	}

	// caluate ops and log to screen
	go func() {
		if config.Opt.Advanced.LogInterval <= 0 {
			log.Infof("log interval is 0, will not log to screen")
			return
		}
		ticker := time.NewTicker(time.Duration(config.Opt.Advanced.LogInterval) * time.Second)
		defer ticker.Stop()
		for range ticker.C {
			logEntryCount.UpdateOPS()
			log.Infof("%s, %s", logEntryCount.String(), theReader.StatusString())
		}
	}()

	ticker := time.NewTicker(1 * time.Second)
	defer ticker.Stop()
	readerCnt := len(chrs)
Loop:
	for {
		select {
		case done := <-readerDone:
			if done {
				readerCnt--
			}
			if readerCnt == 0 {
				break Loop
			}
		case <-ticker.C:
			pingEntry := entry.NewEntry()
			pingEntry.DbId = 0
			pingEntry.CmdName = "PING"
			pingEntry.Argv = []string{"PING"}
			pingEntry.Group = "connection"
			theWriter.Write(pingEntry)
		}
	}

	theWriter.Close()       // Wait for all writing operations to complete
	utils.ReleaseFileLock() // Release file lock
	log.Infof("all done")
}

func waitShutdown(cancel context.CancelFunc) {
	quitCh := make(chan os.Signal, 1)
	signal.Notify(quitCh, syscall.SIGINT, syscall.SIGTERM, syscall.SIGQUIT)
	sigTimes := 0
	for {
		sig := <-quitCh
		sigTimes++
		if sig != syscall.SIGINT {
			log.Infof("Got signal: %s.", sig)
		} else {
			log.Infof("Got signal: %s to exit. Press Ctrl+C again to force exit.", sig)
			if sigTimes >= 2 {
				os.Exit(0)
			}
			cancel()
		}

	}

}
