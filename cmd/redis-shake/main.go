package main

import (
	"context"
	_ "net/http/pprof"
	"os"
	"os/signal"
	"syscall"

	"RedisShake/internal/config"
	"RedisShake/internal/entry"
	"RedisShake/internal/function"
	"RedisShake/internal/log"
	"RedisShake/internal/reader"
	"RedisShake/internal/status"
	"RedisShake/internal/utils"
	"RedisShake/internal/writer"

	"github.com/mcuadros/go-defaults"
)

func main() {
	v := config.LoadConfig()

	log.Init(config.Opt.Advanced.LogLevel, config.Opt.Advanced.LogFile, config.Opt.Advanced.Dir)
	utils.ChdirAndAcquireFileLock()
	utils.SetNcpu()
	utils.SetPprofPort()
	luaRuntime := function.New(config.Opt.Function)

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	// create reader
	var theReader reader.Reader
	if v.IsSet("sync_reader") {
		opts := new(reader.SyncReaderOptions)
		defaults.SetDefaults(opts)
		err := v.UnmarshalKey("sync_reader", opts)
		if err != nil {
			log.Panicf("failed to read the SyncReader config entry. err: %v", err)
		}
		if opts.Cluster {
			theReader = reader.NewSyncClusterReader(ctx, opts)
			log.Infof("create SyncClusterReader: %v", opts.Address)
		} else {
			theReader = reader.NewSyncStandaloneReader(ctx, opts)
			log.Infof("create SyncStandaloneReader: %v", opts.Address)
		}
	} else if v.IsSet("scan_reader") {
		opts := new(reader.ScanReaderOptions)
		defaults.SetDefaults(opts)
		err := v.UnmarshalKey("scan_reader", opts)
		if err != nil {
			log.Panicf("failed to read the ScanReader config entry. err: %v", err)
		}
		if opts.Cluster {
			theReader = reader.NewScanClusterReader(ctx, opts)
			log.Infof("create ScanClusterReader: %v", opts.Address)
		} else {
			theReader = reader.NewScanStandaloneReader(ctx, opts)
			log.Infof("create ScanStandaloneReader: %v", opts.Address)
		}
	} else if v.IsSet("rdb_reader") {
		opts := new(reader.RdbReaderOptions)
		defaults.SetDefaults(opts)
		err := v.UnmarshalKey("rdb_reader", opts)
		if err != nil {
			log.Panicf("failed to read the RdbReader config entry. err: %v", err)
		}
		theReader = reader.NewRDBReader(opts)
		log.Infof("create RdbReader: %v", opts.Filepath)
	} else if v.IsSet("aof_reader") {
		opts := new(reader.AOFReaderOptions)
		defaults.SetDefaults(opts)
		err := v.UnmarshalKey("aof_reader", opts)
		if err != nil {
			log.Panicf("failed to read the AOFReader config entry. err: %v", err)
		}
		theReader = reader.NewAOFReader(opts)
		log.Infof("create AOFReader: %v", opts.Filepath)
	} else {
		log.Panicf("no reader config entry found")
	}

	// create writer
	var theWriter writer.Writer
	if v.IsSet("redis_writer") {
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
			theWriter = writer.NewRedisClusterWriter(ctx, opts)
			log.Infof("create RedisClusterWriter: %v", opts.Address)
		} else {
			theWriter = writer.NewRedisStandaloneWriter(ctx, opts)
			log.Infof("create RedisStandaloneWriter: %v", opts.Address)
		}
		if config.Opt.Advanced.EmptyDBBeforeSync {
			// exec FLUSHALL command to flush db
			entry := entry.NewEntry()
			entry.Argv = []string{"FLUSHALL"}
			theWriter.Write(entry)
		}
	} else {
		log.Panicf("no writer config entry found")
	}

	// create status
	status.Init(theReader, theWriter)

	log.Infof("start syncing...")

	ch := theReader.StartRead(ctx)
	go waitShutdown(cancel)

	for e := range ch {
		// calc arguments
		e.Parse()
		status.AddReadCount(e.CmdName)

		// filter
		log.Debugf("function before: %v", e)
		entries := luaRuntime.RunFunction(e)
		log.Debugf("function after: %v", entries)

		for _, entry := range entries {
			entry.Parse()
			theWriter.Write(entry)
			status.AddWriteCount(entry.CmdName)
		}
	}

	theWriter.Close()       // Wait for all writing operations to complete
	utils.ReleaseFileLock() // Release file lock
	log.Infof("all done")
}

func waitShutdown(cancel context.CancelFunc) {
	quitCh := make(chan os.Signal, 1)
	signal.Notify(quitCh, syscall.SIGINT, syscall.SIGTERM, syscall.SIGQUIT)
	sig := <-quitCh
	log.Infof("Got signal: %s to exit.", sig)
	cancel()
}
