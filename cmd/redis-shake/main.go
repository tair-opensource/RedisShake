package main

import (
	"RedisShake/internal/commands"
	"RedisShake/internal/config"
	"RedisShake/internal/log"
	"RedisShake/internal/reader"
	"RedisShake/internal/status"
	"RedisShake/internal/transform"
	"RedisShake/internal/utils"
	"RedisShake/internal/writer"
	"github.com/mcuadros/go-defaults"
	_ "net/http/pprof"
)

func main() {
	v := config.LoadConfig()

	log.Init(config.Opt.Advanced.LogLevel, config.Opt.Advanced.LogFile)
	utils.ChdirAndAcquireFileLock()
	utils.SetNcpu()
	utils.SetPprofPort()
	transform.Init()

	// create reader
	var theReader reader.Reader
	if v.IsSet("SyncStandaloneReader") {
		opts := new(reader.SyncStandaloneReaderOptions)
		defaults.SetDefaults(opts)
		err := v.UnmarshalKey("SyncStandaloneReader", opts)
		if err != nil {
			log.Panicf("failed to read the SyncReader config entry. err: %v", err)
		}
		theReader = reader.NewSyncStandaloneReader(opts)
		log.Infof("create SyncStandaloneReader: %v", opts.Address)
	} else if v.IsSet("SyncClusterReader") {
		opts := new(reader.SyncClusterReaderOptions)
		defaults.SetDefaults(opts)
		err := v.UnmarshalKey("SyncClusterReader", opts)
		if err != nil {
			log.Panicf("failed to read the SyncReader config entry. err: %v", err)
		}
		theReader = reader.NewSyncClusterReader(opts)
		log.Infof("create SyncClusterReader: %v", opts.Address)
	} else if v.IsSet("ScanStandaloneReader") {
		opts := new(reader.ScanStandaloneReaderOptions)
		defaults.SetDefaults(opts)
		err := v.UnmarshalKey("ScanStandaloneReader", opts)
		if err != nil {
			log.Panicf("failed to read the ScanReader config entry. err: %v", err)
		}
		theReader = reader.NewScanStandaloneReader(opts)
		log.Infof("create ScanStandaloneReader: %v", opts.Address)
	} else if v.IsSet("ScanClusterReader") {
		opts := new(reader.ScanClusterReaderOptions)
		defaults.SetDefaults(opts)
		err := v.UnmarshalKey("ScanClusterReader", opts)
		if err != nil {
			log.Panicf("failed to read the ScanReader config entry. err: %v", err)
		}
		theReader = reader.NewScanClusterReader(opts)
		log.Infof("create ScanClusterReader: %v", opts.Address)
	} else if v.IsSet("RdbReader") {
		opts := new(reader.RdbReaderOptions)
		defaults.SetDefaults(opts)
		err := v.UnmarshalKey("RdbReader", opts)
		if err != nil {
			log.Panicf("failed to read the RdbReader config entry. err: %v", err)
		}
		theReader = reader.NewRDBReader(opts)
		log.Infof("create RdbReader: %v", opts.Filepath)
	} else {
		log.Panicf("no reader config entry found")
	}

	// create writer
	var theWriter writer.Writer
	if v.IsSet("RedisStandaloneWriter") {
		opts := new(writer.RedisStandaloneWriterOptions)
		defaults.SetDefaults(opts)
		err := v.UnmarshalKey("RedisStandaloneWriter", opts)
		if err != nil {
			log.Panicf("failed to read the RedisStandaloneWriter config entry. err: %v", err)
		}
		theWriter = writer.NewRedisStandaloneWriter(opts)
		log.Infof("create RedisStandaloneWriter: %v", opts.Address)
	} else if v.IsSet("RedisClusterWriter") {
		opts := new(writer.RedisClusterWriterOptions)
		defaults.SetDefaults(opts)
		err := v.UnmarshalKey("RedisClusterWriter", opts)
		if err != nil {
			log.Panicf("failed to read the RedisClusterWriter config entry. err: %v", err)
		}
		theWriter = writer.NewRedisClusterWriter(opts)
		log.Infof("create RedisClusterWriter: %v", opts.Address)
	} else {
		log.Panicf("no writer config entry found")
	}

	// create status
	status.Init(theReader, theWriter)

	ch := theReader.StartRead()
	for e := range ch {
		// calc arguments
		e.CmdName, e.Group, e.Keys = commands.CalcKeys(e.Argv)
		e.Slots = commands.CalcSlots(e.Keys)

		// filter
		code := transform.Transform(e)
		if code == transform.Allow {
			theWriter.Write(e)
			status.AddEntryCount(e.CmdName, true)
		} else if code == transform.Disallow {
			status.AddEntryCount(e.CmdName, false)
		} else {
			log.Panicf("error when run lua filter. entry: %s", e.String())
		}
	}

	theWriter.Close()       // Wait for all writing operations to complete
	utils.ReleaseFileLock() // Release file lock
	log.Infof("all done")
}
