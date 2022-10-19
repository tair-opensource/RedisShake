package main

import (
	"fmt"
	"time"
	"github.com/alibaba/RedisShake/internal/commands"
	"github.com/alibaba/RedisShake/internal/config"
	"github.com/alibaba/RedisShake/internal/filter"
	"github.com/alibaba/RedisShake/internal/log"
	"github.com/alibaba/RedisShake/internal/reader"
	"github.com/alibaba/RedisShake/internal/statistics"
	"github.com/alibaba/RedisShake/internal/writer"
	"net/http"
	_ "net/http/pprof"
	"os"
	"runtime"
)

func main() {
	if len(os.Args) < 2 || len(os.Args) > 3 {
		fmt.Println("Usage: redis-shake <config file> <lua file>")
		fmt.Println("Example: redis-shake config.toml lua.lua")
		os.Exit(1)
	}

	if len(os.Args) == 3 {
		luaFile := os.Args[2]
		filter.LoadFromFile(luaFile)
	}

	// load config
	configFile := os.Args[1]
	config.LoadFromFile(configFile)

	log.Init()
	log.Infof("GOOS: %s, GOARCH: %s", runtime.GOOS, runtime.GOARCH)
	log.Infof("Ncpu: %d, GOMAXPROCS: %d", config.Config.Advanced.Ncpu, runtime.GOMAXPROCS(0))
	log.Infof("pid: %d", os.Getpid())
	log.Infof("pprof_port: %d", config.Config.Advanced.PprofPort)
	if len(os.Args) == 2 {
		log.Infof("No lua file specified, will not filter any cmd.")
	}

	if config.Config.Advanced.PprofPort != 0 {
		go func() {
			err := http.ListenAndServe(fmt.Sprintf("localhost:%d", config.Config.Advanced.PprofPort), nil)
			if err != nil {
				log.PanicError(err)
			}
		}()
	}

	// create writer
	parallel_num := config.Config.Advanced.RDBRestoreParallelNum
	log.Infof("RDB parallel num: %d", parallel_num)

	theWriters := make([]writer.Writer, parallel_num)
	writer_ch := make(chan writer.Writer, parallel_num)

	target := &config.Config.Target
	switch config.Config.Target.Type {
	case "standalone":
		for i := uint64(0); i < parallel_num; i++ {
			theWriters[i] = writer.NewRedisWriter(target.Address, target.Username, target.Password, target.IsTLS)
			writer_ch <- theWriters[i]
		}
	case "cluster":
		for i := uint64(0); i < parallel_num; i++ {
			theWriters[i] = writer.NewRedisClusterWriter(target.Address, target.Username, target.Password, target.IsTLS)
			writer_ch <- theWriters[i]
		}
	default:
		log.Panicf("unknown target type: %s", target.Type)
	}

	// create reader
	source := &config.Config.Source
	var theReader reader.Reader
	if source.Type == "sync" {
		theReader = reader.NewPSyncReader(source.Address, source.Username, source.Password, source.IsTLS, source.ElastiCachePSync)
	} else if source.Type == "restore" {
		theReader = reader.NewRDBReader(source.RDBFilePath)
	} else {
		log.Panicf("unknown source type: %s", source.Type)
	}
	ch := theReader.StartRead()

	// start sync
	statistics.Init()
	id := uint64(0)
	for e := range ch {
		// calc arguments
		e.Id = id
		id++
		e.CmdName, e.Group, e.Keys = commands.CalcKeys(e.Argv)
		e.Slots = commands.CalcSlots(e.Keys)

		// filter
		code := filter.Filter(e)
		if code == filter.Allow {
			writer_e := e.CopyEntry()
			w := <- writer_ch
			go func() {
				w.Write(writer_e)
				writer_ch <- w
			} ()
			statistics.AddAllowEntriesCount()
		} else if code == filter.Disallow {
			// do something
			statistics.AddDisallowEntriesCount()
		} else {
			log.Panicf("error when run lua filter. entry: %s", e.ToString())
		}
	}

	time.Sleep(1 * time.Second) // for restore waiting for async response

	if config.Config.Source.Type == "sync" {
		var aof_writer writer.Writer
		target := &config.Config.Target
		switch config.Config.Target.Type {
		case "standalone":
			aof_writer = writer.NewRedisWriter(target.Address, target.Username,
											   target.Password, target.IsTLS)
		case "cluster":
			aof_writer = writer.NewRedisClusterWriter(target.Address, target.Username,
													  target.Password, target.IsTLS)
		}
		aof_ch := theReader.StartReadAOF()

		id := uint64(0)
		for e := range aof_ch {
			e.Id = id
			id++
			e.CmdName, e.Group, e.Keys = commands.CalcKeys(e.Argv)
			e.Slots = commands.CalcSlots(e.Keys)
			aof_writer.Write(e)
		}
	}
	log.Infof("finished.")
}
