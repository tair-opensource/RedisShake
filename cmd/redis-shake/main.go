package main

import (
	"fmt"
	"net/http"
	_ "net/http/pprof"
	"os"
	"runtime"

	"github.com/alibaba/RedisShake/internal/commands"
	"github.com/alibaba/RedisShake/internal/config"
	"github.com/alibaba/RedisShake/internal/filter"
	"github.com/alibaba/RedisShake/internal/log"
	"github.com/alibaba/RedisShake/internal/reader"
	"github.com/alibaba/RedisShake/internal/statistics"
	"github.com/alibaba/RedisShake/internal/writer"
)

func main() {
	if len(os.Args) < 2 || len(os.Args) > 3 {
		fmt.Println("Usage: redis-shake <config file> <filter file>")
		fmt.Println("Example: redis-shake config.toml filter.lua")
		os.Exit(1)
	}

	// load filter file
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

	// start pprof
	if config.Config.Advanced.PprofPort != 0 {
		go func() {
			err := http.ListenAndServe(fmt.Sprintf("localhost:%d", config.Config.Advanced.PprofPort), nil)
			if err != nil {
				log.PanicError(err)
			}
		}()
	}

	// start statistics
	if config.Config.Advanced.MetricsPort != 0 {
		statistics.Metrics.Address = config.Config.Source.Address
		go func() {
			log.Infof("metrics url: http://localhost:%d", config.Config.Advanced.MetricsPort)
			mux := http.NewServeMux()
			mux.HandleFunc("/", statistics.Handler)
			err := http.ListenAndServe(fmt.Sprintf("localhost:%d", config.Config.Advanced.MetricsPort), mux)
			if err != nil {
				log.PanicError(err)
			}
		}()
	}

	// create writer
	var theWriter writer.Writer
	target := &config.Config.Target
	switch config.Config.Target.Type {
	case "standalone":
		theWriter = writer.NewRedisWriter(target.Address, target.Username, target.Password, target.IsTLS)
	case "cluster":
		theWriter = writer.NewRedisClusterWriter(target.Address, target.Username, target.Password, target.IsTLS)
	default:
		log.Panicf("unknown target type: %s", target.Type)
	}

	// create reader
	source := &config.Config.Source
	var theReader reader.Reader
	if config.Config.Type == "sync" {
		theReader = reader.NewPSyncReader(source.Address, source.Username, source.Password, source.IsTLS, source.ElastiCachePSync)
	} else if config.Config.Type == "restore" { // TODO: new aof reader
		if source.RDBFilePath != "" {
			theReader = reader.NewRDBReader(source.RDBFilePath)
		} else {
			theReader = reader.NewAOFReader(source.AOFFilePath) // 如果是mp-aof 用户传入 manifest文件的地址 ，其他的传递aof的地址
		}

	} else if config.Config.Type == "scan" {
		theReader = reader.NewScanReader(source.Address, source.Username, source.Password, source.IsTLS)
	} else {
		log.Panicf("unknown source type: %s", config.Config.Type)
	}
	ch := theReader.StartRead()

	// start sync
	statistics.Init()
	id := uint64(0)
	for e := range ch {
		statistics.UpdateInQueueEntriesCount(uint64(len(ch)))
		// calc arguments
		e.Id = id
		id++
		e.CmdName, e.Group, e.Keys = commands.CalcKeys(e.Argv)
		e.Slots = commands.CalcSlots(e.Keys)

		// filter
		code := filter.Filter(e)
		statistics.UpdateEntryId(e.Id)
		if code == filter.Allow {
			theWriter.Write(e)
			statistics.AddAllowEntriesCount()
		} else if code == filter.Disallow {
			// do something
			statistics.AddDisallowEntriesCount()
		} else {
			log.Panicf("error when run lua filter. entry: %s", e.ToString())
		}
	}
	theWriter.Close()
	log.Infof("finished.")
}
