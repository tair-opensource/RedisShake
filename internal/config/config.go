package config

import (
	"bytes"
	"fmt"
	"github.com/pelletier/go-toml/v2"
	"io/ioutil"
	"os"
	"runtime"
)

type tomlSource struct {
	// sync mode
	Version          float32 `toml:"version"`
	Address          string  `toml:"address"`
	Username         string  `toml:"username"`
	Password         string  `toml:"password"`
	IsTLS            bool    `toml:"tls"`
	ElastiCachePSync string  `toml:"elasticache_psync"`

	// restore mode
	RDBFilePath string `toml:"rdb_file_path"`
}

type tomlTarget struct {
	Type     string  `toml:"type"`
	Version  float32 `toml:"version"`
	Username string  `toml:"username"`
	Address  string  `toml:"address"`
	Password string  `toml:"password"`
	IsTLS    bool    `toml:"tls"`
}

type tomlAdvanced struct {
	Dir string `toml:"dir"`

	Ncpu int `toml:"ncpu"`

	PprofPort   int `toml:"pprof_port"`
	MetricsPort int `toml:"metrics_port"`

	// log
	LogFile     string `toml:"log_file"`
	LogLevel    string `toml:"log_level"`
	LogInterval int    `toml:"log_interval"`

	// rdb restore
	RDBRestoreCommandBehavior string `toml:"rdb_restore_command_behavior"`

	// for writer
	PipelineCountLimit              uint64 `toml:"pipeline_count_limit"`
	TargetRedisClientMaxQuerybufLen uint64 `toml:"target_redis_client_max_querybuf_len"`
	TargetRedisProtoMaxBulkLen      uint64 `toml:"target_redis_proto_max_bulk_len"`
}

type tomlShakeConfig struct {
	Type     string
	Source   tomlSource
	Target   tomlTarget
	Advanced tomlAdvanced
}

var Config tomlShakeConfig

func init() {
	Config.Type = "sync"

	// source
	Config.Source.Version = 5.0
	Config.Source.Address = ""
	Config.Source.Username = ""
	Config.Source.Password = ""
	Config.Source.IsTLS = false
	Config.Source.ElastiCachePSync = ""
	// restore
	Config.Source.RDBFilePath = ""

	// target
	Config.Target.Type = "standalone"
	Config.Target.Version = 5.0
	Config.Target.Address = ""
	Config.Target.Username = ""
	Config.Target.Password = ""
	Config.Target.IsTLS = false

	// advanced
	Config.Advanced.Dir = "data"
	Config.Advanced.Ncpu = 4
	Config.Advanced.PprofPort = 0
	Config.Advanced.MetricsPort = 0
	Config.Advanced.LogFile = "redis-shake.log"
	Config.Advanced.LogLevel = "info"
	Config.Advanced.LogInterval = 5
	Config.Advanced.RDBRestoreCommandBehavior = "rewrite"
	Config.Advanced.PipelineCountLimit = 1024
	Config.Advanced.TargetRedisClientMaxQuerybufLen = 1024 * 1000 * 1000
	Config.Advanced.TargetRedisProtoMaxBulkLen = 512 * 1000 * 1000
}

func LoadFromFile(filename string) {

	buf, err := ioutil.ReadFile(filename)
	if err != nil {
		panic(err.Error())
	}

	decoder := toml.NewDecoder(bytes.NewReader(buf))
	decoder.SetStrict(true)
	err = decoder.Decode(&Config)
	if err != nil {
		missingError, ok := err.(*toml.StrictMissingError)
		if ok {
			panic(fmt.Sprintf("decode config error:\n%s", missingError.String()))
		}
		panic(err.Error())
	}

	// dir
	err = os.MkdirAll(Config.Advanced.Dir, os.ModePerm)
	if err != nil {
		panic(err.Error())
	}
	err = os.Chdir(Config.Advanced.Dir)
	if err != nil {
		panic(err.Error())
	}

	// cpu core
	var ncpu int
	if Config.Advanced.Ncpu == 0 {
		ncpu = runtime.NumCPU()
	} else {
		ncpu = Config.Advanced.Ncpu
	}
	runtime.GOMAXPROCS(ncpu)

	if Config.Source.Version < 2.8 {
		panic("source redis version must be greater than 2.8")
	}
	if Config.Target.Version < 2.8 {
		panic("target redis version must be greater than 2.8")
	}

	if Config.Type != "sync" && Config.Type != "restore" && Config.Type != "scan" {
		panic("type must be sync/restore/scan")
	}
}
