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
	Address  string `toml:"address"`
	Password string `toml:"password"`
	IsTLS    bool   `toml:"tls"`
}

type tomlTarget struct {
	Type     string `toml:"type"`
	Address  string `toml:"address"`
	Password string `toml:"password"`
	IsTLS    bool   `toml:"tls"`
}

type tomlAdvanced struct {
	Dir string `toml:"dir"`

	Ncpu int `toml:"ncpu"`

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
	Source   tomlSource
	Target   tomlTarget
	Advanced tomlAdvanced
}

var Config tomlShakeConfig

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
}
