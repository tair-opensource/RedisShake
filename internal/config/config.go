package config

import (
	"fmt"
	"os"
	"strings"

	"RedisShake/internal/log"

	"github.com/mcuadros/go-defaults"
	"github.com/rs/zerolog"
	"github.com/spf13/viper"
)

type AdvancedOptions struct {
	Dir string `mapstructure:"dir" default:"data"`

	Ncpu int `mapstructure:"ncpu" default:"0"`

	PprofPort  int `mapstructure:"pprof_port" default:"0"`
	StatusPort int `mapstructure:"status_port" default:"6479"`

	// log
	LogFile     string `mapstructure:"log_file" default:"shake.log"`
	LogLevel    string `mapstructure:"log_level" default:"info"`
	LogInterval int    `mapstructure:"log_interval" default:"5"`

	// redis-shake gets key and value from rdb file, and uses RESTORE command to
	// create the key in target redis. Redis RESTORE will return a "Target key name
	// is busy" error when key already exists. You can use this configuration item
	// to change the default behavior of restore:
	// panic:   redis-shake will stop when meet "Target key name is busy" error.
	// rewrite: redis-shake will replace the key with new value.
	// ignore:  redis-shake will skip restore the key when meet "Target key name is busy" error.
	RDBRestoreCommandBehavior string `mapstructure:"rdb_restore_command_behavior" default:"panic"`

	PipelineCountLimit              uint64 `mapstructure:"pipeline_count_limit" default:"1024"`
	TargetRedisClientMaxQuerybufLen int64  `mapstructure:"target_redis_client_max_querybuf_len" default:"1024000000"`
	TargetRedisProtoMaxBulkLen      uint64 `mapstructure:"target_redis_proto_max_bulk_len" default:"512000000"`

	AwsPSync string `mapstructure:"aws_psync" default:""` // 10.0.0.1:6379@nmfu2sl5osync,10.0.0.1:6379@xhma21xfkssync
}

type ModuleOptions struct {
	TargetMBbloomVersion int `mapstructure:"target_mbbloom_version" default:"0"` // v1.0.0 <=> 10000
}

func (opt *AdvancedOptions) GetPSyncCommand(address string) string {
	items := strings.Split(opt.AwsPSync, ",")
	for _, item := range items {
		if strings.HasPrefix(item, address) {
			return strings.Split(item, "@")[1]
		}
	}
	log.Panicf("can not find aws psync command. address=[%s],aws_psync=[%s]", address, opt.AwsPSync)
	return ""
}

type ShakeOptions struct {
	Function string `mapstructure:"function" default:""`
	Advanced AdvancedOptions
	Module   ModuleOptions
}

var Opt ShakeOptions

func LoadConfig() *viper.Viper {
	defaults.SetDefaults(&Opt)

	v := viper.New()
	if len(os.Args) > 2 {
		fmt.Println("Usage: redis-shake [config file]")
		fmt.Println("Example: ")
		fmt.Println(" 		redis-shake sync.toml # load config from sync.toml")
		fmt.Println("		redis-shake 		  # load config from environment variables")
		os.Exit(1)
	}
	consoleWriter := zerolog.ConsoleWriter{Out: os.Stdout, TimeFormat: "2006-01-02 15:04:05"}
	logger := zerolog.New(consoleWriter).With().Timestamp().Logger()
	// load config from file
	if len(os.Args) == 2 {
		logger.Info().Msgf("load config from file: %s", os.Args[1])
		configFile := os.Args[1]
		v.SetConfigFile(configFile)
		err := v.ReadInConfig()
		if err != nil {
			panic(err)
		}
	}

	// load config from environment variables
	if len(os.Args) == 1 {
		logger.Warn().Msg("load config from environment variables")
		v.SetConfigType("env")
		v.AutomaticEnv()
	}

	// unmarshal config
	err := v.Unmarshal(&Opt)
	if err != nil {
		panic(err)
	}
	return v
}
