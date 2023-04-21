package log

import (
	"RedisShake/internal/config"
	"fmt"
	"github.com/rs/zerolog"
	"os"
	"path/filepath"
)

var logger zerolog.Logger

func Init(level string, file string) {

	// log level
	switch level {
	case "debug":
		zerolog.SetGlobalLevel(zerolog.DebugLevel)
	case "info":
		zerolog.SetGlobalLevel(zerolog.InfoLevel)
	case "warn":
		zerolog.SetGlobalLevel(zerolog.WarnLevel)
	default:
		panic(fmt.Sprintf("unknown log level: %s", level))
	}

	// dir
	dir, err := filepath.Abs(config.Opt.Advanced.Dir)
	if err != nil {
		panic(fmt.Sprintf("failed to determine current directory: %v", err))
	}
	err = os.RemoveAll(dir)
	if err != nil {
		panic(fmt.Sprintf("remove dir failed. dir=[%s], error=[%v]", dir, err))
	}
	err = os.MkdirAll(dir, 0777)
	if err != nil {
		panic(fmt.Sprintf("mkdir failed. dir=[%s], error=[%v]", dir, err))
	}
	path := filepath.Join(dir, file)

	// log file
	consoleWriter := zerolog.ConsoleWriter{Out: os.Stdout, TimeFormat: "2006-01-02 15:04:05"}
	fileWriter, err := os.OpenFile(path, os.O_WRONLY|os.O_CREATE|os.O_APPEND, 0666)
	if err != nil {
		panic(fmt.Sprintf("open log file failed. file=[%s], err=[%s]", path, err))
	}
	multi := zerolog.MultiLevelWriter(consoleWriter, fileWriter)
	logger = zerolog.New(multi).With().Timestamp().Logger()
	Infof("log_level: [%v], log_file: [%v]", level, path)
}
