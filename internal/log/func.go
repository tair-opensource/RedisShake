package log

import (
	"fmt"
	"os"
	"strings"

	"github.com/go-stack/stack"
)

func Debugf(format string, args ...interface{}) {
	logger.Debug().Msgf(format, args...)
}

func Infof(format string, args ...interface{}) {
	logger.Info().Msgf(format, args...)
}

func Warnf(format string, args ...interface{}) {
	logger.Warn().Msgf(format, args...)
}

func Panicf(format string, args ...interface{}) {
	frames := stack.Trace()
	errMsg := fmt.Sprintf(format, args...)
	for _, frame := range frames {
		frameStr := fmt.Sprintf("%+v", frame)
		if strings.HasPrefix(frameStr, "redis-shake/main.go") {
			frameStr = "RedisShake/cmd/" + frameStr
		}
		if strings.HasPrefix(frameStr, "RedisShake/internal/log/func") {
			continue
		}
		errMsg += fmt.Sprintf("\n\t\t\t%v -> %n()", frameStr, frame)
	}
	logger.Error().Msg(errMsg)
	os.Exit(1)
}
