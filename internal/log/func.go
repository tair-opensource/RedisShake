package log

import (
	"os"

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
	frames := stack.Trace().TrimRuntime()
	for _, frame := range frames {
		logger.Warn().Msgf("%+v -> %n()", frame, frame)
	}
	logger.Error().Msgf(format, args...)
	os.Exit(1)
}
