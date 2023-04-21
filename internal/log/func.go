package log

import (
	"fmt"
	"github.com/go-stack/stack"
	"os"
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
	msgs := fmt.Sprintf(format, args...)
	for _, frame := range frames {
		msgs += fmt.Sprintf("\n%+v -> %n()", frame, frame)
	}
	logger.Error().Msg(msgs)
	os.Exit(1)
}
