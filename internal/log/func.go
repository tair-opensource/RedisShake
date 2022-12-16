package log

import (
	"runtime/debug"
	"strings"
)

func Assert(condition bool, msg string) {
	if !condition {
		Panicf("Assert failed: %s", msg)
	}
}

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
	stack := string(debug.Stack())
	stack = strings.ReplaceAll(stack, "\n\t", "]<-")
	stack = strings.ReplaceAll(stack, "\n", "  [")
	logger.Info().Msg(stack)

	logger.Panic().Msgf(format, args...)
}

func PanicError(err error) {
	Panicf(err.Error())
}

func PanicIfError(err error) {
	if err != nil {
		PanicError(err)
	}
}
