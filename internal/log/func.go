package log

import (
	"fmt"
	"github.com/rs/zerolog"
)

func Assert(condition bool, msg string) {
	if !condition {
		Panicf("Assert failed: %s", msg)
	}
}

func Debugf(format string, args ...interface{}) {
	logFinally(logger.Debug(), format, args...)
}

func Infof(format string, args ...interface{}) {
	logFinally(logger.Info(), format, args...)
}

func Warnf(format string, args ...interface{}) {
	logFinally(logger.Warn(), format, args...)
}

func Panicf(format string, args ...interface{}) {
	logFinally(logger.Panic(), format, args...)
}

func PanicError(err error) {
	Panicf(err.Error())
}

func PanicIfError(err error) {
	if err != nil {
		PanicError(err)
	}
}

func logFinally(event *zerolog.Event, format string, args ...interface{}) {
	str := fmt.Sprintf(format, args...)
	//inxTrunct := -1
	//keyStart := -1
	//valueStart := -1
	//key := ""
	//value := ""
	//for inx, b := range str {
	//	switch b {
	//	case ' ':
	//		keyStart = inx + 1
	//	case '=':
	//		if keyStart == -1 {
	//			continue
	//		}
	//		key = str[keyStart:inx]
	//	case '[':
	//		valueStart = inx + 1
	//	case ']':
	//		if valueStart == -1 {
	//			continue
	//		}
	//		value = str[valueStart:inx]
	//		if key == "" || value == "" {
	//			continue
	//		}
	//		event = event.Str(key, value)
	//		if inxTrunct == -1 {
	//			inxTrunct = keyStart - 1
	//		}
	//		keyStart = -1
	//		valueStart = -1
	//	}
	//}
	//if inxTrunct != -1 {
	//	str = str[:inxTrunct]
	//}
	event.Msg(str)
}
