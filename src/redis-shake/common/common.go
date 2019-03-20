package utils

import(
	logRotate "gopkg.in/natefinch/lumberjack.v2"
	"pkg/libs/bytesize"
)

const(
	GolangSecurityTime = "2006-01-02T15:04:05Z"
	// GolangSecurityTime = "2006-01-02 15:04:05"
	ReaderBufferSize = bytesize.MB * 32
	WriterBufferSize = bytesize.MB * 8
)

var(
	Version = "$"
	LogRotater *logRotate.Logger
	StartTime string
)