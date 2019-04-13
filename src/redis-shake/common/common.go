package utils

import (
	"net"
	"fmt"
	"strings"

	"pkg/libs/bytesize"

	logRotate "gopkg.in/natefinch/lumberjack.v2"
)

const (
	GolangSecurityTime = "2006-01-02T15:04:05Z"
	// GolangSecurityTime = "2006-01-02 15:04:05"
	ReaderBufferSize = bytesize.MB * 32
	WriterBufferSize = bytesize.MB * 8
)

var (
	Version = "$"
	LogRotater *logRotate.Logger
	StartTime string
)

// read until hit the end of RESP: "\r\n"
func ReadRESPEnd(c net.Conn) (string, error) {
	var ret string
	for {
		b := make([]byte, 1)
		if _, err := c.Read(b); err != nil {
			return "", fmt.Errorf("read error[%v], current return[%s]", err, ret)
		}

		ret += string(b)
		if strings.HasSuffix(ret, "\r\n") {
			break
		}
	}
	return ret, nil
}

func RemoveRESPEnd(input string) string {
	length := len(input)
	if length >= 2 {
		return input[: length - 2]
	}
	return input
}