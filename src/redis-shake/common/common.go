package utils

import (
	"bytes"
	"fmt"
	"net"
	"strings"

	"pkg/libs/bytesize"
	"redis-shake/configure"

	logRotate "gopkg.in/natefinch/lumberjack.v2"
)

const (
	GolangSecurityTime = "2006-01-02T15:04:05Z"
	// GolangSecurityTime = "2006-01-02 15:04:05"
	ReaderBufferSize = bytesize.MB * 32
	WriterBufferSize = bytesize.MB * 8

	LogLevelNone  = "none"
	LogLevelError = "error"
	LogLevelWarn  = "warn"
	LogLevelInfo  = "info"
	LogLevelAll   = "all"

	TencentCluster = "tencent_cluster"
	AliyunCluster  = "aliyun_cluster"
	UCloudCluster  = "ucloud_cluster"
)

var (
	Version          = "$"
	LogRotater       *logRotate.Logger
	StartTime        string
	TargetRoundRobin int
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
		return input[:length-2]
	}
	return input
}

func ParseInfo(content []byte) map[string]string {
	result := make(map[string]string, 10)
	lines := bytes.Split(content, []byte("\r\n"))
	for i := 0; i < len(lines); i++ {
		items := bytes.SplitN(lines[i], []byte(":"), 2)
		if len(items) != 2 {
			continue
		}
		result[string(items[0])] = string(items[1])
	}
	return result
}

func GetTotalLink() int {
	if len(conf.Options.SourceAddressList) != 0 {
		return len(conf.Options.SourceAddressList)
	} else {
		return len(conf.Options.RdbInput)
	}
}

func PickTargetRoundRobin(n int) int {
	defer func() {
		TargetRoundRobin = (TargetRoundRobin + 1) % n
	}()
	return TargetRoundRobin
}
