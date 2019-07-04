package utils

import (
	"bytes"
	"fmt"
	"net"
	"strings"
	"reflect"
	"unsafe"
	"encoding/binary"

	"pkg/libs/bytesize"
	"redis-shake/configure"

	logRotate "gopkg.in/natefinch/lumberjack.v2"
	"github.com/cupcake/rdb/crc64"
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
	LogLevelDebug = "debug"
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
	RDBVersion       uint = 9 // 9 for 5.0
)

const (
	KB = 1024
	MB = 1024 * KB
	GB = 1024 * MB
	TB = 1024 * GB
	PB = 1024 * TB
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

func String2Bytes(s string) []byte {
	sh := (*reflect.StringHeader)(unsafe.Pointer(&s))
	bh := reflect.SliceHeader{
		Data: sh.Data,
		Len:  sh.Len,
		Cap:  sh.Len,
	}
	return *(*[]byte)(unsafe.Pointer(&bh))
}

func Bytes2String(b []byte) string {
	return *(*string)(unsafe.Pointer(&b))
}

func CheckVersionChecksum(d []byte) (uint, uint64, error) {
	/* Write the footer, this is how it looks like:
	 * ----------------+---------------------+---------------+
	 * ... RDB payload | 2 bytes RDB version | 8 bytes CRC64 |
	 * ----------------+---------------------+---------------+
	 * RDB version and CRC are both in little endian.
	 */
	length := len(d)
	if length < 10 {
		return 0, 0, fmt.Errorf("rdb: invalid dump length")
	}

	footer := length - 10
	rdbVersion := uint((d[footer + 1] << 8) | d[footer])
	if rdbVersion > RDBVersion {
		return 0, 0, fmt.Errorf("current version[%v] > RDBVersion[%v]", rdbVersion, RDBVersion)
	}

	checksum := binary.LittleEndian.Uint64(d[length - 8:])
	digest := crc64.Digest(d[: length - 8])
	if checksum != digest {
		return 0, 0, fmt.Errorf("rdb: invalid CRC checksum[%v] != digest[%v]", checksum, digest)
	}

	return rdbVersion, checksum, nil
}