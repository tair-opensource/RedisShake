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
	"strconv"
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

	TencentCluster = "tencent_cluster"
	AliyunCluster  = "aliyun_cluster"
	UCloudCluster  = "ucloud_cluster"
	CodisCluster   = "codis_cluster"

	CoidsErrMsg = "ERR backend server 'server' not found"

	CheckpointKey     = "redis-shake-checkpoint"
	CheckpointOffset  = "offset"
	CheckpointRunId   = "runid"
	CheckpointVersion = "version"
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
	if conf.Options.Type == conf.TypeSync || conf.Options.Type == conf.TypeRump || conf.Options.Type == conf.TypeDump {
		return len(conf.Options.SourceAddressList)
	} else if conf.Options.Type == conf.TypeDecode || conf.Options.Type == conf.TypeRestore {
		return len(conf.Options.SourceRdbInput)
	}
	return 0
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

func GetMetric(input int64) string {
	switch {
	case input > PB:
		return fmt.Sprintf("%.2fPB", float64(input) / PB)
	case input > TB:
		return fmt.Sprintf("%.2fTB", float64(input) / TB)
	case input > GB:
		return fmt.Sprintf("%.2fGB", float64(input) / GB)
	case input > MB:
		return fmt.Sprintf("%.2fMB", float64(input) / MB)
	case input > KB:
		return fmt.Sprintf("%.2fKB", float64(input) / KB)
	default:
		return fmt.Sprintf("%dB", input)
	}
}

/*
 * compare the version with given level. e.g.,
 * 2.0.1, 2.0.3, level = 2 => equal: 0
 * 2.0.1, 2.0.3, level = 3 => smaller: 1
 * 3.1.1, 2.1 level = 2 => bigger: 2
 * 3, 3.2, level = 2 => smaller: 1
 * 3.a, 3.2, level = 2 => unknown: 3
 */
func CompareVersion(a, b string, level int) int {
	if level <= 0 {
		return 0
	}

	var err error
	as := strings.Split(a, ".")
	bs := strings.Split(b, ".")
	for l := 0; l < level; l++ {
		var av, bv int
		// parse av
		if l > len(as) {
			av = 0
		} else {
			av, err = strconv.Atoi(as[l])
			if err != nil {
				return 3
			}
		}

		// parse bv
		if l > len(bs) {
			bv = 0
		} else {
			bv, err = strconv.Atoi(bs[l])
			if err != nil {
				return 3
			}
		}

		if av > bv {
			return 2
		} else if av < bv {
			return 1
		}
	}

	return 0
}

// compare two unordered list. return true means equal.
func CompareUnorderedList(a, b []string) bool {
	if len(a) != len(b) {
		return false
	}

	if len(a) == 0 {
		return true
	}

	setA := map[string]struct{}{}

	for _, x := range a {
		setA[x] = struct{}{}
	}

	for _, x := range b {
		if _, ok := setA[x]; !ok {
			return false
		}
	}

	return true
}