package oplog

import (
	"bytes"
	"encoding/binary"
	"fmt"
	"strconv"
	"strings"
	"time"
	"unsafe"
)

type errorOplog struct {
	s string
}

func (e *errorOplog) Error() string {
	return e.s
}

/*
struct OplogHeader {
    uint32_t version:8; // version of oplog
    uint32_t cmd_num:4; / number of commands in one oplog, currently 2 or 3
    uint32_t cmd_flag:4;
    uint32_t dbid:16;
    int32_t timestamp;
    int64_t server_id;
    int64_t opid;
    int64_t src_opid; // opid of source redis
};
*/
type OplogHeader struct {
	Version   int8
	Status    uint8
	DbId      int16
	Timestamp int32
	ServerId  uint64
	Opid      int64
	SrcOpid   int64
}

func (p *OplogHeader) IsOPLogDelByExpire() bool {
	// #define REDIS_OPLOG_DEL_BY_EVICTION_FLAG (1<<7)
	// #define REDIS_OPLOG_DEL_BY_EXPIRE_FLAG (1<<6)
	return p.Status&(uint8(1)<<6) != 0
}

func (p *OplogHeader) IsOPLogDelByEviction() bool {
	// #define REDIS_OPLOG_DEL_BY_EVICTION_FLAG (1<<7)
	// #define REDIS_OPLOG_DEL_BY_EXPIRE_FLAG (1<<6)
	return p.Status&(uint8(1)<<7) != 0
}

func (p *OplogHeader) GetCmdNum() int {
	return int(p.Status & uint8(0xF0))
}

func (p OplogHeader) String() string {
	t := time.Unix(int64(p.Timestamp), 0).Local()
	return fmt.Sprintf("{Version:%d, Status:%d, Dbid:%d, Timestamp:%d, Time:%s, ServerId:%d, Opid:%d, SrcOpid:%d, IsDelByExpire:%v, IsDelByEviction:%v}",
		p.Version, p.Status, p.DbId, p.Timestamp, t.Format(time.RFC3339), p.ServerId, p.Opid, p.SrcOpid, p.IsOPLogDelByExpire(), p.IsOPLogDelByEviction())
}

const OplogHeaderSize = unsafe.Sizeof(OplogHeader{})

var OplogHeaderPrefix = []byte("*2\r\n$6\r\nOPINFO\r\n$32\r\n")

type Oplog struct {
	FullContent []byte
	Header      OplogHeader
	Cmd         []RedisCmd
}

func (p *Oplog) CmdContent() []byte {
	return p.FullContent[len(OplogHeaderPrefix)+32+2:]
}

func (p *Oplog) IsOPLogDelByExpire() bool {
	return p.Header.IsOPLogDelByExpire()
}

func (p *Oplog) IsOPLogDelByEviction() bool {
	return p.Header.IsOPLogDelByEviction()
}

// parseLen parses bulk string and array lengths.
func parseLen(p []byte) (int64, error) {
	if len(p) == 0 {
		return -1, &errorOplog{"protocal error, malformed length"}
	}

	if p[0] == '-' && len(p) == 2 && p[1] == '1' {
		// handle $-1 and $-1 null replies.
		return -1, nil
	}

	var n int64
	for _, b := range p {
		n *= 10
		if b < '0' || b > '9' {
			return -1, &errorOplog{"protocal error, illegal bytes in length"}
		}
		n += int64(b - '0')
	}

	return n, nil
}

func parseCmd(fullcontent []byte) (*RedisCmd, []byte, error) {
	if fullcontent[0] != '*' {
		return nil, fullcontent, &errorOplog{fmt.Sprintf("protocal error, cmd not start with '*': %s", string(fullcontent))}
	}
	p := bytes.IndexByte(fullcontent, '\n')
	if p == -1 || fullcontent[p-1] != '\r' {
		return nil, fullcontent, &errorOplog{fmt.Sprintf("protocal error, expect line terminator: %s", string(fullcontent))}
	}

	arrayLen, err := parseLen(fullcontent[1 : p-1])
	if err != nil {
		return nil, fullcontent, err
	}

	reply := RedisCmd{Args: make([][]byte, 0, arrayLen)}
	left := fullcontent[p+1:]
	for i := int64(0); i < arrayLen; i++ {
		if left[0] != '$' {
			return nil, fullcontent, &errorOplog{fmt.Sprintf("protocal error, expect '$': %s", string(left))}
		}
		endIndex := bytes.IndexByte(left, '\n')
		if endIndex == -1 || left[endIndex-1] != '\r' {
			return nil, fullcontent, &errorOplog{fmt.Sprintf("protocal error, expect line terminator: %s", string(left))}
		}
		bulkLen, err := parseLen(left[1 : endIndex-1])
		if err != nil {
			return nil, fullcontent, err
		}
		reply.Args = append(reply.Args, left[endIndex+1:endIndex+1+int(bulkLen)])
		left = left[endIndex+1+int(bulkLen)+2:]
	}

	reply.CmdCode = ParseCommandStrToCode(reply.Args[0])
	return &reply, left, nil
}

func ParseOplog(fullcontent []byte) (*Oplog, error) {
	oplog := &Oplog{FullContent: fullcontent}
	left := fullcontent
	for {
		var redisCmd *RedisCmd
		var err error
		redisCmd, left, err = parseCmd(left)
		if err != nil {
			return nil, err
		}
		oplog.Cmd = append(oplog.Cmd, *redisCmd)
		if len(left) == 0 {
			break
		}
	}

	if oplog.Cmd[0].CmdCode != OPINFO {
		return nil, &errorOplog{fmt.Sprintf("oplog error, first cmd is not OPINFO, but is %s", string(oplog.Cmd[0].Args[0]))}
	}

	if len(oplog.Cmd[0].Args[1]) != int(OplogHeaderSize) {
		return nil, &errorOplog{fmt.Sprintf("oplog error, len(oplog.Cmd[0].args[1]) %d != int(OplogHeaderSize) %d",
			len(oplog.Cmd[0].Args[1]), int(OplogHeaderSize))}
	}

	dest := (*(*[1<<31 - 1]byte)(unsafe.Pointer(&oplog.Header)))[:OplogHeaderSize]
	copy(dest, oplog.Cmd[0].Args[1])
	return oplog, nil
}

func ParseOplogHeader(content []byte) (*OplogHeader, error) {
	if len(content) != int(OplogHeaderSize) {
		return nil, &errorOplog{fmt.Sprintf("parse oplog header failed, len(content) %d != int(OplogHeaderSize) %d",
			len(content), int(OplogHeaderSize))}
	}
	var reply OplogHeader
	dest := (*(*[1<<31 - 1]byte)(unsafe.Pointer(&reply)))[:OplogHeaderSize]
	copy(dest, content)
	return &reply, nil
}

type FakeOplogMaker struct {
	header         OplogHeader
	readCmdContent []byte
}

func NewFakeOplogMaker(serverId uint64) *FakeOplogMaker {
	// 必须选SCRIPT LOAD，优点如下：
	//   1. 不涉及key，不会和用户的key冲突
	//   2. 不涉及事务或lua中的eval/evalsha, 不会干扰主从版升级集群版
	//   3. 对于集群版，proxy会把"OPINFO"和"SCRIPT LOAD"命令转发到所有后端redis db，省事
	var args [3]string
	args[0] = "SCRIPT"
	args[1] = "LOAD"
	args[2] = "return 0"

	var buf bytes.Buffer
	buf.WriteString("*3\r\n")
	for _, arg := range args[:] {
		buf.WriteString(fmt.Sprintf("$%d\r\n%s\r\n", len(arg), arg))
	}

	return &FakeOplogMaker{
		header: OplogHeader{
			Version:   1,
			Status:    2, // 8bit, [0-4] command num, [5-7] flags
			DbId:      0,
			Timestamp: 0,
			ServerId:  serverId,
			Opid:      0,
			SrcOpid:   -1,
		},
		readCmdContent: buf.Bytes(),
	}
}

func (p *FakeOplogMaker) MakeFakeOplog(opid int64) (*Oplog, error) {
	p.header.Timestamp = int32(time.Now().Unix())
	p.header.Opid = opid

	var buf bytes.Buffer
	buf.Write(OplogHeaderPrefix)
	binary.Write(&buf, binary.LittleEndian, p.header)
	buf.WriteString("\r\n")
	buf.Write(p.readCmdContent)

	return ParseOplog(buf.Bytes())
}

// ParsePSyncFullResp parse applyinfo which is response of "psync ? -1". For example, applyinfo is "applied_info{0:100,7171317:1867040,1:100}".
func ParsePsyncFullApplyInfo(applyinfo string) (map[uint64]int64, error) {
	reply := make(map[uint64]int64)
	var content string
	var err error
	var kvs, kv []string
	var key uint64
	var value int64

	if !strings.HasPrefix(applyinfo, "applied_info{") {
		goto error_return
	}
	if !strings.HasSuffix(applyinfo, "}") {
		goto error_return
	}
	content = applyinfo[len("applied_info{") : len(applyinfo)-len("}")]
	if len(content) != 0 {
		kvs = strings.Split(content, ",")
		for _, item := range kvs {
			// item "0:100"
			kv = strings.Split(item, ":")
			if len(kv) != 2 {
				goto error_return
			}
			key, err = strconv.ParseUint(kv[0], 10, 0)
			if err != nil {
				return nil, err
			}
			value, err = strconv.ParseInt(kv[1], 10, 0)
			if err != nil {
				return nil, err
			}
			reply[key] = value
		}
	}
	return reply, nil

error_return:
	return nil, fmt.Errorf("invalid apply info string: %s", applyinfo)
}
