package rdb

import (
	"bufio"
	"bytes"
	"encoding/binary"
	"io"
	"os"
	"strconv"
	"time"

	"github.com/alibaba/RedisShake/internal/config"
	"github.com/alibaba/RedisShake/internal/entry"
	"github.com/alibaba/RedisShake/internal/log"
	"github.com/alibaba/RedisShake/internal/rdb/structure"
	"github.com/alibaba/RedisShake/internal/rdb/types"
	"github.com/alibaba/RedisShake/internal/statistics"
	"github.com/alibaba/RedisShake/internal/utils"
)

const (
	kFlagFunction2 = 245  // function library data
	kFlagFunction  = 246  // old function library data for 7.0 rc1 and rc2
	kFlagModuleAux = 247  // Module auxiliary data.
	kFlagIdle      = 0xf8 // LRU idle time.
	kFlagFreq      = 0xf9 // LFU frequency.
	kFlagAUX       = 0xfa // RDB aux field.
	kFlagResizeDB  = 0xfb // Hash table resize hint.
	kFlagExpireMs  = 0xfc // Expire time in milliseconds.
	kFlagExpire    = 0xfd // Old expire time in seconds.
	kFlagSelect    = 0xfe // DB number of the following keys.
	kEOF           = 0xff // End of the RDB file.
)

type Loader struct {
	replStreamDbId int // https://github.com/alibaba/RedisShake/pull/430#issuecomment-1099014464

	nowDBId  int
	expireMs int64
	idle     int64
	freq     int64

	filPath string
	fp      *os.File

	ch         chan *entry.Entry
	dumpBuffer bytes.Buffer
}

func NewLoader(filPath string, ch chan *entry.Entry) *Loader {
	ld := new(Loader)
	ld.ch = ch
	ld.filPath = filPath
	return ld
}

func (ld *Loader) ParseRDB() int {
	var err error
	ld.fp, err = os.OpenFile(ld.filPath, os.O_RDONLY, 0666)
	if err != nil {
		log.Panicf("open file failed. file_path=[%s], error=[%s]", ld.filPath, err)
	}
	defer func() {
		err = ld.fp.Close()
		if err != nil {
			log.Panicf("close file failed. file_path=[%s], error=[%s]", ld.filPath, err)
		}
	}()
	rd := bufio.NewReader(ld.fp)
	//magic + version
	buf := make([]byte, 9)
	_, err = io.ReadFull(rd, buf)
	if err != nil {
		log.PanicError(err)
	}
	if !bytes.Equal(buf[:5], []byte("REDIS")) {
		log.Panicf("verify magic string, invalid file format. bytes=[%v]", buf[:5])
	}
	version, err := strconv.Atoi(string(buf[5:]))
	if err != nil {
		log.PanicError(err)
	}
	log.Infof("RDB version: %d", version)

	// read entries
	ld.parseRDBEntry(rd)

	// force update rdb_sent_size for issue: https://github.com/alibaba/RedisShake/issues/485
	fi, err := os.Stat(ld.filPath)
	if err != nil {
		log.Panicf("NewRDBReader: os.Stat error: %s", err.Error())
	}
	statistics.Metrics.RdbSendSize = uint64(fi.Size())
	return ld.replStreamDbId
}

func (ld *Loader) parseRDBEntry(rd *bufio.Reader) {
	// for stat
	UpdateRDBSentSize := func() {
		offset, err := ld.fp.Seek(0, io.SeekCurrent)
		if err != nil {
			log.PanicError(err)
		}
		statistics.UpdateRDBSentSize(uint64(offset))
	}
	defer UpdateRDBSentSize()
	// read one entry
	tick := time.Tick(time.Second * 1)
	for {
		typeByte := structure.ReadByte(rd)
		switch typeByte {
		case kFlagIdle:
			ld.idle = int64(structure.ReadLength(rd))
		case kFlagFreq:
			ld.freq = int64(structure.ReadByte(rd))
		case kFlagAUX:
			key := structure.ReadString(rd)
			value := structure.ReadString(rd)
			if key == "repl-stream-db" {
				var err error
				ld.replStreamDbId, err = strconv.Atoi(value)
				if err != nil {
					log.PanicError(err)
				}
				log.Infof("RDB repl-stream-db: %d", ld.replStreamDbId)
			} else if key == "lua" {
				e := entry.NewEntry()
				e.Argv = []string{"script", "load", value}
				e.IsBase = true
				ld.ch <- e
				log.Infof("LUA script: [%s]", value)
			} else {
				log.Infof("RDB AUX fields. key=[%s], value=[%s]", key, value)
			}
		case kFlagResizeDB:
			dbSize := structure.ReadLength(rd)
			expireSize := structure.ReadLength(rd)
			log.Infof("RDB resize db. db_size=[%d], expire_size=[%d]", dbSize, expireSize)
		case kFlagExpireMs:
			ld.expireMs = int64(structure.ReadUint64(rd)) - time.Now().UnixMilli()
			if ld.expireMs < 0 {
				ld.expireMs = 1
			}
		case kFlagExpire:
			ld.expireMs = int64(structure.ReadUint32(rd))*1000 - time.Now().UnixMilli()
			if ld.expireMs < 0 {
				ld.expireMs = 1
			}
		case kFlagSelect:
			ld.nowDBId = int(structure.ReadLength(rd))
		case kEOF:
			return
		default:
			key := structure.ReadString(rd)
			var value bytes.Buffer
			anotherReader := io.TeeReader(rd, &value)
			o := types.ParseObject(anotherReader, typeByte, key)
			if uint64(value.Len()) > config.Config.Advanced.TargetRedisProtoMaxBulkLen {
				cmds := o.Rewrite()
				for _, cmd := range cmds {
					e := entry.NewEntry()
					e.IsBase = true
					e.DbId = ld.nowDBId
					e.Argv = cmd
					ld.ch <- e
				}
				if ld.expireMs != 0 {
					e := entry.NewEntry()
					e.IsBase = true
					e.DbId = ld.nowDBId
					e.Argv = []string{"PEXPIRE", key, strconv.FormatInt(ld.expireMs, 10)}
					ld.ch <- e
				}
			} else {
				e := entry.NewEntry()
				e.IsBase = true
				e.DbId = ld.nowDBId
				v := ld.createValueDump(typeByte, value.Bytes())
				e.Argv = []string{"restore", key, strconv.FormatInt(ld.expireMs, 10), v}
				if config.Config.Advanced.RDBRestoreCommandBehavior == "rewrite" {
					if config.Config.Target.Version < 3.0 {
						log.Panicf("RDB restore command behavior is rewrite, but target redis version is %f, not support REPLACE modifier", config.Config.Target.Version)
					}
					e.Argv = append(e.Argv, "replace")
				}
				if ld.idle != 0 && config.Config.Target.Version >= 5.0 {
					e.Argv = append(e.Argv, "idletime", strconv.FormatInt(ld.idle, 10))
				}
				if ld.freq != 0 && config.Config.Target.Version >= 5.0 {
					e.Argv = append(e.Argv, "freq", strconv.FormatInt(ld.freq, 10))
				}
				ld.ch <- e
			}
			ld.expireMs = 0
			ld.idle = 0
			ld.freq = 0
		}
		select {
		case <-tick:
			UpdateRDBSentSize()
		default:
		}
	}
}

func (ld *Loader) createValueDump(typeByte byte, val []byte) string {
	ld.dumpBuffer.Reset()
	_, _ = ld.dumpBuffer.Write([]byte{typeByte})
	_, _ = ld.dumpBuffer.Write(val)
	_ = binary.Write(&ld.dumpBuffer, binary.LittleEndian, uint16(6))
	// calc crc
	sum64 := utils.CalcCRC64(ld.dumpBuffer.Bytes())
	_ = binary.Write(&ld.dumpBuffer, binary.LittleEndian, sum64)
	return ld.dumpBuffer.String()
}
