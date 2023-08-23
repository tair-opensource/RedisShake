package rdb

import (
	"bufio"
	"bytes"
	"fmt"
	"io"
	"os"
	"strconv"
	"time"

	"github.com/alibaba/RedisShake/internal/config"
	"github.com/alibaba/RedisShake/internal/entry"
	"github.com/alibaba/RedisShake/internal/log"
	"github.com/alibaba/RedisShake/internal/rdb/structure"
	"github.com/alibaba/RedisShake/internal/rdb/types"
)

func RedisCheckRDBMain(filepath string, fp *os.File) int64 {
	if fp == nil {
		fmt.Fprintf(os.Stderr, "The file: %s fp is nil", filepath)
		os.Exit(1)
	}
	log.Infof("Checking RDB file %s", filepath)
	ld := NewLoader(filepath, nil)

	RDBPos := ld.CheckParseRDB()

	if RDBPos > 0 {
		log.Infof("\\o/ RDB looks OK! \\o/")
	} else {
		return -1
	}
	return RDBPos
}
func (ld *Loader) CheckParseRDB() int64 {
	var err error
	ld.fp, err = os.OpenFile(ld.filPath, os.O_RDONLY, 0666)
	if err != nil {
		log.Panicf("open file failed. filepath=[%s], error=[%s]", ld.filPath, err)
	}
	defer func() {
		err = ld.fp.Close()
		if err != nil {
			log.Panicf("close file failed. filepath=[%s], error=[%s]", ld.filPath, err)
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
	rdbpos := ld.CheckparseRDBEntry(rd)

	return rdbpos
}

func (ld *Loader) CheckparseRDBEntry(rd *bufio.Reader) int64 {
	// for stat
	var RDBPos int64
	var rdbsize int64 = 9
	UpdateRDBSize := func() {
		var err error
		RDBPos, err = ld.fp.Seek(0, io.SeekCurrent)
		if err != nil {
			log.PanicError(err)
		}

	}
	defer UpdateRDBSize()

	// read one entry
	tick := time.Tick(time.Second * 1)
	for {
		typeByte := structure.ReadByte(rd)
		rdbsize += 1
		switch typeByte {
		case kFlagIdle:
			tempidle, tempOffset := structure.ReadLengthWithOffset(rd)
			ld.idle = int64(tempidle)
			rdbsize += tempOffset
		case kFlagFreq:
			ld.freq = int64(structure.ReadByte(rd))
			rdbsize += 1
		case kFlagAUX:
			key, tempOffset := structure.ReadStringWithOffset(rd)
			rdbsize += tempOffset
			value, tempOffset := structure.ReadStringWithOffset(rd)
			rdbsize += tempOffset

			if key == "repl-stream-db" {
				var err error
				ld.replStreamDbId, err = strconv.Atoi(value)
				if err != nil {
					log.PanicError(err)
				}
				log.Infof("RDB repl-stream-db: %d position: %d", ld.replStreamDbId, RDBPos)
			} else if key == "lua" {
				e := entry.NewEntry()
				e.Argv = []string{"script", "load", value}
				e.IsBase = true

				log.Infof("LUA script: [%s]", value)
			} else {
				log.Infof("RDB AUX fields. key=[%s], value=[%s]", key, value)
			}
		case kFlagResizeDB:
			dbSize, dbsizeoffset := structure.ReadLengthWithOffset(rd)
			expireSize, expiresizeoffset := structure.ReadLengthWithOffset(rd)
			rdbsize += dbsizeoffset + expiresizeoffset
			log.Infof("RDB resize db. dbsize=[%d], expiresize=[%d]", dbSize, expireSize)
		case kFlagExpireMs:
			ld.expireMs = int64(structure.ReadUint64(rd)) - time.Now().UnixMilli()
			rdbsize += 8
			if ld.expireMs < 0 {
				ld.expireMs = 1
			}
		case kFlagExpire:
			ld.expireMs = int64(structure.ReadUint32(rd))*1000 - time.Now().UnixMilli()
			rdbsize += 4
			if ld.expireMs < 0 {
				ld.expireMs = 1
			}
		case kFlagSelect:
			DBId, DBIDoffset := structure.ReadLengthWithOffset(rd)
			ld.nowDBId = int(DBId)
			rdbsize += DBIDoffset
		case kEOF:
			UpdateRDBSize()
			return rdbsize
		default:
			key, tempoffset := structure.ReadStringWithOffset(rd)
			rdbsize += tempoffset
			var value bytes.Buffer
			anotherReader := io.TeeReader(rd, &value)
			o, tempOffsets := types.ParseObjectWithOffset(anotherReader, typeByte, key)

			rdbsize += tempOffsets
			if uint64(value.Len()) > config.Config.Advanced.TargetRedisProtoMaxBulkLen {
				cmds := o.Rewrite()
				for _, cmd := range cmds {
					e := entry.NewEntry()
					e.IsBase = true
					e.DbId = ld.nowDBId
					e.Argv = cmd

				}
				if ld.expireMs != 0 {
					e := entry.NewEntry()
					e.IsBase = true
					e.DbId = ld.nowDBId
					e.Argv = []string{"PEXPIRE", key, strconv.FormatInt(ld.expireMs, 10)}
				}
			} else {
				e := entry.NewEntry()
				e.IsBase = true
				e.DbId = ld.nowDBId

				v := ld.createValueDump(typeByte, value.Bytes())

				//value 口口口
				e.Argv = []string{"restore", key, strconv.FormatInt(ld.expireMs, 10), v}
				if config.Config.Advanced.RDBRestoreCommandBehavior == "rewrite" {
					if config.Config.Target.Version < 3.0 {
						log.Panicf("RDB restore command behavior is rewrite, but target redis version is %f, not support REPLACE modifier,position: %d", config.Config.Target.Version, RDBPos)
					}
					e.Argv = append(e.Argv, "replace")
				}
				if ld.idle != 0 && config.Config.Target.Version >= 5.0 {
					e.Argv = append(e.Argv, "idletime", strconv.FormatInt(ld.idle, 10))
				}
				if ld.freq != 0 && config.Config.Target.Version >= 5.0 {
					e.Argv = append(e.Argv, "freq", strconv.FormatInt(ld.freq, 10))
				}

			}
			ld.expireMs = 0
			ld.idle = 0
			ld.freq = 0
		}
		select {
		case <-tick:
			UpdateRDBSize()
		default:
		}
		UpdateRDBSize()
	}
	return RDBPos
}
