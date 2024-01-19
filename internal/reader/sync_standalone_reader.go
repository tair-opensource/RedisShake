package reader

import (
	"bufio"
	"context"
	"fmt"
	"io"
	"io/fs"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"time"

	"RedisShake/internal/client"
	"RedisShake/internal/config"
	"RedisShake/internal/entry"
	"RedisShake/internal/log"
	"RedisShake/internal/rdb"
	"RedisShake/internal/utils"
	rotate "RedisShake/internal/utils/file_rotate"

	"github.com/dustin/go-humanize"
)

type SyncReaderOptions struct {
	Cluster       bool   `mapstructure:"cluster" default:"false"`
	Address       string `mapstructure:"address" default:""`
	Username      string `mapstructure:"username" default:""`
	Password      string `mapstructure:"password" default:""`
	Tls           bool   `mapstructure:"tls" default:"false"`
	SyncRdb       bool   `mapstructure:"sync_rdb" default:"true"`
	SyncAof       bool   `mapstructure:"sync_aof" default:"true"`
	PreferReplica bool   `mapstructure:"prefer_replica" default:"false"`
}

type State string

const (
	kHandShake  State = "hand shaking"
	kWaitBgsave State = "waiting bgsave"
	kReceiveRdb State = "receiving rdb"
	kSyncRdb    State = "syncing rdb"
	kSyncAof    State = "syncing aof"
)

type syncStandaloneReader struct {
	ctx    context.Context
	opts   *SyncReaderOptions
	client *client.Redis

	ch   chan *entry.Entry
	DbId int

	rd *bufio.Reader

	stat struct {
		Name    string `json:"name"`
		Address string `json:"address"`
		Dir     string `json:"dir"`

		// status
		Status State `json:"status"`

		// rdb info
		RdbFileSizeBytes int64  `json:"rdb_file_size_bytes"` // bytes of the rdb file
		RdbFileSizeHuman string `json:"rdb_file_size_human"`
		RdbReceivedBytes int64  `json:"rdb_received_bytes"` // bytes of RDB received from master
		RdbReceivedHuman string `json:"rdb_received_human"`
		RdbSentBytes     int64  `json:"rdb_sent_bytes"` // bytes of RDB sent to chan
		RdbSentHuman     string `json:"rdb_sent_human"`

		// aof info
		AofReceivedOffset int64  `json:"aof_received_offset"` // offset of AOF received from master
		AofSentOffset     int64  `json:"aof_sent_offset"`     // offset of AOF sent to chan
		AofReceivedBytes  int64  `json:"aof_received_bytes"`  // bytes of AOF received from master
		AofReceivedHuman  string `json:"aof_received_human"`
	}
}

func NewSyncStandaloneReader(ctx context.Context, opts *SyncReaderOptions) Reader {
	r := new(syncStandaloneReader)
	r.opts = opts
	r.client = client.NewRedisClient(ctx, opts.Address, opts.Username, opts.Password, opts.Tls)
	r.rd = r.client.BufioReader()
	r.stat.Name = "reader_" + strings.Replace(opts.Address, ":", "_", -1) + "_" + r.client.ReplId
	r.stat.Address = opts.Address
	r.stat.Status = kHandShake
	r.stat.Dir = utils.GetAbsPath(r.stat.Name)
	r.stat.AofReceivedOffset = readLastReplOffset(r.stat.Dir)
	return r
}

func (r *syncStandaloneReader) StartRead(ctx context.Context) chan *entry.Entry {
	r.ctx = ctx
	r.ch = make(chan *entry.Entry, 1024)
	go func() {
		r.sendReplconfListenPort()
		fullReSync := r.sendPSync()
		go r.sendReplconfAck() // start sent replconf ack
		if fullReSync {
			// empty out of date file before full sync
			utils.CreateEmptyDir(r.stat.Dir)
			rdbFilePath := r.receiveRDB()
			r.sendRDB(rdbFilePath)
		}

		// create aof file first
		aofWriter := rotate.NewAOFWriter(r.stat.Name, r.stat.Dir, r.stat.AofReceivedOffset)
		go r.receiveAOF(r.rd, aofWriter)
		if r.opts.SyncAof {
			r.stat.Status = kSyncAof
			r.sendAOF(r.stat.AofReceivedOffset)
		}
		r.client.Close()
		aofWriter.Close()
		// must be closed last so that other resources can be released
		close(r.ch)
	}()

	return r.ch
}

func (r *syncStandaloneReader) sendReplconfListenPort() {
	// use status_port as redis-shake port
	argv := []string{"replconf", "listening-port", strconv.Itoa(config.Opt.Advanced.StatusPort)}
	r.client.Send(argv...)
	_, err := r.client.Receive()
	if err != nil {
		log.Warnf("[%s] send replconf command to redis server failed. error=[%v]", r.stat.Name, err)
	}
}

// the return indicate whether full sync
func (r *syncStandaloneReader) sendPSync() bool {
	// send PSync
	var argv []string
	if r.opts.SyncRdb || r.stat.AofReceivedOffset <= 0 {
		argv = []string{"PSYNC", "?", "-1"}
	} else {
		argv = []string{"PSYNC", r.client.ReplId, strconv.FormatInt(r.stat.AofReceivedOffset, 10)}
	}
	if config.Opt.Advanced.AwsPSync != "" {
		argv = []string{config.Opt.Advanced.GetPSyncCommand(r.stat.Address), "?", "-1"}
	}
	r.client.Send(argv...)

	// format: \n\n\n+<reply>\r\n
	for {
		bytes, err := r.rd.Peek(1)
		if err != nil {
			log.Panicf(err.Error())
		}
		if bytes[0] != '\n' {
			break
		}
	}

	reply := r.client.ReceiveString()
	if reply == "CONTINUE" {
		log.Infof("increment sync start at last offset: %d", r.stat.AofReceivedOffset)
		b, err := r.rd.ReadByte()
		if err != nil {
			log.Panicf(err.Error())
		}
		if b != '\n' {
			log.Panicf("unexpected data:%s", string(b))
		}
		return false
	}

	// FULLRESYNC <replID> <offset>
	masterOffset, err := strconv.Atoi(strings.Split(reply, " ")[2])
	if err != nil {
		log.Panicf(err.Error())
	}
	r.stat.AofReceivedOffset = int64(masterOffset)
	return true
}

func (r *syncStandaloneReader) receiveRDB() string {
	log.Debugf("[%s] source db is doing bgsave.", r.stat.Name)
	r.stat.Status = kWaitBgsave
	timeStart := time.Now()
	// format: \n\n\n$<length>\r\n<rdb>
	for {
		b, err := r.rd.ReadByte()
		if err != nil {
			log.Panicf(err.Error())
		}
		if b == '\n' { // heartbeat
			continue
		}
		if b != '$' {
			log.Panicf("[%s] invalid rdb format. b=[%s]", r.stat.Name, string(b))
		}
		break
	}
	log.Debugf("[%s] source db bgsave finished. timeUsed=[%.2f]s", r.stat.Name, time.Since(timeStart).Seconds())
	lengthStr, err := r.rd.ReadString('\n')
	if err != nil {
		log.Panicf(err.Error())
	}
	lengthStr = strings.TrimSpace(lengthStr)
	length, err := strconv.ParseInt(lengthStr, 10, 64)
	if err != nil {
		log.Panicf(err.Error())
	}
	log.Debugf("[%s] rdb file size: [%v]", r.stat.Name, humanize.IBytes(uint64(length)))
	r.stat.RdbFileSizeBytes = length
	r.stat.RdbFileSizeHuman = humanize.IBytes(uint64(length))

	// create rdb file
	rdbFilePath, err := filepath.Abs(r.stat.Name + "/dump.rdb")
	if err != nil {
		log.Panicf(err.Error())
	}
	timeStart = time.Now()
	log.Debugf("[%s] start receiving RDB. path=[%s]", r.stat.Name, rdbFilePath)
	rdbFileHandle, err := os.OpenFile(rdbFilePath, os.O_WRONLY|os.O_CREATE|os.O_TRUNC, 0666)
	if err != nil {
		log.Panicf(err.Error())
	}

	// receive rdb
	r.stat.Status = kReceiveRdb
	remainder := length
	const bufSize int64 = 32 * 1024 * 1024 // 32MB
	buf := make([]byte, bufSize)
	for remainder != 0 {
		readOnce := bufSize
		if remainder < readOnce {
			readOnce = remainder
		}
		n, err := r.rd.Read(buf[:readOnce])
		if err != nil {
			log.Panicf(err.Error())
		}
		remainder -= int64(n)
		_, err = rdbFileHandle.Write(buf[:n])
		if err != nil {
			log.Panicf(err.Error())
		}

		r.stat.RdbReceivedBytes += int64(n)
		r.stat.RdbReceivedHuman = humanize.IBytes(uint64(r.stat.RdbReceivedBytes))
	}
	err = rdbFileHandle.Close()
	if err != nil {
		log.Panicf(err.Error())
	}
	log.Debugf("[%s] save RDB finished. timeUsed=[%.2f]s", r.stat.Name, time.Since(timeStart).Seconds())
	return rdbFilePath
}

func (r *syncStandaloneReader) receiveAOF(rd io.Reader, aofWriter *rotate.AOFWriter) {
	log.Debugf("[%s] start receiving aof data, and save to file", r.stat.Name)
	buf := make([]byte, 16*1024) // 16KB is enough for writing file
	for {
		select {
		case <-r.ctx.Done():
			return
		default:
			n, err := rd.Read(buf)
			if err != nil {
				log.Panicf(err.Error())
			}
			r.stat.AofReceivedBytes += int64(n)
			r.stat.AofReceivedHuman = humanize.IBytes(uint64(r.stat.AofReceivedBytes))
			aofWriter.Write(buf[:n])
			r.stat.AofReceivedOffset += int64(n)
		}
	}
}

func (r *syncStandaloneReader) sendRDB(rdbFilePath string) {
	// start parse rdb
	log.Debugf("[%s] start sending RDB to target", r.stat.Name)
	r.stat.Status = kSyncRdb
	updateFunc := func(offset int64) {
		r.stat.RdbSentBytes = offset
		r.stat.RdbSentHuman = humanize.IBytes(uint64(offset))
	}
	rdbLoader := rdb.NewLoader(r.stat.Name, updateFunc, rdbFilePath, r.ch)
	r.DbId = rdbLoader.ParseRDB(r.ctx)
	log.Debugf("[%s] send RDB finished", r.stat.Name)
	// delete file
	_ = os.Remove(rdbFilePath)
	log.Debugf("[%s] delete RDB file", r.stat.Name)
}

func (r *syncStandaloneReader) sendAOF(offset int64) {
	time.Sleep(1 * time.Second) // wait for receiveAOF create aof file
	aofReader := rotate.NewAOFReader(r.stat.Name, r.stat.Dir, offset)
	defer aofReader.Close()
	r.client.SetBufioReader(bufio.NewReader(aofReader))
	for {
		select {
		case <-r.ctx.Done():
			return
		default:
			argv := client.ArrayString(r.client.Receive())
			r.stat.AofSentOffset = aofReader.Offset()
			// select
			if strings.EqualFold(argv[0], "select") {
				DbId, err := strconv.Atoi(argv[1])
				if err != nil {
					log.Panicf(err.Error())
				}
				r.DbId = DbId
				continue
			}
			// ping
			if strings.EqualFold(argv[0], "ping") {
				continue
			}
			// replconf @AWS
			if strings.EqualFold(argv[0], "replconf") {
				continue
			}
			// opinfo @Aliyun
			if strings.EqualFold(argv[0], "opinfo") {
				continue
			}
			// sentinel
			if strings.EqualFold(argv[0], "publish") && strings.EqualFold(argv[1], "__sentinel__:hello") {
				continue
			}

			e := entry.NewEntry()
			e.Argv = argv
			e.DbId = r.DbId
			r.ch <- e
		}
	}
}

// sendReplconfAck send replconf ack to master to keep heartbeat between redis-shake and source redis.
func (r *syncStandaloneReader) sendReplconfAck() {
	ticker := time.NewTicker(time.Millisecond * 100)
	defer ticker.Stop()
	for range ticker.C {
		select {
		case <-r.ctx.Done():
			return
		default:
			if r.stat.AofReceivedOffset != 0 {
				r.client.Send("replconf", "ack", strconv.FormatInt(r.stat.AofReceivedOffset, 10))
			}
		}
	}
}

func (r *syncStandaloneReader) Status() interface{} {
	return r.stat
}

func (r *syncStandaloneReader) StatusString() string {
	if r.stat.Status == kSyncRdb {
		return fmt.Sprintf("%s, size=[%s/%s]", r.stat.Status, r.stat.RdbSentHuman, r.stat.RdbFileSizeHuman)
	}
	if r.stat.Status == kSyncAof {
		return fmt.Sprintf("%s, diff=[%v]", r.stat.Status, -r.stat.AofSentOffset+r.stat.AofReceivedOffset)
	}
	return string(r.stat.Status)
}

func (r *syncStandaloneReader) StatusConsistent() bool {
	return r.stat.AofReceivedOffset != 0 &&
		r.stat.AofReceivedOffset == r.stat.AofSentOffset &&
		len(r.ch) == 0
}

func readLastReplOffset(dir string) int64 {
	var offset int64 = 0
	if !utils.IsExist(dir) {
		return 0
	}
	if err := filepath.Walk(dir, func(path string, info fs.FileInfo, err error) error {
		if err != nil {
			return err
		}
		ext := filepath.Ext(path)
		if !info.IsDir() && (ext == ".aof") {
			baseOffset, err := strconv.ParseInt(strings.TrimSuffix(info.Name(), ext), 10, 64)
			if err != nil {
				log.Warnf("illegal file name of aof: %s", info.Name())
				return nil
			}
			if baseOffset + info.Size() > offset {
				offset = baseOffset + info.Size()
			}
		}
		return nil
	}); err != nil {
		log.Warnf("parse repl offset from aof file err: %s", err.Error())
		return 0
	}
	log.Infof("read repl offset:%d for increment sync", offset)
	return offset
}
