package reader

import (
	"bufio"
	"errors"
	"github.com/alibaba/RedisShake/internal/client"
	"github.com/alibaba/RedisShake/internal/entry"
	"github.com/alibaba/RedisShake/internal/log"
	"github.com/alibaba/RedisShake/internal/rdb"
	"github.com/alibaba/RedisShake/internal/reader/rotate"
	"github.com/alibaba/RedisShake/internal/statistics"
	"io"
	"io/ioutil"
	"os"
	"strconv"
	"strings"
	"time"
)

type psyncReader struct {
	client  *client.Redis
	address string
	ch      chan *entry.Entry
	DbId    int

	rd             *bufio.Reader
	receivedOffset int64
}

func NewPSyncReader(address string, password string, isTls bool) Reader {
	r := new(psyncReader)
	r.init(address, password, isTls)
	return r
}

func (r *psyncReader) init(address string, password string, isTls bool) {
	r.address = address
	standalone := client.NewRedisClient(address, password, isTls)

	r.client = standalone
	r.rd = r.client.BufioReader()
	log.Infof("psyncReader connected to redis successful. address=[%s]", address)
}

func (r *psyncReader) StartRead() chan *entry.Entry {
	r.ch = make(chan *entry.Entry, 1024)

	go func() {
		r.clearDir()
		go r.sendReplconfAck()
		r.saveRDB()
		startOffset := r.receivedOffset
		go r.saveAOF(r.rd)
		r.sendRDB()
		time.Sleep(1 * time.Second) // wait for saveAOF create aof file
		r.sendAOF(startOffset)
	}()

	return r.ch
}

func (r *psyncReader) clearDir() {
	files, err := ioutil.ReadDir("./")
	if err != nil {
		log.PanicError(err)
	}

	for _, f := range files {
		if strings.HasSuffix(f.Name(), ".rdb") || strings.HasSuffix(f.Name(), ".aof") {
			err = os.Remove(f.Name())
			if err != nil {
				log.PanicError(err)
			}
			log.Warnf("remove file. filename=[%s]", f.Name())
		}
	}
}

func (r *psyncReader) saveRDB() {
	log.Infof("start save RDB. address=[%s]", r.address)
	argv := []string{"replconf", "listening-port", "10007"} // 10007 is magic number
	log.Infof("send %v", argv)
	reply := r.client.DoWithStringReply(argv...)
	if reply != "OK" {
		log.Warnf("send replconf command to redis server failed. address=[%s], reply=[%s], error=[]", r.address, reply)
	}

	// send psync
	argv = []string{"PSYNC", "?", "-1"}
	r.client.Send(argv...)
	log.Infof("send %v", argv)
	// format: \n\n\n$<reply>\r\n
	for true {
		// \n\n\n$
		b, err := r.rd.ReadByte()
		if err != nil {
			log.PanicError(err)
		}
		if b == '\n' {
			continue
		}
		if b != '+' {
			log.Panicf("invalid rdb format. address=[%s], b=[%s]", r.address, string(b))
		}
		break
	}
	reply, err := r.rd.ReadString('\n')
	if err != nil {
		log.PanicError(err)
	}
	reply = strings.TrimSpace(reply)
	log.Infof("receive [%s]", reply)
	masterOffset, err := strconv.Atoi(strings.Split(reply, " ")[2])
	if err != nil {
		log.PanicError(err)
	}
	r.receivedOffset = int64(masterOffset)

	log.Infof("source db is doing bgsave. address=[%s]", r.address)
	timeStart := time.Now()
	// format: \n\n\n$<length>\r\n<rdb>
	for true {
		// \n\n\n$
		b, err := r.rd.ReadByte()
		if err != nil {
			log.PanicError(err)
		}
		if b == '\n' {
			continue
		}
		if b != '$' {
			log.Panicf("invalid rdb format. address=[%s], b=[%s]", r.address, string(b))
		}
		break
	}
	log.Infof("source db bgsave finished. timeUsed=[%.2f]s, address=[%s]", time.Since(timeStart).Seconds(), r.address)
	lengthStr, err := r.rd.ReadString('\n')
	if err != nil {
		log.PanicError(err)
	}
	lengthStr = strings.TrimSpace(lengthStr)
	length, err := strconv.ParseInt(lengthStr, 10, 64)
	if err != nil {
		log.PanicError(err)
	}
	log.Infof("received rdb length. length=[%d]", length)
	statistics.SetRDBFileSize(length)

	// create rdb file
	rdbFilePath := "dump.rdb"
	log.Infof("create dump.rdb file. filename_path=[%s]", rdbFilePath)
	rdbFileHandle, err := os.OpenFile(rdbFilePath, os.O_WRONLY|os.O_CREATE|os.O_TRUNC, 0666)
	if err != nil {
		log.PanicError(err)
	}

	// read rdb
	var readTotal int64 = 0
	buf := make([]byte, 32*1024*1024)
	for readTotal < length {
		n, err := r.rd.Read(buf)
		if err != nil {
			log.PanicError(err)
		}
		readTotal += int64(n)
		statistics.UpdateRDBReceivedSize(readTotal)
		_, err = rdbFileHandle.Write(buf[:n])
		if err != nil {
			log.PanicError(err)
		}
	}
	err = rdbFileHandle.Close()
	if err != nil {
		log.PanicError(err)
	}
	log.Infof("save RDB finished. address=[%s], total_bytes=[%d]", r.address, readTotal)
}

func (r *psyncReader) saveAOF(rd io.Reader) {
	log.Infof("start save AOF. address=[%s]", r.address)
	// create aof file
	aofWriter := rotate.NewAOFWriter(r.receivedOffset)
	buf := make([]byte, 16*1024) // 16KB is enough for writing file
	for {
		n, err := rd.Read(buf)
		if errors.Is(err, io.EOF) {
			log.Infof("read aof finished. address=[%s]", r.address)
			break
		}
		if err != nil {
			log.PanicError(err)
		}
		r.receivedOffset += int64(n)
		statistics.UpdateAOFReceivedOffset(r.receivedOffset)
		aofWriter.Write(buf[:n])
	}
	aofWriter.Close()
}

func (r *psyncReader) sendRDB() {
	// start parse rdb
	log.Infof("start send RDB. address=[%s]", r.address)
	rdbLoader := rdb.NewLoader("dump.rdb", r.ch)
	r.DbId = rdbLoader.ParseRDB()
	log.Infof("send RDB finished. address=[%s], repl-stream-db=[%d]", r.address, r.DbId)
}

func (r *psyncReader) sendAOF(offset int64) {
	aofReader := rotate.NewAOFReader(offset)
	r.client.SetBufioReader(bufio.NewReader(aofReader))
	for {
		argv := client.ArrayString(r.client.Receive())
		log.Debugf("psyncReader receive. argv=%v", argv)
		// select
		if strings.EqualFold(argv[0], "select") {
			DbId, err := strconv.Atoi(argv[1])
			if err != nil {
				log.PanicError(err)
			}
			r.DbId = DbId
			continue
		}

		e := entry.NewEntry()
		e.Argv = argv
		e.DbId = r.DbId
		e.Offset = aofReader.Offset()
		r.ch <- e
	}
}

func (r *psyncReader) sendReplconfAck() {
	for range time.Tick(time.Millisecond * 100) {
		// send ack receivedOffset
		r.client.Send("replconf", "ack", strconv.FormatInt(r.receivedOffset, 10))
	}
}
