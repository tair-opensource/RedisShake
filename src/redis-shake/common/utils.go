// Copyright 2016 CodisLabs. All Rights Reserved.
// Licensed under the MIT (MIT-LICENSE.txt) license.

package utils

import (
	"bufio"
	"bytes"
	"crypto/tls"
	"encoding/binary"
	"fmt"
	"io"
	"math/rand"
	"net"
	"os"
	"strconv"
	"strings"
	"time"

	"pkg/libs/atomic2"
	"pkg/libs/errors"
	"pkg/libs/log"
	"pkg/libs/stats"
	"pkg/rdb"
	"pkg/redis"

	"redis-shake/configure"

	"github.com/FZambia/go-sentinel"
	redigo "github.com/garyburd/redigo/redis"
	redigoCluster "github.com/vinllen/redis-go-cluster"
)

func OpenRedisConn(target []string, authType, passwd string, isCluster bool, tlsEnable bool) redigo.Conn {
	return OpenRedisConnWithTimeout(target, authType, passwd, 0, 0, isCluster, tlsEnable)
}

func OpenRedisConnWithTimeout(target []string, authType, passwd string, readTimeout, writeTimeout time.Duration,
	isCluster bool, tlsEnable bool) redigo.Conn {
	// return redigo.NewConn(OpenNetConn(target, authType, passwd), readTimeout, writeTimeout)
	if isCluster {
		// the alive time isn't the tcp keep_alive parameter
		cluster, err := redigoCluster.NewCluster(
			&redigoCluster.Options{
				StartNodes:   target,
				ConnTimeout:  5 * time.Second,
				ReadTimeout:  readTimeout,
				WriteTimeout: writeTimeout,
				KeepAlive:    32,               // number of available connections
				AliveTime:    10 * time.Second, // hard code to set alive time in single connection, not the tcp keep alive
				Password:     passwd,
			})
		if err != nil {
			log.Panicf("create cluster connection error[%v]", err)
			return nil
		}
		return NewClusterConn(cluster, RecvChanSize)
	} else {
		// tls only support single connection currently
		return redigo.NewConn(OpenNetConn(target[0], authType, passwd, tlsEnable), readTimeout, writeTimeout)
	}
}

func OpenNetConn(target, authType, passwd string, tlsEnable bool) net.Conn {
	d := &net.Dialer{
		KeepAlive: time.Duration(conf.Options.KeepAlive) * time.Second,
	}
	var c net.Conn
	var err error
	if tlsEnable {
		c, err = tls.DialWithDialer(d, "tcp", target, &tls.Config{InsecureSkipVerify: false})
	} else {
		c, err = d.Dial("tcp", target)
	}
	if err != nil {
		log.PanicErrorf(err, "cannot connect to '%s'", target)
	}

	// log.Infof("try to auth address[%v] with type[%v]", target, authType)
	AuthPassword(c, authType, passwd)
	// log.Info("auth OK!")
	return c
}

func OpenNetConnSoft(target, authType, passwd string, tlsEnable bool) net.Conn {
	var c net.Conn
	var err error
	if tlsEnable {
		c, err = tls.Dial("tcp", target, &tls.Config{InsecureSkipVerify: false})
	} else {
		c, err = net.Dial("tcp", target)
	}
	if err != nil {
		return nil
	}
	AuthPassword(c, authType, passwd)
	return c
}

func OpenReadFile(name string) (*os.File, int64) {
	f, err := os.Open(name)
	if err != nil {
		log.PanicErrorf(err, "cannot open file-reader '%s'", name)
	}
	s, err := f.Stat()
	if err != nil {
		log.PanicErrorf(err, "cannot stat file-reader '%s'", name)
	}
	return f, s.Size()
}

func OpenWriteFile(name string) *os.File {
	f, err := os.OpenFile(name, os.O_CREATE|os.O_WRONLY|os.O_TRUNC, 0666)
	if err != nil {
		log.PanicErrorf(err, "cannot open file-writer '%s'", name)
	}
	return f
}

func OpenReadWriteFile(name string) *os.File {
	f, err := os.OpenFile(name, os.O_CREATE|os.O_RDWR|os.O_TRUNC, 0600)
	if err != nil {
		log.PanicErrorf(err, "cannot open file-readwriter '%s'", name)
	}
	return f
}

func SendPSyncListeningPort(c net.Conn, port int) {
	_, err := c.Write(redis.MustEncodeToBytes(redis.NewCommand("replconf", "listening-port", port)))
	if err != nil {
		log.PanicError(errors.Trace(err), "write replconf listening-port failed")
	}

	ret, err := ReadRESPEnd(c)
	if err != nil {
		log.PanicError(errors.Trace(err), "read auth response failed")
	}
	if strings.ToUpper(ret) != "+OK\r\n" {
		log.Panicf("repl listening-port failed[%v]", RemoveRESPEnd(ret))
	}
}

func AuthPassword(c net.Conn, authType, passwd string) {
	if passwd == "" {
		log.Infof("input password is empty, skip auth address[%v] with type[%v].", c.RemoteAddr(), authType)
		return
	}

	_, err := c.Write(redis.MustEncodeToBytes(redis.NewCommand(authType, passwd)))
	if err != nil {
		log.PanicError(errors.Trace(err), "write auth command failed")
	}

	ret, err := ReadRESPEnd(c)
	if err != nil {
		log.PanicError(errors.Trace(err), "read auth response failed")
	}
	if strings.ToUpper(ret) != "+OK\r\n" {
		log.Panicf("auth failed[%v]", RemoveRESPEnd(ret))
	}
}

func OpenSyncConn(target string, authType, passwd string, tlsEnable bool) (net.Conn, <-chan int64) {
	c := OpenNetConn(target, authType, passwd, tlsEnable)
	if _, err := c.Write(redis.MustEncodeToBytes(redis.NewCommand("sync"))); err != nil {
		log.PanicError(errors.Trace(err), "write sync command failed")
	}
	return c, waitRdbDump(c)
}

// pipeline mode which means we don't wait all dump finish and run the next step
func waitRdbDump(r io.Reader) <-chan int64 {
	size := make(chan int64)
	// read rdb size
	go func() {
		var rsp string
		for {
			b := []byte{0}
			if _, err := r.Read(b); err != nil {
				log.PanicErrorf(err, "read sync response = '%s'", rsp)
			}
			if len(rsp) == 0 && b[0] == '\n' {
				size <- 0
				continue
			}
			rsp += string(b)
			if strings.HasSuffix(rsp, "\r\n") {
				break
			}
		}
		if rsp[0] != '$' {
			log.Panicf("invalid sync response, rsp = '%s'", rsp)
		}
		n, err := strconv.Atoi(rsp[1 : len(rsp)-2])
		if err != nil || n <= 0 {
			log.PanicErrorf(err, "invalid sync response = '%s', n = %d", rsp, n)
		}
		size <- int64(n)
	}()
	return size
}

func SendPSyncFullsync(br *bufio.Reader, bw *bufio.Writer) (string, int64, <-chan int64) {
	cmd := redis.NewCommand("psync", "?", -1)
	if err := redis.Encode(bw, cmd, true); err != nil {
		log.PanicError(err, "write psync command failed, fullsync")
	}
	r, err := redis.Decode(br)
	if err != nil {
		log.PanicError(err, "invalid psync response, fullsync")
	}
	if e, ok := r.(*redis.Error); ok {
		log.Panicf("invalid psync response, fullsync, %s", e.Value)
	}
	x, err := redis.AsString(r, nil)
	if err != nil {
		log.PanicError(err, "invalid psync response, fullsync")
	}
	xx := strings.Split(string(x), " ")
	if len(xx) < 3 || strings.ToLower(xx[0]) != "fullresync" {
		log.Panicf("invalid psync response = '%s', should be fullresync", x)
	}
	v, err := strconv.ParseInt(xx[2], 10, 64)
	if err != nil {
		log.PanicError(err, "parse psync offset failed")
	}

	// log.PurePrintf("%s\n", NewLogItem("FullSyncStart", "INFO", LogDetail{}))
	log.Infof("Event:FullSyncStart\tId:%s\t", conf.Options.Id)
	runid, offset := xx[1], v
	return runid, offset, waitRdbDump(br)
}

func SendPSyncContinue(br *bufio.Reader, bw *bufio.Writer, runid string, offset int64) (string, int64, <-chan int64) {
	if offset != -1 {
		offset += 1
	}

	cmd := redis.NewCommand("psync", runid, offset)
	if err := redis.Encode(bw, cmd, true); err != nil {
		log.PanicError(err, "write psync command failed, continue")
	}
	r, err := redis.Decode(br)
	if err != nil {
		log.PanicError(err, "invalid psync response, continue")
	}

	// parse return message
	if e, ok := r.(*redis.Error); ok {
		log.Panicf("invalid psync response, continue, %s", e.Value)
	}
	x, err := redis.AsString(r, nil)
	if err != nil {
		log.PanicError(err, "invalid psync response, continue")
	}
	xx := strings.Split(string(x), " ")

	// is full sync?
	if len(xx) == 1 && strings.ToLower(xx[0]) == "continue" {
		// continue
		log.Infof("Event:IncSyncStart\tId:%s\t", conf.Options.Id)
		return runid, offset - 1, nil
	} else if len(xx) >= 3 && strings.ToLower(xx[0]) == "fullresync" {
		v, err := strconv.ParseInt(xx[2], 10, 64)
		if err != nil {
			log.PanicError(err, "parse psync offset failed")
		}

		log.Infof("Event:FullSyncStart\tId:%s\t", conf.Options.Id)
		runid, offset := xx[1], v

		return runid, offset, waitRdbDump(br)
	} else {
		log.Panicf("invalid psync response = '%s', should be continue", x)
	}

	// unreachable
	return "", -1, nil
}

func SendPSyncAck(bw *bufio.Writer, offset int64) error {
	cmd := redis.NewCommand("replconf", "ack", offset)
	return redis.Encode(bw, cmd, true)
}

// TODO
func paclusterSlotsHB(c redigo.Conn) {
	_, err := Values(c.Do("pacluster", "slotshb"))
	if err != nil {
		log.PanicError(err, "pacluster slotshb error")
	}
}

func SelectDB(c redigo.Conn, db uint32) {
	s, err := String(c.Do("select", db))
	if err != nil {
		log.PanicError(err, "select command error")
	}
	if s != "OK" {
		log.Panicf("select command response = '%s', should be 'OK'", s)
	}
}

func lpush(c redigo.Conn, key []byte, field []byte) {
	_, err := Int64(c.Do("lpush", string(key), string(field)))
	if err != nil {
		log.PanicError(err, "lpush command error")
	}
}

func rpush(c redigo.Conn, key []byte, field []byte) {
	_, err := Int64(c.Do("rpush", string(key), string(field)))
	if err != nil {
		log.PanicError(err, "rpush command error")
	}
}

func Float64ToByte(float float64) string {
	return strconv.FormatFloat(float, 'f', -1, 64)
}

func zadd(c redigo.Conn, key []byte, score []byte, member []byte) {
	_, err := Int64(c.Do("zadd", string(key), string(score), string(member)))
	if err != nil {
		log.PanicError(err, "zadd command error key ", string(key))
	}
}

func sadd(c redigo.Conn, key []byte, member []byte) {
	_, err := Int64(c.Do("sadd", key, member))
	if err != nil {
		log.PanicError(err, "sadd command error key:", string(key))
	}
}

func hset(c redigo.Conn, key []byte, field []byte, value []byte) {
	_, err := Int64(c.Do("hset", string(key), string(field), string(value)))
	if err != nil {
		log.PanicError(err, "hset command error key ", string(key))
	}
}

func set(c redigo.Conn, key []byte, value []byte) {
	s, err := String(c.Do("set", string(key), string(value)))
	if err != nil {
		log.PanicError(err, "set command error")
	}
	if s != "OK" {
		log.Panicf("set command response = '%s', should be 'OK'", s)
	}
}

func flushAndCheckReply(c redigo.Conn, count int) {
	// for redis-go-cluster driver, "Receive" function returns all the replies once flushed.
	// However, this action is different with redigo driver that "Receive" only returns 1
	// reply each time.
	c.Flush()
	for j := 0; j < count; j++ {
		_, err := c.Receive()
		if err != nil {
			log.PanicError(err, "flush command to redis failed")
		}
	}
}

func restoreQuicklistEntry(c redigo.Conn, e *rdb.BinEntry) {

	r := rdb.NewRdbReader(bytes.NewReader(e.Value))
	_, err := r.ReadByte()
	if err != nil {
		log.PanicError(err, "read rdb ")
	}
	// log.Info("restore quicklist key: ", string(e.Key), ", type: ", e.Type)

	count := 0
	if n, err := r.ReadLength(); err != nil {
		log.PanicError(err, "read rdb ")
	} else {
		// log.Info("quicklist item size: ", int(n))
		for i := 0; i < int(n); i++ {
			ziplist, err := r.ReadString()
			// log.Info("zipList: ", ziplist)
			if err != nil {
				log.PanicError(err, "read rdb ")
			}
			buf := rdb.NewSliceBuffer(ziplist)
			if zln, err := r.ReadZiplistLength(buf); err != nil {
				log.PanicError(err, "read rdb")
			} else {
				// log.Info("ziplist one of quicklist, size: ", int(zln))
				for i := int64(0); i < zln; i++ {
					entry, err := r.ReadZiplistEntry(buf)
					if err != nil {
						log.PanicError(err, "read rdb ")
					}
					// log.Info("rpush key: ", e.Key, " value: ", entry)
					count++
					c.Send("RPUSH", e.Key, entry)
					if count == 100 {
						flushAndCheckReply(c, count)
						count = 0
					}
				}
				flushAndCheckReply(c, count)
				count = 0
			}
		}
	}
}

func restoreBigRdbEntry(c redigo.Conn, e *rdb.BinEntry) error {
	//read type
	var err error
	r := rdb.NewRdbReader(bytes.NewReader(e.Value))
	t, err := r.ReadByte()
	if err != nil {
		log.PanicError(err, "read rdb ")
	}

	log.Debug("restore big key ", string(e.Key), " Value Length ", len(e.Value), " type ", t)
	count := 0
	switch t {
	case rdb.RdbTypeHashZiplist:
		//ziplist
		ziplist, err := r.ReadString()
		if err != nil {
			log.PanicError(err, "read rdb ")
		}
		buf := rdb.NewSliceBuffer(ziplist)
		length, err := r.ReadZiplistLength(buf)
		if err != nil {
			log.PanicError(err, "read rdb ")
		}
		length /= 2
		log.Info("restore big hash key ", string(e.Key), " field count ", length)
		for i := int64(0); i < length; i++ {
			field, err := r.ReadZiplistEntry(buf)
			if err != nil {
				log.PanicError(err, "read rdb ")
			}
			value, err := r.ReadZiplistEntry(buf)
			if err != nil {
				log.PanicError(err, "read rdb ")
			}
			count++
			err = c.Send("HSET", e.Key, field, value)
			if (count == 100) || (i == (length - 1)) {
				flushAndCheckReply(c, count)
				count = 0
			}
			//hset(c, e.Key, field, value)
		}
	case rdb.RdbTypeZSetZiplist:
		ziplist, err := r.ReadString()
		if err != nil {
			log.PanicError(err, "read rdb ")
		}
		buf := rdb.NewSliceBuffer(ziplist)
		cardinality, err := r.ReadZiplistLength(buf)
		if err != nil {
			log.PanicError(err, "read rdb ")
		}
		cardinality /= 2
		log.Info("restore big zset key ", string(e.Key), " field count ", cardinality)
		for i := int64(0); i < cardinality; i++ {
			member, err := r.ReadZiplistEntry(buf)
			if err != nil {
				log.PanicError(err, "read rdb ")
			}
			scoreBytes, err := r.ReadZiplistEntry(buf)
			if err != nil {
				log.PanicError(err, "read rdb ")
			}
			_, err = strconv.ParseFloat(string(scoreBytes), 64)
			if err != nil {
				log.PanicError(err, "read rdb ")
			}
			count++
			err = c.Send("ZADD", e.Key, scoreBytes, member)
			if (count == 100) || (i == (cardinality - 1)) {
				flushAndCheckReply(c, count)
				count = 0
			}
			//zadd(c, e.Key, scoreBytes, member)
		}
	case rdb.RdbTypeSetIntset:
		intset, err := r.ReadString()
		if err != nil {
			log.PanicError(err, "read rdb ")
		}
		buf := rdb.NewSliceBuffer(intset)
		intSizeBytes, err := buf.Slice(4)
		if err != nil {
			log.PanicError(err, "read rdb ")
		}
		intSize := binary.LittleEndian.Uint32(intSizeBytes)

		if intSize != 2 && intSize != 4 && intSize != 8 {
			log.PanicError(err, "rdb: unknown intset encoding ")
		}

		lenBytes, err := buf.Slice(4)
		if err != nil {
			log.PanicError(err, "read rdb ")
		}
		cardinality := binary.LittleEndian.Uint32(lenBytes)

		log.Info("restore big set key ", string(e.Key), " field count ", cardinality)
		for i := uint32(0); i < cardinality; i++ {
			intBytes, err := buf.Slice(int(intSize))
			if err != nil {
				log.PanicError(err, "read rdb ")
			}
			var intString string
			switch intSize {
			case 2:
				intString = strconv.FormatInt(int64(int16(binary.LittleEndian.Uint16(intBytes))), 10)
			case 4:
				intString = strconv.FormatInt(int64(int32(binary.LittleEndian.Uint32(intBytes))), 10)
			case 8:
				intString = strconv.FormatInt(int64(int64(binary.LittleEndian.Uint64(intBytes))), 10)
			}
			count++
			err = c.Send("SADD", e.Key, []byte(intString))
			if (count == 100) || (i == (cardinality - 1)) {
				flushAndCheckReply(c, count)
				count = 0
			}
			//sadd(c, e.Key, []byte(intString))
		}
	case rdb.RdbTypeListZiplist:
		ziplist, err := r.ReadString()
		if err != nil {
			log.PanicError(err, "read rdb ")
		}
		buf := rdb.NewSliceBuffer(ziplist)
		length, err := r.ReadZiplistLength(buf)
		if err != nil {
			log.PanicError(err, "read rdb ")
		}
		log.Info("restore big list key ", string(e.Key), " field count ", length)
		for i := int64(0); i < length; i++ {
			entry, err := r.ReadZiplistEntry(buf)
			if err != nil {
				log.PanicError(err, "read rdb ")
			}

			//rpush(c, e.Key, entry)
			count++
			if err = c.Send("RPUSH", e.Key, entry); err != nil {
				break
			}
			if (count == 100) || (i == (length - 1)) {
				flushAndCheckReply(c, count)
				count = 0
			}
		}
	case rdb.RdbTypeHashZipmap:
		var length int
		zipmap, err := r.ReadString()
		if err != nil {
			log.PanicError(err, "read rdb ")
		}
		buf := rdb.NewSliceBuffer(zipmap)
		lenByte, err := buf.ReadByte()
		if err != nil {
			log.PanicError(err, "read rdb ")
		}
		if lenByte >= 254 { // we need to count the items manually
			length, err = r.CountZipmapItems(buf)
			length /= 2
			if err != nil {
				log.PanicError(err, "read rdb ")
			}
		} else {
			length = int(lenByte)
		}
		log.Info("restore big hash key ", string(e.Key), " field count ", length)
		for i := 0; i < length; i++ {
			field, err := r.ReadZipmapItem(buf, false)
			if err != nil {
				log.PanicError(err, "read rdb ")
			}
			value, err := r.ReadZipmapItem(buf, true)
			if err != nil {
				log.PanicError(err, "read rdb ")
			}
			count++
			if err = c.Send("HSET", e.Key, field, value); err != nil {
				break
			}

			if (count == 100) || (i == (int(length) - 1)) {
				flushAndCheckReply(c, count)
				count = 0
			}
			//hset(c, e.Key, field, value)
		}
	case rdb.RdbTypeString:
		value, err := r.ReadString()
		if err != nil {
			log.PanicError(err, "read rdb ")
		}
		set(c, e.Key, value)
	case rdb.RdbTypeList:
		if n, err := r.ReadLength(); err != nil {
			log.PanicError(err, "read rdb ")
		} else {
			log.Info("restore big list key ", string(e.Key), " field count ", int(n))
			for i := 0; i < int(n); i++ {
				field, err := r.ReadString()
				if err != nil {
					log.PanicError(err, "read rdb ")
				}

				//rpush(c, e.Key, field)
				count++
				if err = c.Send("RPUSH", e.Key, field); err != nil {
					break
				}
				if (count == 100) || (i == (int(n) - 1)) {
					flushAndCheckReply(c, count)
					count = 0
				}
			}
		}
	case rdb.RdbTypeSet:
		if n, err := r.ReadLength(); err != nil {
			log.PanicError(err, "read rdb ")
		} else {
			log.Info("restore big set key ", string(e.Key), " field count ", int(n))
			for i := 0; i < int(n); i++ {
				member, err := r.ReadString()
				if err != nil {
					log.PanicError(err, "read rdb ")
				}

				count++
				if err = c.Send("SADD", e.Key, member); err != nil {
					break
				}
				if (count == 100) || (i == (int(n) - 1)) {
					flushAndCheckReply(c, count)
					count = 0
				}
				//sadd(c, e.Key, member)
			}
		}
	case rdb.RdbTypeZSet, rdb.RdbTypeZSet2:
		if n, err := r.ReadLength(); err != nil {
			log.PanicError(err, "read rdb ")
		} else {
			log.Info("restore big zset key ", string(e.Key), " field count ", int(n))
			for i := 0; i < int(n); i++ {
				member, err := r.ReadString()
				if err != nil {
					log.PanicError(err, "read rdb ")
				}
				var score float64
				if t == rdb.RdbTypeZSet2 {
					score, err = r.ReadDouble()
				} else {
					score, err = r.ReadFloat()
				}
				if err != nil {
					log.PanicError(err, "read rdb ")
				}

				count++
				log.Info("restore big zset key ", string(e.Key), " score ", (Float64ToByte(score)), " member ", string(member))
				if c.Send("ZADD", e.Key, Float64ToByte(score), member); err != nil {
					break
				}

				if (count == 100) || (i == (int(n) - 1)) {
					flushAndCheckReply(c, count)
					count = 0
				}
				//zadd(c, e.Key, Float64ToByte(score), member)
			}
		}
	case rdb.RdbTypeHash:
		var n uint32
		if e.NeedReadLen == 1 {
			rlen, err := r.ReadLength()
			if err != nil {
				log.PanicError(err, "read rdb ")
			}
			if e.RealMemberCount != 0 {
				n = e.RealMemberCount
			} else {
				n = rlen
			}
		} else {
			n = e.RealMemberCount
		}
		log.Info("restore big hash key ", string(e.Key), " field count ", int(n))
		for i := 0; i < int(n); i++ {
			field, err := r.ReadString()
			if err != nil {
				log.Info("idx: ", i, " n: ", n)
				log.PanicError(err, "read rdb ")
			}
			value, err := r.ReadString()
			if err != nil {
				log.Info("idx: ", i, " n: ", n)
				log.PanicError(err, "read rdb ")
			}

			//hset(c, e.Key, field, value)
			count++
			if err = c.Send("HSET", e.Key, field, value); err != nil {
				break
			}

			if (count == 100) || (i == (int(n) - 1)) {
				flushAndCheckReply(c, count)
				count = 0
			}
		}
		log.Info("complete restore big hash key: ", string(e.Key), " field:", n)
	case rdb.RdbTypeQuicklist:
		if n, err := r.ReadLength(); err != nil {
			log.PanicError(err, "read rdb ")
		} else {
			log.Info("quicklist item size: ", int(n))
			for i := 0; i < int(n); i++ {
				ziplist, err := r.ReadString()
				log.Info("zipList: ", ziplist)
				if err != nil {
					log.PanicError(err, "read rdb ")
				}
				buf := rdb.NewSliceBuffer(ziplist)
				if zln, err := r.ReadZiplistLength(buf); err != nil {
					log.PanicError(err, "read rdb")
				} else {
					log.Info("ziplist one of quicklist, size: ", int(zln))
					for i := int64(0); i < zln; i++ {
						entry, err := r.ReadZiplistEntry(buf)
						if err != nil {
							log.PanicError(err, "read rdb ")
						}

						log.Info("rpush key: ", e.Key, " value: ", entry)
						count++
						if err = c.Send("RPUSH", e.Key, entry); err != nil {
							return err
						}
						if count == 100 {
							flushAndCheckReply(c, count)
							count = 0
						}
					}
					flushAndCheckReply(c, count)
					count = 0
				}
			}
		}
	default:
		log.PanicError(fmt.Errorf("can't deal rdb type:%d", t), "restore big key fail")
	}

	return err
}

func RestoreRdbEntry(c redigo.Conn, e *rdb.BinEntry) {
	/*
	 * for ucloud, special judge.
	 * 046110.key -> key
	 */
	if conf.Options.SourceRdbSpecialCloud == UCloudCluster {
		e.Key = e.Key[7:]
	}

	var ttlms uint64
	if conf.Options.ReplaceHashTag {
		e.Key = bytes.Replace(e.Key, []byte("{"), []byte(""), 1)
		e.Key = bytes.Replace(e.Key, []byte("}"), []byte(""), 1)
	}
	if e.ExpireAt != 0 {
		now := uint64(time.Now().Add(conf.Options.ShiftTime).UnixNano())
		now /= uint64(time.Millisecond)
		if now >= e.ExpireAt {
			ttlms = 1
		} else {
			ttlms = e.ExpireAt - now
		}
	}
	if e.Type == rdb.RdbTypeQuicklist {
		exist, err := Bool(c.Do("exists", e.Key))
		if err != nil {
			log.Panicf(err.Error())
		}
		if exist {
			switch conf.Options.KeyExists {
			case "rewrite":
				if !conf.Options.Metric {
					log.Infof("warning, rewrite key: %v", string(e.Key))
				}
				_, err := Int64(c.Do("del", e.Key))
				if err != nil {
					log.Panicf("del ", string(e.Key), err)
				}
			case "ignore":
				log.Warnf("target key name is busy but ignore: %v", string(e.Key))
			case "none":
				log.Panicf("target key name is busy: %v", string(e.Key))
			}
		}
		restoreQuicklistEntry(c, e)
		if e.ExpireAt != 0 {
			r, err := Int64(c.Do("pexpire", e.Key, ttlms))
			if err != nil && r != 1 {
				log.Panicf("expire ", string(e.Key), err)
			}
		}
		return
	}

	// load lua script
	if e.Type == rdb.RdbFlagAUX && string(e.Key) == "lua" {
		if conf.Options.FilterLua == false {
			_, err := c.Do("script", "load", e.Value)
			if err != nil {
				log.Panicf(err.Error())
			}
		}
		return
	}

	// TODO, need to judge big key
	if e.Type != rdb.RDBTypeStreamListPacks &&
			(uint64(len(e.Value)) > conf.Options.BigKeyThreshold || e.RealMemberCount != 0) {
		log.Debugf("restore big key[%s] with length[%v] and member count[%v]", e.Key, len(e.Value), e.RealMemberCount)
		//use command
		if conf.Options.KeyExists == "rewrite" && e.NeedReadLen == 1 {
			if !conf.Options.Metric {
				log.Infof("warning, rewrite big key:", string(e.Key))
			}
			_, err := Int64(c.Do("del", e.Key))
			if err != nil {
				log.Panicf("del ", string(e.Key), err)
			}
		}

		if err := restoreBigRdbEntry(c, e); err != nil {
			log.Panic(err)
		}

		if e.ExpireAt != 0 {
			r, err := Int64(c.Do("pexpire", e.Key, ttlms))
			if err != nil && r != 1 {
				log.Panicf("expire ", string(e.Key), err)
			}
		}
		return
	}

	params := []interface{}{e.Key, ttlms, e.Value}
	if ret := CompareVersion(conf.Options.TargetVersion, "5.0", 2); ret == 0 || ret == 2 {
		if e.IdleTime != 0 {
			params = append(params, "IDLETIME")
			params = append(params, e.IdleTime)
		}
		if e.Freq != 0 {
			params = append(params, "FREQ")
			params = append(params, e.Freq)
		}
	}

	log.Debugf("restore key[%s] with params[%v]", e.Key, params)
	// fmt.Printf("key: %v, value: %v params: %v\n", string(e.Key), e.Value, params)
	// s, err := String(c.Do("restore", params...))
RESTORE:
	s, err := redigoCluster.String(c.Do("restore", params...))
	if err != nil {
		/*The reply value of busykey in 2.8 kernel is "target key name is busy",
		  but in 4.0 kernel is "BUSYKEY Target key name already exists"*/
		if strings.Contains(err.Error(), "Target key name is busy") ||
			strings.Contains(err.Error(), "BUSYKEY Target key name already exists") {
			switch conf.Options.KeyExists {
			case "rewrite":
				if !conf.Options.Metric {
					log.Infof("warning, rewrite key: %v", string(e.Key))
				}

				if conf.Options.TargetReplace {
					params = append(params, "REPLACE")
				} else {
					_, err = redigoCluster.Int(c.Do("del", e.Key))
					if err != nil {
						log.Panicf("delete key[%v] failed[%v]", string(e.Key), err)
					}
				}

				goto RESTORE
			case "ignore":
				log.Warnf("target key name is busy but ignore: %v", string(e.Key))
				case "none":
				log.Panicf("target key name is busy: %v", string(e.Key))
			}
		} else if strings.Contains(err.Error(), "Bad data format") {
			// from big version to small version may has this error. we need to split the data struct
			log.Warnf("return error[%v], ignore it and try to split the value", err)
			if err := restoreBigRdbEntry(c, e); err != nil {
				log.Panic(err)
			}
		} else if strings.Contains(err.Error(), "Bad data format") {
			// from big version to small version may has this error. we need to split the data struct
			log.Warnf("return error[%v], ignore it and try to split the value", err)
			if err := restoreBigRdbEntry(c, e); err != nil {
				log.Panic(err)
			}
		} else {
			log.PanicError(err, "restore command error key:", string(e.Key), " err:", err.Error())
		}
	} else if s != "OK" {
		log.Panicf("restore command response = '%s', should be 'OK'", s)
	}
}

func Iocopy(r io.Reader, w io.Writer, p []byte, max int) int {
	if max <= 0 || len(p) == 0 {
		log.Panicf("invalid max = %d, len(p) = %d", max, len(p))
	}
	if len(p) > max {
		p = p[:max]
	}
	if n, err := r.Read(p); err != nil {
		log.PanicError(err, "read error, please check source redis log or network")
	} else {
		p = p[:n]
	}
	if _, err := w.Write(p); err != nil {
		log.PanicError(err, "write error")
	}
	return len(p)
}

func FlushWriter(w *bufio.Writer) {
	if err := w.Flush(); err != nil {
		log.PanicError(err, "flush error")
	}
}

func NewRDBLoader(reader *bufio.Reader, rbytes *atomic2.Int64, size int) chan *rdb.BinEntry {
	pipe := make(chan *rdb.BinEntry, size)
	go func() {
		defer close(pipe)
		l := rdb.NewLoader(stats.NewCountReader(reader, rbytes))
		if err := l.Header(); err != nil {
			log.PanicError(err, "parse rdb header error")
		}
		for {
			if entry, err := l.NextBinEntry(); err != nil {
				log.PanicError(err, "parse rdb entry error, if the err is :EOF, please check that if the src db log has client outout buffer oom, if so set output buffer larger.")
			} else {
				if entry != nil {
					pipe <- entry
				} else {
					if rdb.FromVersion > 2 {
						if err := l.Footer(); err != nil {
							log.PanicError(err, "parse rdb checksum error")
						}
					}
					return
				}
			}
		}
	}()
	return pipe
}

func GetRedisVersion(target, authType, auth string, tlsEnable bool) (string, error) {
	c := OpenRedisConn([]string{target}, authType, auth, false, tlsEnable)
	defer c.Close()

	infoStr, err := Bytes(c.Do("info", "server"))
	if err != nil {
		if err.Error() == CoidsErrMsg {
			// "info xxx" command is disable in codis, try to use "info" and parse "xxx"
			infoStr, err = Bytes(c.Do("info"))
			infoStr = CutRedisInfoSegment(infoStr, "server")
		} else {
			return "", err
		}
	}

	infoKV := ParseRedisInfo(infoStr)
	if value, ok := infoKV["redis_version"]; ok {
		return value, nil
	} else {
		return "", fmt.Errorf("MissingRedisVersionInInfo")
	}
}

func GetRDBChecksum(target, authType, auth string, tlsEnable bool) (string, error) {
	c := OpenRedisConn([]string{target}, authType, auth, false, tlsEnable)
	defer c.Close()

	content, err := c.Do("config", "get", "rdbchecksum")
	if err != nil {
		return "", err
	}

	conentList := content.([]interface{})
	if len(conentList) != 2 {
		return "", fmt.Errorf("return length != 2, return: %v", conentList)
	}
	return string(conentList[1].([]byte)), nil
}

func CheckHandleNetError(err error) bool {
	if err == io.EOF {
		return true
	} else if _, ok := err.(net.Error); ok {
		return true
	}
	return false
}

func GetFakeSlaveOffset(c redigo.Conn) (string, error) {
	infoStr, err := Bytes(c.Do("info", "Replication"))
	if err != nil {
		return "", err
	}

	kv := ParseRedisInfo(infoStr)

	for k, v := range kv {
		if strings.Contains(k, "slave") && strings.Contains(v, fmt.Sprintf("port=%d", conf.Options.HttpProfile)) {
			list := strings.Split(v, ",")
			for _, item := range list {
				if strings.HasPrefix(item, "offset=") {
					return strings.Split(item, "=")[1], nil
				}
			}
		}
	}
	return "", fmt.Errorf("OffsetNotFoundInInfo")
}

func GetLocalIp(preferdInterfaces []string) (ip string, interfaceName string, err error) {
	var addr net.Addr
	ip = ""
	for _, name := range preferdInterfaces {
		i, err := net.InterfaceByName(name)
		if err != nil {
			continue
		}
		addrs, err := i.Addrs()
		if err != nil || len(addrs) == 0 {
			continue
		}
		addr = addrs[0]

		switch v := addr.(type) {
		case *net.IPNet:
			ip = v.IP.String()
		case *net.IPAddr:
			ip = v.IP.String()
		}
		if len(ip) != 0 {
			return ip, name, nil
		}
	}
	return ip, "", fmt.Errorf("fetch local ip failed, interfaces: %s", strings.Join(preferdInterfaces, ","))
}

// GetReadableRedisAddressThroughSentinel gets readable redis address
// First, the function will pick one from available slaves randomly.
// If there is no available slave, it will pick master.
func GetReadableRedisAddressThroughSentinel(sentinelAddrs []string, sentinelMasterName string, fromMaster bool) (string, error) {
	sentinelGroup := sentinel.Sentinel{
		Addrs:      sentinelAddrs,
		MasterName: sentinelMasterName,
		Dial:       defaultDialFunction,
	}
	if fromMaster == false {
		if slaves, err := sentinelGroup.Slaves(); err == nil {
			if addr, err := getAvailableSlaveAddress(slaves); err == nil {
				return addr, nil
			} else {
				return "", err
			}
		} else {
			return "", err
		}
	}
	return sentinelGroup.MasterAddr()
}

// getAvailableSlaveAddress picks a slave address randomly.
func getAvailableSlaveAddress(slaves []*sentinel.Slave) (string, error) {
	for {
		length := len(slaves)
		if length == 0 {
			break
		}
		randSlaveIndex := rand.Intn(length)
		if slave := slaves[randSlaveIndex]; slave.Available() {
			return slave.Addr(), nil
		}
		slaves = append(slaves[:randSlaveIndex], slaves[randSlaveIndex+1:]...)
	}
	return "", fmt.Errorf("there is no available slave")
}

// getWritableRedisAddressThroughSentinel gets writable redis address
// The function will return redis master address.
func GetWritableRedisAddressThroughSentinel(sentinelAddrs []string, sentinelMasterName string) (string, error) {
	sentinelGroup := sentinel.Sentinel{
		Addrs:      sentinelAddrs,
		MasterName: sentinelMasterName,
		Dial:       defaultDialFunction,
	}
	return sentinelGroup.MasterAddr()
}

var defaultDialFunction = func(addr string) (redigo.Conn, error) {
	timeout := 500 * time.Millisecond
	c, err := redigo.DialTimeout("tcp", addr, timeout, timeout, timeout)
	if err != nil {
		return nil, err
	}
	return c, nil
}
