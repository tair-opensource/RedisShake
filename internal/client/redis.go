package client

import (
	"bufio"
	"context"
	"crypto/tls"
	"net"
	"regexp"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"RedisShake/internal/client/proto"
	"RedisShake/internal/log"
)

type Redis struct {
	conn        net.Conn
	reader      *bufio.Reader
	writer      *bufio.Writer
	protoReader *proto.Reader
	protoWriter *proto.Writer
	timer     *time.Timer
	sendCount uint64
	mu        sync.Mutex
}

func NewSentinelMasterClient(ctx context.Context, address string, username string, password string, Tls bool) *Redis {
	return NewRedisClient(ctx, address, username, password, Tls, false)
}

func NewRedisClient(ctx context.Context, address string, username string, password string, Tls bool, replica bool) *Redis {
	r := new(Redis)
	var conn net.Conn
	var dialer = &net.Dialer{
		Timeout:   5 * time.Minute,
		KeepAlive: 5 * time.Minute,
	}
	ctxWithDeadline, cancel := context.WithTimeout(ctx, 1*time.Second)
	defer cancel()
	var err error
	if Tls {
		tlsDialer := &tls.Dialer{
			NetDialer: dialer,
			Config:    &tls.Config{InsecureSkipVerify: true},
		}
		conn, err = tlsDialer.DialContext(ctxWithDeadline, "tcp", address)
	} else {
		conn, err = dialer.DialContext(ctxWithDeadline, "tcp", address)
	}
	if err != nil {
		log.Panicf("dial failed. address=[%s], tls=[%v], err=[%v]", address, Tls, err)
	}

	r.conn = conn
	r.reader = bufio.NewReader(conn)
	r.writer = bufio.NewWriter(conn)
	r.protoReader = proto.NewReader(r.reader)
	r.protoWriter = proto.NewWriter(r.writer)

	// auth
	if password != "" {
		var reply string
		if username != "" {
			reply = r.DoWithStringReply("auth", username, password)
		} else {
			reply = r.DoWithStringReply("auth", password)
		}
		if reply != "OK" {
			log.Panicf("auth failed with reply: %s", reply)
		}
	}

	// ping to test connection
	reply := r.DoWithStringReply("ping")
	if reply != "PONG" {
		panic("ping failed with reply: " + reply)
	}
	reply = r.DoWithStringReply("info", "replication")
	// get best replica
	if replica {
		replicaInfo := getReplicaAddr(reply, address)
		log.Infof("best replica: %s", replicaInfo.BestReplica)
		r = NewRedisClient(ctx, replicaInfo.BestReplica, username, password, Tls, false)
	}

	r.timer = time.NewTimer(time.Second)
	go r.autoFlush(ctx)

	return r
}

type Replica struct {
	Addr   string
	Offset string
}

type RedisReplicaInfo struct {
	Role        string
	BestReplica string
}

func getReplicaAddr(info, addr string) RedisReplicaInfo {
	infoReplica := RedisReplicaInfo{}
	replicas := make([]Replica, 0)
	slaveInfoRegexp := regexp.MustCompile(`slave\d+:ip=.*`)
	for _, line := range strings.Split(info, "\n") {
		line = strings.TrimSpace(line)
		switch {
		case strings.HasPrefix(line, "role:slave"):
			infoReplica.Role = "slave"
			infoReplica.BestReplica = addr
			return infoReplica
		case strings.HasPrefix(line, "role:master"):
			infoReplica.Role = "master"
		case slaveInfoRegexp.MatchString(line):
			slaveInfo := strings.Split(line, ":")
			s1 := slaveInfo[1]
			slaveInfo = strings.Split(s1, ",")
			replica := Replica{}
			var host string
			var port string
			var offset string
			for _, item := range slaveInfo {
				if strings.HasPrefix(item, "ip=") {
					host = strings.Split(item, "=")[1]
				}
				if strings.HasPrefix(item, "port=") {
					port = strings.Split(item, "=")[1]
				}
				if strings.HasPrefix(item, "offset=") {
					offset = strings.Split(item, "=")[1]
				}
			}
			replica.Addr = host + ":" + port
			replica.Offset = offset
			replicas = append(replicas, replica)
		}
	}
	if len(replicas) == 0 {
		log.Panicf("no replica found, should not set `prefer_replica` to true")
	}
	best := replicas[0]
	for _, replica := range replicas {
		if replica.Offset > best.Offset {
			best = replica
		}
	}
	infoReplica.BestReplica = best.Addr
	return infoReplica
}

func (r *Redis) DoWithStringReply(args ...interface{}) string {
	r.Send(args...)

	replyInterface, err := r.Receive()
	if err != nil {
		log.Panicf(err.Error())
	}
	reply := replyInterface.(string)
	return reply
}

func (r *Redis) Do(args ...interface{}) interface{} {
	r.Send(args...)

	reply, err := r.Receive()
	if err != nil {
		log.Panicf(err.Error())
	}
	return reply
}

func (r *Redis) Send(args ...interface{}) {
	argsInterface := make([]interface{}, len(args))
	for inx, item := range args {
		argsInterface[inx] = item
	}
	err := r.protoWriter.WriteArgs(argsInterface)
	if err != nil {
		log.Panicf(err.Error())
	}
	r.flush()
}

func (r *Redis) SendBytes(buf []byte) {
	_, err := r.writer.Write(buf)
	if err != nil {
		log.Panicf(err.Error())
	}
	r.flush()
}

func (r *Redis) SendBytesBuff(buf []byte) {
	r.mu.Lock()
	defer r.mu.Unlock()
	_, err := r.writer.Write(buf)
	if err != nil {
		log.Panicf(err.Error())
	}
	r.flushBuff()
}

func (r *Redis) flushBuff() {
	if !r.timer.Stop() {
		select {
		case <-r.timer.C:
		default:
		}
	}
	r.timer.Reset(time.Second)
	if atomic.AddUint64(&r.sendCount, 1)%100 != 0 {
		return
	}
	r.flush()
}

func (r *Redis) flush() {
	err := r.writer.Flush()
	if err != nil {
		log.Panicf(err.Error())
	}
	atomic.StoreUint64(&r.sendCount, 0)
}

func (r *Redis) autoFlush(ctx context.Context) {
	for {
		select {
		case <-ctx.Done():
			return
		case <-r.timer.C:
			if atomic.LoadUint64(&r.sendCount) > 0 {
				r.mu.Lock()
				err := r.writer.Flush()
				r.mu.Unlock()
				if err != nil {
					log.Panicf(err.Error())
				}
			}
		}
	}
}

func (r *Redis) Receive() (interface{}, error) {
	return r.protoReader.ReadReply()
}

func (r *Redis) ReceiveString() string {
	reply, err := r.Receive()
	if err != nil {
		log.Panicf(err.Error())
	}
	return reply.(string)
}

func (r *Redis) BufioReader() *bufio.Reader {
	return r.reader
}

func (r *Redis) SetBufioReader(rd *bufio.Reader) {
	r.reader = rd
	r.protoReader = proto.NewReader(r.reader)
}

func (r *Redis) Close() {
	if err := r.conn.Close(); err != nil {
		log.Infof("close redis conn err: %s\n", err.Error())
	}
	// release the timer
	if !r.timer.Stop() {
		select {
		case <-r.timer.C:
		default:
		}
	}
}

/* Commands */

func (r *Redis) Scan(cursor uint64, count int) (newCursor uint64, keys []string) {
	r.Send("scan", strconv.FormatUint(cursor, 10), "count", count)
	reply, err := r.Receive()
	if err != nil {
		log.Panicf(err.Error())
	}

	array := reply.([]interface{})
	if len(array) != 2 {
		log.Panicf("scan return length error. ret=%v", reply)
	}

	// cursor
	newCursor, err = strconv.ParseUint(array[0].(string), 10, 64)
	if err != nil {
		log.Panicf(err.Error())
	}
	// keys
	keys = make([]string, 0)
	for _, item := range array[1].([]interface{}) {
		keys = append(keys, item.(string))
	}
	return
}
