package client

import (
	"bufio"
	"crypto/tls"
	"net"
	"strconv"
	"strings"
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

	ReplId string
}

func NewRedisClient(address string, username string, password string, Tls bool) *Redis {
	r := new(Redis)
	var conn net.Conn
	var dialer net.Dialer
	var err error
	dialer.Timeout = 3 * time.Second
	if Tls {
		conn, err = tls.DialWithDialer(&dialer, "tcp", address, &tls.Config{InsecureSkipVerify: true})
	} else {
		conn, err = dialer.Dial("tcp", address)
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

	// replId
	r.ReplId = func() string {
		reply := r.DoWithStringReply("info", "replication")
		replyIdField := "master_replid"
		idx1 := strings.Index(reply, replyIdField)
		if idx1 < 0 {
			log.Panicf("can not found replid with reply: %s", reply)
		}
		idx2 := strings.IndexByte(reply[idx1:], '\r')
		if idx2 < 0 {
			log.Panicf("can not found replid with reply: %s", reply)
		}
		return reply[idx1 + len(replyIdField) + 1 : idx1 + idx2]
	}()

	// ping to test connection
	reply := r.DoWithStringReply("ping")
	if reply != "PONG" {
		panic("ping failed with reply: " + reply)
	}

	return r
}

func (r *Redis) DoWithStringReply(args ...string) string {
	r.Send(args...)

	replyInterface, err := r.Receive()
	if err != nil {
		log.Panicf(err.Error())
	}
	reply := replyInterface.(string)
	return reply
}

func (r *Redis) Do(args ...string) interface{} {
	r.Send(args...)

	reply, err := r.Receive()
	if err != nil {
		log.Panicf(err.Error())
	}
	return reply
}

func (r *Redis) Send(args ...string) {
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

func (r *Redis) flush() {
	err := r.writer.Flush()
	if err != nil {
		log.Panicf(err.Error())
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
}

/* Commands */

func (r *Redis) Scan(cursor uint64) (newCursor uint64, keys []string) {
	r.Send("scan", strconv.FormatUint(cursor, 10), "count", "2048")
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
