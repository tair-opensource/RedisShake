package client

import (
	"RedisShake/internal/log"
	"net"
)

func SetWriteConnBuff(conn net.Conn, size int) {
	bufSize := getConnBuff(conn)
	if bufSize >= size {
		log.Debugf("send buffer size is enough, no need to change. current %d", bufSize)
		return
	}
	err := conn.(*net.TCPConn).SetWriteBuffer(size)
	if err != nil {
		log.Warnf("set tcpConn write buffer size 128K failed. err=[%v]", err)
	}
}
