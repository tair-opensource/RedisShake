package client

import (
	"RedisShake/internal/log"
	"fmt"
	"net"
	"syscall"
)

func getConnBuff(conn net.Conn) (sendBufSize int) {
	// get the underlying socket descriptor
	rawConn, err := conn.(*net.TCPConn).SyscallConn()
	if err != nil {
		fmt.Println("Error getting raw connection:", err)
		return
	}

	err = rawConn.Control(func(fd uintptr) {
		// get the write buffer size
		sendBufSize, err = syscall.GetsockoptInt(syscall.Handle(fd), syscall.SOL_SOCKET, syscall.SO_SNDBUF)
	})
	if err != nil {
		fmt.Println("Error getting send buffer size:", err)
		return
	}
	return
}

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
