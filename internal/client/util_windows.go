package client

import (
	"RedisShake/internal/log"
	"net"
	"syscall"
)

func getConnBuff(conn net.Conn) (sendBufSize int) {
	// get the underlying socket descriptor
	rawConn, err := conn.(*net.TCPConn).SyscallConn()
	if err != nil {
		log.Warnf("Error getting raw connection: %v", err)
		return
	}

	err = rawConn.Control(func(fd uintptr) {
		// get the write buffer size
		sendBufSize, err = syscall.GetsockoptInt(syscall.Handle(fd), syscall.SOL_SOCKET, syscall.SO_SNDBUF)
	})
	if err != nil {
		log.Warnf("Error getting send buffer size: %v", err)
		return
	}
	return
}
