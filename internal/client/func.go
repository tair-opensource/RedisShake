package client

import (
	"bytes"
	"errors"
	"strings"

	"RedisShake/internal/client/proto"
	"RedisShake/internal/log"
)

func EncodeArgv(argv []string, buf *bytes.Buffer) {
	writer := proto.NewWriter(buf)
	argvInterface := make([]interface{}, len(argv))

	for inx, item := range argv {
		argvInterface[inx] = item
	}
	err := writer.WriteArgs(argvInterface)
	if err != nil {
		log.Panicf("%v", err)
	}
}

// IsCluster is for determining whether the server is in cluster mode.
func (r *Redis) IsCluster() bool {
	reply := r.DoWithStringReply("INFO", "Cluster")
	return strings.Contains(reply, "cluster_enabled:1")
}

// ParseServerVersion parses the server info string and returns whether the server is Valkey.
// Returns true if the server is Valkey, false if it's Redis, and an error if neither is found.
func ParseServerVersion(serverInfo string) (isValkey bool, err error) {
	for _, line := range strings.Split(serverInfo, "\n") {
		line = strings.TrimSpace(line)
		if strings.HasPrefix(line, "valkey_version:") {
			return true, nil
		}
		if strings.HasPrefix(line, "redis_version:") {
			return false, nil
		}
	}
	return false, errors.New("server version not found in info string")
}

// IsValkey detects whether the connected server is Valkey by checking the server info.
// Returns true if the server is Valkey, false if it's Redis.
func (r *Redis) IsValkey() bool {
	reply := r.DoWithStringReply("INFO", "server")
	isValkey, err := ParseServerVersion(reply)
	if err != nil {
		log.Warnf("failed to detect server type: %v, assuming Redis", err)
		return false
	}
	return isValkey
}
