package client

import (
	"bytes"
	"errors"
	"fmt"
	"strconv"
	"strings"
	"time"

	"RedisShake/internal/client/proto"
	"RedisShake/internal/config"
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
		log.Panicf(err.Error())
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

func (r *Redis) FlushAllAsync() error {
	reply := r.DoWithStringReply("FLUSHALL", "ASYNC")
	if reply != "OK" {
		return fmt.Errorf("FLUSHALL ASYNC failed: %s", reply)
	}

	deadline := time.Now().Add(config.Opt.Advanced.LazyFreePendingObjectsMaxWait)
	for time.Now().Before(deadline) {
		info := r.DoWithStringReply("INFO", "memory")
		pending := parseLazyFreePendingObjects(info)
		if pending == 0 {
			return nil
		}
		time.Sleep(config.Opt.Advanced.LazyFreePendingObjectsCheckInterval)
	}
	return fmt.Errorf("timeout waiting for lazyfree_pending_objects to be 0")
}

func parseLazyFreePendingObjects(info string) int {
	for _, line := range strings.Split(info, "\n") {
		line = strings.TrimSpace(line)
		if strings.HasPrefix(line, "lazyfree_pending_objects:") {
			parts := strings.Split(line, ":")
			if len(parts) >= 2 {
				val, err := strconv.Atoi(strings.TrimSpace(parts[1]))
				if err != nil {
					return -1
				}
				return val
			}
		}
	}
	return -1
}
