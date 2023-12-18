package client

import (
	"bytes"
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
		log.Panicf(err.Error())
	}
}

// IsCluster is for determining whether the server is in cluster mode.
func (r *Redis) IsCluster() bool {
	reply := r.DoWithStringReply("INFO", "Cluster")
	return strings.Contains(reply, "cluster_enabled:1")
}
