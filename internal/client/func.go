package client

import (
	"bytes"
	"github.com/alibaba/RedisShake/internal/client/proto"
	"github.com/alibaba/RedisShake/internal/log"
)

func EncodeArgv(argv []string, buf *bytes.Buffer) {
	writer := proto.NewWriter(buf)
	argvInterface := make([]interface{}, len(argv))

	for inx, item := range argv {
		argvInterface[inx] = item
	}
	err := writer.WriteArgs(argvInterface)
	if err != nil {
		log.PanicError(err)
	}
}
