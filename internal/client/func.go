package client

import (
	"bytes"
	"github.com/alibaba/RedisShake/internal/client/proto"
	"github.com/alibaba/RedisShake/internal/log"
)

func ArrayString(replyInterface interface{}, err error) []string {
	if err != nil {
		log.PanicError(err)
	}
	replyArray := replyInterface.([]interface{})
	replyArrayString := make([]string, len(replyArray))
	for inx, item := range replyArray {
		replyArrayString[inx] = item.(string)
	}
	return replyArrayString
}

func EncodeArgv(argv []string) *bytes.Buffer {
	buf := new(bytes.Buffer)
	writer := proto.NewWriter(buf)
	argvInterface := make([]interface{}, len(argv))

	for inx, item := range argv {
		argvInterface[inx] = item
	}
	err := writer.WriteArgs(argvInterface)
	if err != nil {
		log.PanicError(err)
	}
	return buf
}
