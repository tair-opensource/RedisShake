package entry

import (
	"RedisShake/internal/client/proto"
	"RedisShake/internal/commands"
	"RedisShake/internal/log"
	"bytes"
	"strings"
)

type Entry struct {
	DbId int      // required
	Argv []string // required

	CmdName string
	Group   string
	Keys    []string
	Slots   []int

	// for stat
	SerializedSize int64
}

func NewEntry() *Entry {
	e := new(Entry)
	return e
}

func (e *Entry) String() string {
	str := strings.Join(e.Argv, " ")
	if len(str) > 100 {
		str = str[:100] + "..."
	}
	return str
}

func (e *Entry) Serialize() []byte {
	buf := new(bytes.Buffer)
	writer := proto.NewWriter(buf)
	argvInterface := make([]interface{}, len(e.Argv))

	for inx, item := range e.Argv {
		argvInterface[inx] = item
	}
	err := writer.WriteArgs(argvInterface)
	if err != nil {
		log.Panicf(err.Error())
	}
	e.SerializedSize = int64(buf.Len())
	return buf.Bytes()
}

func (e *Entry) Preprocess() {
	e.CmdName, e.Group, e.Keys = commands.CalcKeys(e.Argv)
	e.Slots = commands.CalcSlots(e.Keys)
}
