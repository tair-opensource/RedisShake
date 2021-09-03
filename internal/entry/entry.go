package entry

import "fmt"

type Entry struct {
	Id          uint64
	IsBase      bool //  whether the command is decoded from dump.rdb file
	DbId        int
	Argv        []string
	TimestampMs uint64

	CmdName string
	Group   string
	Keys    []string
	Slots   []int

	// for statistics
	Offset      int64
	EncodedSize uint64 // the size of the entry after encode
}

func NewEntry() *Entry {
	e := Entry{}
	e.Argv = make([]string, 0)
	e.Keys = make([]string, 0)
	e.Slots = make([]int, 0)
	e.DbId = 0
	e.TimestampMs = 0
	return &e
}

func (e *Entry) NextEntry() *Entry {
	newE := NewEntry()
	newE.Id = e.Id + 1
	newE.DbId = e.DbId
	newE.TimestampMs = 0
	return newE
}

func (e *Entry) ToString() string {
	return fmt.Sprintf("%v", e.Argv)
}
