package aof

import (
	"container/list"
	"github.com/alibaba/RedisShake/internal/entry"
)

type AofManifestFileType string

const (
	AofManifestFileTypeBase AofManifestFileType = "b" /* Base file */
	AofManifestTypeHist     AofManifestFileType = "h" /* History file */
	AofManifestTypeIncr     AofManifestFileType = "i" /* INCR file */
)

/* AOF manifest definition */
type aofInfo struct {
	fileName    string
	fileSeq     int64
	aofFileType AofManifestFileType
}

type aofManifest struct {
	baseAofInfo     *aofInfo
	incrAofList     *list.List
	historyList     *list.List
	currBaseFileSeq int64
	currIncrFIleSeq int64
	dirty           int64
}

// TODO: 待填充完整loader
type Loader struct {
	filPath string
	ch      chan *entry.Entry
}

func NewLoader(filPath string, ch chan *entry.Entry) *Loader {
	ld := new(Loader)
	ld.ch = ch
	ld.filPath = filPath
	return ld
}

// TODO：完成checAofMain后写单测进行测试
func (ld *Loader) ParseRDB() int {
	// 加载aof目录
	// 进行check_aof， aof
	CheckAofMain(ld.filPath)
	// TODO：执行加载
	return 0
}
