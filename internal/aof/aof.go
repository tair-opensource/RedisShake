package aof

import (
	"bufio"
	"bytes"
	"fmt"
	"io"
	"os"
	"strconv"
	"strings"
	"time"
	"unicode"

	"github.com/alibaba/RedisShake/internal/config"
	"github.com/alibaba/RedisShake/internal/entry"
	"github.com/alibaba/RedisShake/internal/log"
	"github.com/alibaba/RedisShake/internal/rdb"
	"github.com/alibaba/RedisShake/internal/statistics"
)

const (
	AofManifestFileTypeBase = "b" /* Base file */
	AofManifestTypeHist     = "h" /* History file */
	AofManifestTypeIncr     = "i" /* INCR file */
	RdbFormatSuffix         = ".rdb"
	AofFormatSuffix         = ".aof"
	BaseFileSuffix          = ".base"
	IncrFileSuffix          = ".incr"
	TempFileNamePrefix      = "temp-"
	COK                     = 1
	CERR                    = -1
	EINTR                   = 4
	ManifestNameSuffix      = ".manifest"
	AofNotExist             = 1
	AofOpenErr              = 3
	AofOK                   = 0
	AofEmpty                = 2
	AofFailed               = 4
	AofTruncated            = 5
	SizeMax                 = 128
	RdbFlagsAofPreamble     = 1 << 0
)

var rdbFileBeingLoaded string = ""

func Ustime() int64 {
	tv := time.Now()
	ust := int64(tv.UnixNano()) / 1000
	return ust

}

func StringNeedsRepr(s string) int {
	len := len(s)
	point := 0
	for len > 0 {
		if s[point] == '\\' || s[point] == '"' || s[point] == '\n' || s[point] == '\r' ||
			s[point] == '\t' || s[point] == '\a' || s[point] == '\b' || !unicode.IsPrint(rune(s[point])) || unicode.IsSpace(rune(s[point])) {
			return 1
		}
		len--
		point++
	}

	return 0
}

func DirExists(dname string) int {
	_, err := os.Stat(dname)
	if err != nil {
		return 0
	}

	return 1
}

func FileExist(filename string) int {
	_, err := os.Stat(filename)
	if err != nil {
		return 0
	}

	return 1
}

func IsHexDigit(c byte) bool {
	return (c >= '0' && c <= '9') || (c >= 'a' && c <= 'f') ||
		(c >= 'A' && c <= 'F')
}

func HexDigitToInt(c byte) int {
	switch c {
	case '0':
		return 0
	case '1':
		return 1
	case '2':
		return 2
	case '3':
		return 3
	case '4':
		return 4
	case '5':
		return 5
	case '6':
		return 6
	case '7':
		return 7
	case '8':
		return 8
	case '9':
		return 9
	case 'a', 'A':
		return 10
	case 'b', 'B':
		return 11
	case 'c', 'C':
		return 12
	case 'd', 'D':
		return 13
	case 'e', 'E':
		return 14
	case 'f', 'F':
		return 15
	default:
		return 0
	}
}

func SplitArgs(line string) ([]string, int) {
	var p string = line
	var current string
	var vector []string
	argc := 0
	i := 0
	lens := len(p)
	for { //SKIP BLANKS
		for i < lens && unicode.IsSpace((rune(p[i]))) {
			i++
		}
		if i < lens {
			inq := false  // Set to true if we are in "quotes"
			insq := false // Set to true if we are in 'single quotes'
			done := false

			for !done {
				if inq {

					if p[i] == '\\' && (p[i+1]) == 'x' && IsHexDigit(p[i+2]) && IsHexDigit(p[i+3]) {
						_, err1 := strconv.ParseInt(string(p[i+2]), 16, 64)
						_, err2 := strconv.ParseInt(string(p[i+3]), 16, 64)
						if err1 == nil && err2 == nil {
							int16 := (HexDigitToInt((p[i+2])) * 16) + HexDigitToInt(p[i+3])
							current = current + fmt.Sprint(int16)
							i += 3
						}

					} else if p[i] == '\\' && i+1 < lens {
						var c byte
						i++
						switch p[i] {
						case 'n':
							c = '\n'
						case 'r':
							c = 'r'
						case 'a':
							c = '\a'
						default:
							c = p[i]
						}
						current += string(c)
					} else if p[i] == '"' {
						if i+1 < lens && !unicode.IsSpace((rune(p[i+1]))) {
							return nil, 0
						}
						done = true
					} else if i >= lens {
						return nil, 0
					} else {
						current += string(p[i])
					}
				} else if insq {
					if p[i] == '\\' && p[i+1] == '\'' {
						i++
						current += "'"
					} else if p[i] == '\'' {
						if i+1 < lens && !unicode.IsSpace((rune(p[i+1]))) {
							return nil, 0
						}
						done = true
					} else if i >= lens {
						return nil, 0
					} else {
						current += string(p[i])
					}

				} else {
					switch p[i] {
					case ' ', '\n', '\r', '\t', '\000':
						done = true
					case '"':
						inq = true
					case '\'':
						insq = true
					default:
						current += string(p[i])
					}
				}
				if i < lens {
					i++
				}
				if i == lens {
					done = true
				}
			}

			vector = append(vector, current)
			argc++
			current = ""

		} else {
			return vector, argc
		}

	}
}

func Stringcatlen(s string, t []byte, lent int) string {
	curlen := len(s)

	if curlen == 0 {
		return ""
	}

	buf := make([]byte, curlen+lent)

	copy(buf[:curlen], []byte(s))
	copy(buf[curlen:], t)
	return string(buf)
}

func Stringcatprintf(s string, fmtStr string, args ...interface{}) string {
	result := fmt.Sprintf(fmtStr, args...)
	if s == "" {
		return result
	} else {
		return s + result
	}
}

func Stringcatrepr(s string, p string, length int) string {
	s = s + string("\"")
	for i := 0; i < length; i++ {
		switch p[i] {
		case '\\', '"':
			s = Stringcatprintf(s, "\\%c", p[i])
		case '\n':
			s = s + "\\n"
		case '\r':
			s = s + "\\r"
		case '\t':
			s = s + "\\t"
		case '\a':
			s = s + "\\a"
		case '\b':
			s = s + "\\b"
		default:
			if strconv.IsPrint(rune(p[i])) {
				s = s + string(p[i])
			} else {
				s = s + "\\x%02x"
			}
		}
	}
	return s + "\""
}

func UpdateLoadingFileName(filename string) {
	rdbFileBeingLoaded = filename
}

/* AOF manifest definition */
type aofInfo struct {
	fileName    string
	fileSeq     int64
	aofFileType string
}

func AofInfoCreate() *aofInfo {
	return new(aofInfo)
}

var Aof_Info aofInfo = *AofInfoCreate()

func (a *aofInfo) GetAofInfoName() string {
	return a.fileName
}

func AofInfoDup(orig *aofInfo) *aofInfo {
	if orig == nil {
		log.Panicf("Assertion failed: orig != nil")
	}
	ai := AofInfoCreate()
	ai.fileName = orig.fileName
	ai.fileSeq = orig.fileSeq
	ai.aofFileType = orig.aofFileType
	return ai
}

func AofInfoFormat(buf string, ai *aofInfo) string {
	var filenameRepr string
	if StringNeedsRepr(ai.fileName) == 1 {
		filenameRepr = Stringcatrepr("", ai.fileName, len(ai.fileName))
	}
	var ret string
	if filenameRepr != "" {
		ret = Stringcatprintf(buf, "%s %s %s %d %s %s\n", AofManifestKeyFileName, filenameRepr, AofManifestKeyFileSeq, ai.fileSeq, AofManifestKeyFileType, ai.aofFileType)
	} else {
		ret = Stringcatprintf(buf, "%s %s %s %d %s %s\n", AofManifestKeyFileName, ai.fileName, AofManifestKeyFileSeq, ai.fileSeq, AofManifestKeyFileType, ai.aofFileType)
	}
	return ret
}

type INFO struct {
	AofDirname         string
	AofUseRdbPreamble  int
	AofManifest        *aofManifest
	AofFilename        string
	AofCurrentSize     int64
	AofRewriteBaseSize int64
}

var AOFINFO INFO = *NewAOFINFO()

func (a *INFO) GetAofdirName() string {
	return a.AofDirname
}

func NewAOFINFO() *INFO {
	return &INFO{
		AofDirname:         config.Config.Source.AofDirName,
		AofUseRdbPreamble:  0,
		AofManifest:        nil,
		AofFilename:        config.Config.Source.AofFileName,
		AofCurrentSize:     0,
		AofRewriteBaseSize: 0,
	}
}

func (a *INFO) SetAofDirName(dirname string) {
	a.AofDirname = dirname
}

func (a *INFO) GetAofUseRdbPreamble() int {
	return a.AofUseRdbPreamble
}

func (a *INFO) SetAofUseRdbPreamble(useRdbPreamble int) {
	a.AofUseRdbPreamble = useRdbPreamble
}

func (a *INFO) GetAofManifest() *aofManifest {
	return a.AofManifest
}

func (a *INFO) SetAofManifest(manifest *aofManifest) {
	a.AofManifest = manifest
}

func (a *INFO) GetAofFilename() string {
	return a.AofFilename
}

func (a *INFO) SetAofFilename(filename string) {
	a.AofFilename = filename
}

func (a *INFO) GetAofCurrentSize() int64 {
	return a.AofCurrentSize
}

func (a *INFO) SetAofCurrentSize(size int64) {
	a.AofCurrentSize = size
}

func (a *INFO) GetAofRewriteBaseSize() int64 {
	return a.AofRewriteBaseSize
}

func (a *INFO) SetAofRewriteBaseSize(size int64) {
	a.AofRewriteBaseSize = size
}

type listIter struct {
	next      *listNode
	direction int
}

type lists struct {
	head, tail *listNode
	len        uint64
}

type listNode struct {
	prev  *listNode
	next  *listNode
	value interface{}
}

func ListCreate() *lists {
	lists := &lists{}
	lists.head = nil
	lists.tail = nil
	lists.len = 0
	return lists
}
func ListNext(iter *listIter) *listNode {
	current := iter.next

	if current != nil {
		if iter.direction == 0 {
			iter.next = current.next
		} else {
			iter.next = current.prev
		}
	}
	return current
}

func (list *lists) ListsRewind(li *listIter) {
	li.next = list.head
	li.direction = 0
}

func ListLinkNodeTail(lists *lists, node *listNode) {
	if lists.len == 0 {
		lists.head = node
		lists.tail = node
		node.prev = nil
		node.next = nil
	} else {
		node.prev = lists.tail
		node.next = nil
		lists.tail.next = node
		lists.tail = node
	}
	lists.len++
}

func ListAddNodeTail(lists *lists, value interface{}) *lists {
	node := &listNode{
		value: value,
		prev:  nil,
		next:  nil,
	}
	ListLinkNodeTail(lists, node)
	return lists
}

func ListsRewindTail(list *lists, li *listIter) {
	li.next = list.tail
	li.direction = 1
}

func ListDup(orig *lists) *lists {
	var copy *lists
	var iter listIter
	var node *listNode
	copy = ListCreate()
	if copy == nil {
		return nil
	}
	copy.ListsRewind(&iter)
	node = ListNext(&iter)
	var value interface{}
	for node != nil {
		value = node.value
	}

	if ListAddNodeTail(copy, value) == nil {
		return nil
	}
	return copy
}

func ListIndex(list *lists, index int64) *listNode {
	var n *listNode

	if index < 0 {
		index = (-index) - 1
		n = list.tail
		for ; index > 0 && n != nil; index-- {
			n = n.prev
		}
	} else {
		n = list.head
		for ; index > 0 && n != nil; index-- {
			n = n.next
		}
	}
	return n
}

func ListLinkNodeHead(list *lists, node *listNode) {
	if list.len == 0 {
		list.head = node
		list.tail = node
		node.prev = nil
		node.next = nil
	} else {
		node.prev = nil
		node.next = list.head
		list.head.prev = node
		list.head = node
	}
	list.len++
}

func ListAddNodeHead(list *lists, value interface{}) *lists {
	node := &listNode{
		value: value,
	}
	ListLinkNodeHead(list, node)

	return list
}

func ListUnlinkNode(list *lists, node *listNode) {
	if node.prev != nil {
		node.prev.next = node.next
	} else {
		list.head = node.next
	}
	if node.next != nil {
		node.next.prev = node.prev
	} else {
		list.tail = node.prev
	}
	node.next = nil
	node.prev = nil

	list.len--
}
func ListDelNode(list *lists, node *listNode) {
	ListUnlinkNode(list, node)

}

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

type aofManifest struct {
	baseAofInfo     *aofInfo
	incrAofList     *lists
	historyList     *lists
	currBaseFileSeq int64
	currIncrFIleSeq int64
	dirty           int64
}

func AofManifestcreate() *aofManifest {
	am := &aofManifest{
		incrAofList: ListCreate(),
		historyList: ListCreate(),
	}
	return am
}

func AOFManifestDup(orig *aofManifest) *aofManifest {
	if orig == nil {
		panic("orig is nil")
	}

	am := &aofManifest{
		currBaseFileSeq: orig.currBaseFileSeq,
		currIncrFIleSeq: orig.currIncrFIleSeq,
		dirty:           orig.dirty,
	}

	if orig.baseAofInfo != nil {
		am.baseAofInfo = AofInfoDup(orig.baseAofInfo)
	}

	am.incrAofList = ListDup(orig.incrAofList)
	am.historyList = ListDup(orig.historyList)

	if am.incrAofList == nil || am.historyList == nil {
		log.Panicf("IncrAOFlist or HistoryAOFlist is nil")
	}
	return am
}

func GetAofManifestAsString(am *aofManifest) string {
	if am == nil {
		panic("am is nil")
	}
	var buf string
	var ln *listNode
	var li listIter

	if am.baseAofInfo != nil {
		buf = AofInfoFormat(buf, am.baseAofInfo)
	}
	am.historyList.ListsRewind(&li)
	ln = ListNext(&li)
	for ln != nil {
		ai, ok := ln.value.(*aofInfo)
		if ok {
			buf = AofInfoFormat(buf, ai)
		}
		ln = ListNext(&li)
	}

	am.incrAofList.ListsRewind(&li)
	ln = ListNext(&li)
	for ln != nil {
		ai, ok := ln.value.(*aofInfo)
		if ok {
			buf = AofInfoFormat(buf, ai)
		}
		ln = ListNext(&li)
	}

	return buf

}

func GetNewBaseFileNameAndMarkPreAsHistory(am *aofManifest) string {
	if am == nil {
		log.Panicf("aofManifest is nil")
	}
	if am.baseAofInfo != nil {
		if am.baseAofInfo.aofFileType != AofManifestFileTypeBase {
			log.Panicf("base_aof_info has invalid file_type")
		}
		am.baseAofInfo.aofFileType = AofManifestTypeHist
	}
	var formatSuffix string
	if AOFINFO.AofUseRdbPreamble == 1 {
		formatSuffix = RdbFormatSuffix
	} else {
		formatSuffix = AofFormatSuffix
	}
	ai := AofInfoCreate()
	ai.fileName = Stringcatprintf("%s.%d%s%d", Aof_Info.GetAofInfoName(), am.currBaseFileSeq+1, BaseFileSuffix, formatSuffix)
	ai.fileSeq = am.currBaseFileSeq + 1
	ai.aofFileType = AofManifestFileTypeBase
	am.baseAofInfo = ai
	am.dirty = 1
	return am.baseAofInfo.fileName
}

func AofLoadManifestFromDisk() {
	AOFINFO.AofManifest = AofManifestcreate()
	if DirExists(AOFINFO.AofDirname) == 0 {
		log.Infof("The AOF directory %v doesn't exist\n", AOFINFO.AofDirname)
		return
	}

	am_name := GetAofManifestFileName()
	am_filepath := MakePath(AOFINFO.AofDirname, am_name)
	if FileExist(am_filepath) == 0 {
		log.Infof("The AOF directory %v doesn't exist\n", AOFINFO.AofDirname)
		return
	}

	am := AofLoadManifestFromFile(am_filepath)
	if am != nil {
		AOFINFO.AofManifest = am
	}

}

func GetNewIncrAofName(am *aofManifest) string {
	ai := AofInfoCreate()
	ai.aofFileType = AofManifestTypeIncr
	ai.fileName = Stringcatprintf("", "%s.%d%s%s", AOFINFO.AofFilename, am.currIncrFIleSeq+1, IncrFileSuffix, AofFormatSuffix)
	ai.fileSeq = am.currIncrFIleSeq + 1
	ListAddNodeTail(am.incrAofList, ai)
	am.dirty = 1
	return ai.fileName
}

func GetTempIncrAofNanme() string {
	return Stringcatprintf("", "%s%s%s", TempFileNamePrefix, AOFINFO.AofFilename, IncrFileSuffix)
}

func GetLastIncrAofName(am *aofManifest) string {
	if am == nil {
		log.Panicf(("aofManifest is nil"))
	}

	if am.incrAofList.len == 0 {
		return GetNewIncrAofName(am)
	}

	lastnode := ListIndex(am.incrAofList, -1)

	ai, ok := lastnode.value.(aofInfo)
	if !ok {
		fmt.Printf("Failed to convert lastnode.value to aofInfo")
		log.Panicf("Failed to convert lastnode.value to aofInfo")
	}
	return ai.fileName
}

func GetAofManifestFileName() string {
	return Stringcatprintf("", "%s%s", AOFINFO.AofFilename, ManifestNameSuffix)
}

func GetTempAofManifestFileName() string {
	return Stringcatprintf("", "%s%s%s", TempFileNamePrefix, AOFINFO.AofFilename, ManifestNameSuffix)
}

func StartLoading(size int64, rdbflags int, async int) {
	/* Load the DB */
	statistics.Metrics.Loading = true
	if async == 1 {
		statistics.Metrics.AsyncLoading = true
	}
	statistics.Metrics.LoadingStartTime = time.Now().Unix()
	statistics.Metrics.LoadingLoadedBytes = 0
	statistics.Metrics.LoadingTotalBytes = size
	log.Infof("The AOF file starts loading.\n")
}
func StopLoading(ret int) {
	statistics.Metrics.Loading = false
	statistics.Metrics.AsyncLoading = false
	if ret == AofOK || ret == AofTruncated {
		log.Infof("The aof file was successfully loaded\n")
	} else {
		log.Infof("There was an error opening the AOF file.\n")
	}
}

func AofFileExist(filename string) int {
	filepath := MakePath(AOFINFO.AofDirname, filename)
	ret := FileExist(filepath)
	return ret
}

func GetAppendOnlyFileSize(filename string, status *int) int64 {
	var size int64

	aofFilepath := MakePath(AOFINFO.AofDirname, filename)

	stat, err := os.Stat(aofFilepath)
	if err != nil {
		if status != nil {
			if os.IsNotExist(err) {
				*status = AofNotExist
			} else {
				*status = AofOpenErr
			}
		}
		log.Panicf("Unable to obtain the AOF file %v length. stat: %v", filename, err.Error())
		size = 0
	} else {
		if status != nil {
			*status = AofOK
		}
		size = stat.Size()
	}
	return size
}

func GetBaseAndIncrAppendOnlyFilesSize(am *aofManifest, status *int) int64 {
	var size int64
	var ln *listNode = new(listNode)
	var li *listIter = new(listIter)
	if am.baseAofInfo != nil {
		if am.baseAofInfo.aofFileType != AofManifestFileTypeBase {
			log.Panicf("File type must be base.")
		}
		size += GetAppendOnlyFileSize(am.baseAofInfo.fileName, status)
		if *status != AofOK {
			return 0
		}
	}

	am.incrAofList.ListsRewind(li)
	ln = ListNext(li)
	for ln != nil {
		ai := ln.value.(*aofInfo)
		if ai.aofFileType != AofManifestTypeIncr {
			log.Panicf("File type must be Incr")
		}
		size += GetAppendOnlyFileSize(ai.fileName, status)
		if *status != AofOK {
			return 0
		}
		ln = ListNext(li)
	}
	return size
}

func GetBaseAndIncrAppendOnlyFilesNum(am *aofManifest) int {
	num := 0
	if am.baseAofInfo != nil {
		num++
	}
	if am.incrAofList != nil {
		num += int(am.incrAofList.len)
	}
	return num
}

func (ld *Loader) LoadSingleAppendOnlyFile(filename string, ch chan *entry.Entry) int {
	ret := AofOK
	AofFilepath := MakePath(AOFINFO.AofDirname, filename)
	var sizes int64 = 0
	fp, err := os.Open(AofFilepath)
	if err != nil {
		if os.IsNotExist(err) {
			if _, err := os.Stat(AofFilepath); err == nil || !os.IsNotExist(err) {
				log.Infof("Fatal error: can't open the append log file %v for reading: %v", filename, err.Error())
				return AofOpenErr
			} else {
				log.Infof("The append log file %v doesn't exist: %v", filename, err.Error())
				return AofNotExist
			}

		}
		defer fp.Close()

		stat, _ := fp.Stat()
		if stat.Size() == 0 {
			return AofEmpty
		}
	}
	sig := make([]byte, 5)
	if n, err := fp.Read(sig); err != nil || n != 5 || !bytes.Equal(sig, []byte("REDIS")) {
		if _, err := fp.Seek(0, 0); err != nil {
			log.Infof("Unrecoverable error reading the append only file %v: %v", filename, err)
			ret = AofFailed
			return ret
		}
	} else {
		log.Infof("Reading RDB base file on AOF loading...")
		ldRDB := rdb.NewLoader(AofFilepath, ch)
		ldRDB.ParseRDB()
		return AofOK
		//Skipped RDB checksum and has not been processed yet.
	}
	sizes += 5
	reader := bufio.NewReader(fp)
	for {

		line, err := reader.ReadBytes('\n')
		{
			if err != nil {
				if err == io.EOF {
					break
				}
			} else {
				_, errs := fp.Seek(0, io.SeekCurrent)
				if errs != nil {
					log.Infof("Unrecoverable error reading the append only file %v: %v", filename, err)
					ret = AofFailed
					return ret
				}
			}
			sizes += int64(len(line))

			if line[0] == '#' {
				continue
			}
			if line[0] != '*' {
				log.Infof("Bad file format reading the append only file %v:make a backup of your AOF file, then use ./redis-check-aof --fix <filename.manifest>", filename)
			}
			argc, _ := strconv.ParseInt(string(line[1:len(line)-2]), 10, 64)
			if argc < 1 {
				log.Infof("Bad file format reading the append only file %v:make a backup of your AOF file, then use ./redis-check-aof --fix <filename.manifest>", filename)
			}
			if argc > int64(SizeMax) {
				log.Infof("Bad file format reading the append only file %v:make a backup of your AOF file, then use ./redis-check-aof --fix <filename.manifest>", filename)
			}
			e := entry.NewEntry()
			argv := []string{}

			for j := 0; j < int(argc); j++ {
				line, err := reader.ReadString('\n')
				if err != nil || line[0] != '$' {
					if err == io.EOF {
						log.Infof("Unrecoverable error reading the append only file %v: %v", filename, err)
						ret = AofFailed
						return ret
					} else {
						log.Infof("Bad file format reading the append only file %v:make a backup of your AOF file, then use ./redis-check-aof --fix <filename.manifest>", filename)
					}
				}
				sizes += int64(len(line))
				len, _ := strconv.ParseInt(string(line[1:len(line)-2]), 10, 64)

				argstring := make([]byte, len)
				_, err = reader.Read(argstring)
				if err != nil {
					log.Infof("Unrecoverable error reading the append only file %v: %v", filename, err)
					ret = AofFailed
					return ret
				}
				argv = append(argv, string(argstring))
				CRLF := make([]byte, 2)
				_, err = reader.Read(CRLF)
				if err != nil {
					log.Infof("Unrecoverable error reading the append only file %v: %v", filename, err)
					ret = AofFailed
					return ret
				}
				sizes += len + 2
			}
			for _, value := range argv {
				e.Argv = append(e.Argv, value)
			}
			ld.ch <- e

		}

	}
	statistics.Metrics.LoadingLoadedBytes = sizes
	return ret
}

func (ld *Loader) LoadAppendOnlyFile(am *aofManifest, ch chan *entry.Entry) int {
	if am == nil {
		log.Panicf("aofManifest is null")
	}
	status := AofOK
	ret := AofOK
	var start int64
	var totalSize int64 = 0
	var baseSize int64 = 0
	var aofName string
	var totalNum, aofNum, lastFile int

	if AofFileExist(AOFINFO.AofFilename) == 1 {
		if DirExists(AOFINFO.AofDirname) == 0 ||
			(am.baseAofInfo == nil && am.incrAofList.len == 0) ||
			(am.baseAofInfo != nil && am.incrAofList.len == 0 &&
				strings.Compare(am.baseAofInfo.fileName, AOFINFO.AofFilename) == 0 && AofFileExist(AOFINFO.AofFilename) == 0) {
			log.Panicf("This is an old version of the AOF file")
		}
	}

	if am.baseAofInfo == nil && am.incrAofList == nil {
		return AofNotExist
	}

	totalNum = GetBaseAndIncrAppendOnlyFilesNum(am)
	if totalNum <= 0 {
		log.Panicf("Assertion failed: IncrAppendOnlyFilestotalNum > 0")
	}

	totalSize = GetBaseAndIncrAppendOnlyFilesSize(am, &status)
	if status != AofOK {
		if status == AofNotExist {
			status = AofFailed
		}
		return status
	} else if totalSize == 0 {
		return AofEmpty
	}

	StartLoading(totalSize, RdbFlagsAofPreamble, 0)
	if am.baseAofInfo != nil {
		if am.baseAofInfo.aofFileType == AofManifestFileTypeBase {
			aofName = string(am.baseAofInfo.fileName)
			UpdateLoadingFileName(aofName)
			baseSize = GetAppendOnlyFileSize(aofName, nil)
			lastFile = totalNum
			start = Ustime()
			ret = ld.LoadSingleAppendOnlyFile(aofName, ch)
			if ret == AofOK || (ret == AofTruncated && lastFile == 1) {
				log.Infof("DB loaded from base file %v: %.3f seconds", aofName, float64(Ustime()-start)/1000000)
			}

			if ret == AofEmpty {
				ret = AofOK
			}

			if ret == AofTruncated && lastFile == 0 {
				ret = AofFailed
				log.Infof("Fatal error: the truncated file is not the last file")
			}

			if ret == AofOpenErr || ret == AofFailed {
				if ret == AofOK || ret == AofTruncated {
					log.Infof("The aof file was successfully loaded\n")
				} else {
					if ret == AofOpenErr {
						log.Infof("There was an error opening the AOF file.\n")
					} else {
						log.Infof("Failed to open AOF file.\n")
					}
				}
				return ret
			}
		}
	}

	if am.incrAofList.len > 0 {
		var ln *listNode = new(listNode)
		var li listIter

		am.incrAofList.ListsRewind(&li)
		ln = ListNext(&li)
		for ln != nil {
			ai := ln.value.(*aofInfo)
			if ai.aofFileType != AofManifestTypeIncr {
				log.Panicf("The manifestType must be Incr")
			}
			aofName = ai.fileName
			UpdateLoadingFileName(aofName)
			lastFile = totalNum
			aofNum++
			start = Ustime()
			ret = ld.LoadSingleAppendOnlyFile(aofName, ch)
			if ret == AofOK || (ret == AofTruncated && lastFile == 1) {
				log.Infof("DB loaded from incr file %v: %.3f seconds", aofName, float64(Ustime()-start)/1000000)
			}

			if ret == AofEmpty {
				ret = AofOK
			}

			if ret == AofTruncated && lastFile == 0 {
				ret = AofFailed
				log.Infof("Fatal error: the truncated file is not the last file\n")
			}

			if ret == AofOpenErr || ret == AofFailed {
				if ret == AofOpenErr {
					log.Infof("There was an error opening the AOF file.\n")
				} else {
					log.Infof("Failed to open AOF file.\n")
				}
				return ret
			}
			ln = ListNext(&li)
		}

	}

	AOFINFO.AofCurrentSize = totalSize
	AOFINFO.AofRewriteBaseSize = baseSize
	return ret

}
