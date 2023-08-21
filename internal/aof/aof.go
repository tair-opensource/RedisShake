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

	"github.com/alibaba/RedisShake/internal/commands"
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
	RDB_FORMAT_SUFFIX       = ".rdb"
	AOF_FORMAT_SUFFIX       = ".aof"
	BASE_FILE_SUFFIX        = ".base"
	INCR_FILE_SUFFIX        = ".incr"
	TEMP_FILE_NAME_PREFIX   = "temp-"
	C_OK                    = 1
	C_ERR                   = -1
	EINTR                   = 4
	MANIFEST_NAME_SUFFIX    = ".manifest"
	AOF_NOT_EXIST           = 1
	AOF_OPEN_ERR            = 3
	AOF_OK                  = 0
	AOF_EMPTY               = 2
	AOF_FAILED              = 4
	AOF_TRUNCATED           = 5
	SIZE_MAX                = 128
	RDBFLAGS_AOF_PREAMBLE   = 1 << 0
)

var rdbFileBeingLoaded string = ""

func UpdateLoadingFileName(filename string) {
	rdbFileBeingLoaded = filename
}

/* AOF manifest definition */
type aofInfo struct {
	fileName    string
	fileSeq     int64
	aofFileType string
}

type INFO struct {
	aof_dirname           string
	aofUseRdbPreamble     int
	aof_manifest          *aofManifest
	aof_filename          string
	aof_current_size      int64
	aof_rewrite_base_size int64
}

var AOFINFO INFO = *NewAOFINFO()

func (a *INFO) GetAofdirName() string {
	return a.aof_dirname
}

func (a *INFO) SetAofDirName(dirname string) {
	a.aof_dirname = dirname
}

func (a *INFO) GetAofUseRdbPreamble() int {
	return a.aofUseRdbPreamble
}

func (a *INFO) SetAofUseRdbPreamble(useRdbPreamble int) {
	a.aofUseRdbPreamble = useRdbPreamble
}

func (a *INFO) GetAofManifest() *aofManifest {
	return a.aof_manifest
}

func (a *INFO) SetAofManifest(manifest *aofManifest) {
	a.aof_manifest = manifest
}

func (a *INFO) GetAofFilename() string {
	return a.aof_filename
}

func (a *INFO) SetAofFilename(filename string) {
	a.aof_filename = filename
}

func (a *INFO) GetAofCurrentSize() int64 {
	return a.aof_current_size
}

func (a *INFO) SetAofCurrentSize(size int64) {
	a.aof_current_size = size
}

func (a *INFO) GetAofRewriteBaseSize() int64 {
	return a.aof_rewrite_base_size
}

func (a *INFO) SetAofRewriteBaseSize(size int64) {
	a.aof_rewrite_base_size = size
}
func NewAOFINFO() *INFO {
	return &INFO{
		aof_dirname:           config.Config.Source.AofDirName,
		aofUseRdbPreamble:     0,
		aof_manifest:          nil,
		aof_filename:          config.Config.Source.AofFileName,
		aof_current_size:      0,
		aof_rewrite_base_size: 0,
	}
}

func Ustime() int64 {
	tv := time.Now()
	ust := int64(tv.UnixNano()) / 1000
	return ust

}

func AofInfoCreate() *aofInfo {
	return new(aofInfo)
}

var Aof_Info aofInfo = *AofInfoCreate()

func (a *aofInfo) GetAofInfoName() string {
	return a.fileName
}

// test ok
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

// test ok
func DirExists(dname string) int {
	_, err := os.Stat(dname)
	if err != nil {
		return 0
	}

	return 1
}

// test ok
func FileExist(filename string) int {
	_, err := os.Stat(filename)
	if err != nil {
		return 0
	}

	return 1
}

// test ok
func IsHexDigit(c byte) bool {
	return (c >= '0' && c <= '9') || (c >= 'a' && c <= 'f') ||
		(c >= 'A' && c <= 'F')
}

/*如果字符串中包含要转义的字符，则返回一
 *通过sdscatrer（），否则为零。
 *
 *通常，这应该用于以某种方式帮助保护聚合字符串
 *与sdsspliargs（）兼容。因此，空间也将
 *被视为需要逃跑。
 */
/*func stringTrim(s string,cset string)string{
	slen:=len(s)
	sp:=0
	ep:=slen-1

	for sp<=ep&&strings.ContainsRune(cset,rune(s[sp])){
		sp++
	}

	for ep>sp&&strings.ContainsRune(cset,rune(s[sp])){
		ep--
	}
	trimmed:=s[sp:ep+1]
	trimmedlen:=len(trimmed)
	if sp<0||ep<slen-1{
		//connvert string to []byte to modify in-place
		b:=*(*[]byte)(unsafe.pointer(&trimmed))
		copy(b,b[:trimmedlen])
		//b[trimmedlen] = 0
		return string(b)
	}
	return s
}*/
//test ok
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

// testok
func SplitArgs(line string) ([]string, int) {
	var p string
	p = line
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
			// Get a token
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
							//var bytes []byte
							//bytes=IntToBytes(int16)
							//current = stringcatlen(current,bytes,1)
							current = current + fmt.Sprint(int16)
							i += 3
						}

					} else if p[i] == '\\' && i+1 < lens {
						c := p[i]
						i++
						switch p[i] {
						case 'n':
							c = '\n'
							break
						case 'r':
							c = 'r'
							break
						case 'a':
							c = '\a'
							break
						default:
							c = p[i]
							break
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
						break
					case '"':
						inq = true
						break
					case '\'':
						insq = true
						break
					default:
						current += string(p[i])
						break
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

// test ok
func Stringcatlen(s string, t []byte, lent int) string {
	curlen := len(s)

	if curlen == 0 {
		return ""
	}

	buf := make([]byte, curlen+lent)

	copy(buf[:curlen], []byte(s))
	copy(buf[curlen:], t)
	//buf[curlen+lent] = 0 ///0
	return string(buf)
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

type aofManifest struct {
	baseAofInfo     *aofInfo
	incrAofList     *lists
	historyList     *lists
	currBaseFileSeq int64
	currIncrFIleSeq int64
	dirty           int64
}

// TODO: 待填充完整loader
type Loader struct {
	filPath string
	ch      chan *entry.Entry
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
func NewLoader(filPath string, ch chan *entry.Entry) *Loader {
	ld := new(Loader)
	ld.ch = ch
	ld.filPath = filPath
	return ld
}

// testok
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

func AofInfoFormat(buf string, ai *aofInfo) string {
	var filenameRepr string
	if StringNeedsRepr(ai.fileName) == 1 {
		filenameRepr = Stringcatrepr("", ai.fileName, len(ai.fileName))
	}
	var ret string
	if filenameRepr != "" {
		ret = Stringcatprintf(buf, "%s %s %s %d %s %s\n", AOF_MANIFEST_KEY_FILE_NAME, filenameRepr, AOF_MANIFEST_KEY_FILE_SEQ, ai.fileSeq, AOF_MANIFEST_KEY_FILE_TYPE, ai.aofFileType)
	} else {
		ret = Stringcatprintf(buf, "%s %s %s %d %s %s\n", AOF_MANIFEST_KEY_FILE_NAME, ai.fileName, AOF_MANIFEST_KEY_FILE_SEQ, ai.fileSeq, AOF_MANIFEST_KEY_FILE_TYPE, ai.aofFileType)
	}
	return ret
}

func AofManifestcreate() *aofManifest {
	am := &aofManifest{
		incrAofList: ListCreate(),
		historyList: ListCreate(),
	}
	return am
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
func ListsRewindTail(list *lists, li *listIter) {
	li.next = list.tail
	li.direction = 1
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
		fmt.Printf("IncrAOFlist or HistoryAOFlist is nil")
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
	if AOFINFO.aofUseRdbPreamble == 1 {
		formatSuffix = RDB_FORMAT_SUFFIX
	} else {
		formatSuffix = AOF_FORMAT_SUFFIX
	}
	ai := AofInfoCreate()
	ai.fileName = Stringcatprintf("%s.%d%s%d", Aof_Info.GetAofInfoName(), am.currBaseFileSeq+1, BASE_FILE_SUFFIX, formatSuffix)
	ai.fileSeq = am.currBaseFileSeq + 1
	ai.aofFileType = AofManifestFileTypeBase
	am.baseAofInfo = ai
	am.dirty = 1
	return am.baseAofInfo.fileName
}

// server 未处理 testok
func AofLoadManifestFromDisk() {
	AOFINFO.aof_manifest = AofManifestcreate()
	if DirExists(AOFINFO.aof_dirname) == 0 {
		log.Infof("The AOF directory %v doesn't exist\n", AOFINFO.aof_dirname)
		return
	}

	am_name := GetAofManifestFileName()
	am_filepath := MakePath(AOFINFO.aof_dirname, am_name)
	if FileExist(am_filepath) == 0 {
		log.Infof("The AOF directory %v doesn't exist\n", AOFINFO.aof_dirname)
		return
	}

	am := AofLoadManifestFromFile(am_filepath)
	if am != nil {
		AOFINFO.aof_manifest = am
	}

}

func GetNewIncrAofName(am *aofManifest) string {
	ai := AofInfoCreate()
	ai.aofFileType = AofManifestTypeIncr
	ai.fileName = Stringcatprintf("", "%s.%d%s%s", AOFINFO.aof_filename, am.currIncrFIleSeq+1, INCR_FILE_SUFFIX, AOF_FORMAT_SUFFIX)
	ai.fileSeq = am.currIncrFIleSeq + 1
	ListAddNodeTail(am.incrAofList, ai)
	am.dirty = 1
	return ai.fileName
}

func GetTempIncrAofNanme() string {
	return Stringcatprintf("", "%s%s%s", TEMP_FILE_NAME_PREFIX, AOFINFO.aof_filename, INCR_FILE_SUFFIX)
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
	return Stringcatprintf("", "%s%s", AOFINFO.aof_filename, MANIFEST_NAME_SUFFIX)
}

func GetTempAofManifestFileName() string {
	return Stringcatprintf("", "%s%s%s", TEMP_FILE_NAME_PREFIX, AOFINFO.aof_filename, MANIFEST_NAME_SUFFIX)
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
	fmt.Printf("The AOF file starts loading.\n")
	log.Infof("The AOF file starts loading.\n")
}
func StopLoading(ret int) {
	statistics.Metrics.Loading = false
	statistics.Metrics.AsyncLoading = false
	if ret == AOF_OK || ret == AOF_TRUNCATED {
		fmt.Printf("The aof file was successfully loaded\n")
		log.Infof("The aof file was successfully loaded\n")
	} else {
		fmt.Printf("There was an error opening the AOF file.\n")
		log.Infof("There was an error opening the AOF file.\n")
	}
}

// test ok
func (ld *Loader) LoadSingleAppendOnlyFile(filename string, ch chan *entry.Entry) int {
	//没释放命令
	loops := 0
	ret := AOF_OK
	AofFilepath := MakePath(AOFINFO.aof_dirname, filename)
	var sizes int64 = 0
	fp, err := os.Open(AofFilepath)
	if err != nil {
		if os.IsNotExist(err) {
			if _, err := os.Stat(AofFilepath); err == nil || !os.IsNotExist(err) {
				fmt.Printf("Fatal error: can't open the append log file %v for reading: %v", filename, err.Error())
				log.Infof("Fatal error: can't open the append log file %v for reading: %v", filename, err.Error())
				return AOF_OPEN_ERR
			} else {
				fmt.Printf("The append log file %v doesn't exist: %v", filename, err.Error())
				log.Infof("The append log file %v doesn't exist: %v", filename, err.Error())
				return AOF_NOT_EXIST
			}

		}
		defer fp.Close()

		stat, _ := fp.Stat()
		if stat.Size() == 0 {
			return AOF_EMPTY
		}
	}
	sig := make([]byte, 5)
	if n, err := fp.Read(sig); err != nil || n != 5 || !bytes.Equal(sig, []byte("REDIS")) {
		if _, err := fp.Seek(0, 0); err != nil {
			fmt.Printf("Unrecoverable error reading the append only file %v: %v", filename, err)
			log.Infof("Unrecoverable error reading the append only file %v: %v", filename, err)
			ret = AOF_FAILED
			return ret
		}
	} else {
		fmt.Printf("Reading RDB base file on AOF loading...")
		log.Infof("Reading RDB base file on AOF loading...")
		ldRDB := rdb.NewLoader(AofFilepath, ch)
		ldRDB.ParseRDB()

		//RDB
	}
	sizes += 5
	reader := bufio.NewReader(fp)
	for { //serve
		if loops%1024 == 0 {
			//一些与事件处理和模块加载进度相关的操作，具体实现可能涉及更多的代码。例如，processEventsWhileBlocked 可能是处理在阻塞期间积累的事件的函数调用，而 processModuleLoadingProgressEvent 则是处理模块加载进度事件的函数调用。
		}

		line, err := reader.ReadString('\n')
		{
			if err != nil {
				if err == io.EOF {
					break
				}
			} else {
				_, errs := fp.Seek(0, os.SEEK_CUR)
				if errs == nil {
					fmt.Printf("Unrecoverable error reading the append only file %v: %v", filename, err)
					log.Infof("Unrecoverable error reading the append only file %v: %v", filename, err)
					ret = AOF_FAILED
					return ret
				}
			}
			sizes += int64(len(line))
			if line[0] == '#' {
				continue
			}
			if line[0] != '*' {
				fmt.Printf("825")
				log.Infof("Bad file format reading the append only file %v:make a backup of your AOF file, then use ./redis-check-aof --fix <filename.manifest>", filename)
			}
			argc, _ := strconv.Atoi(string(line[1:]))
			if argc < 1 {
				fmt.Printf("830")
				log.Infof("Bad file format reading the append only file %v:make a backup of your AOF file, then use ./redis-check-aof --fix <filename.manifest>", filename)
			}
			if argc > int(SIZE_MAX) {
				fmt.Printf("834")
				log.Infof("Bad file format reading the append only file %v:make a backup of your AOF file, then use ./redis-check-aof --fix <filename.manifest>", filename)
			}
			e := entry.NewEntry()
			argv := []string{}

			for j := 0; j < argc; j++ {
				line, err := reader.ReadString('\n')
				if err != nil || line[0] != '$' {
					if err == io.EOF {
						fmt.Printf("Unrecoverable error reading the append only file %v: %v", filename, err)
						log.Infof("Unrecoverable error reading the append only file %v: %v", filename, err)
						ret = AOF_FAILED
						return ret
					} else {
						fmt.Printf("849")
						log.Infof("Bad file format reading the append only file %v:make a backup of your AOF file, then use ./redis-check-aof --fix <filename.manifest>", filename)
					}
				}
				sizes += int64(len(line))
				len, _ := strconv.ParseInt(line[1:], 10, 64)

				argstring := make([]byte, len)
				_, err = reader.Read(argstring)
				if err != nil {
					fmt.Printf("Unrecoverable error reading the append only file %v: %v", filename, err)
					log.Infof("Unrecoverable error reading the append only file %v: %v", filename, err)
					ret = AOF_FAILED
					return ret
				}
				//argv[j] = createObject(OBJ_STRING, argsds) //这里没写
				argv = append(argv, string(argstring))
				CRLF := make([]byte, 2)
				_, err = reader.Read(CRLF)
				if err != nil {
					//fargc = j + 1 // Free up to j.
					fmt.Printf("Unrecoverable error reading the append only file %v: %v", filename, err)
					log.Infof("Unrecoverable error reading the append only file %v: %v", filename, err)
					ret = AOF_FAILED
					return ret
				}
				sizes += len + 2
			}
			for _, value := range argv {
				ok := commands.LookupCommand(value) //包未导出 而且键值对判定问题
				if ok == 0 {
					fmt.Printf("unknown command. argv=%v", argv)
					log.Infof("unknown command. argv=%v", argv)
					ret = AOF_FAILED
					return ret
				}
			}
			for _, value := range argv {
				e.Argv = append(e.Argv, value)
			}
			ld.ch <- e
			/*rw := writer.NewRedisWriter(config.Config.Source.Address, config.Config.Source.Username, config.Config.Source.Password, config.Config.Target.IsTLS)
			rw.Write(e) //是否go携程*/

		}

	}
	statistics.Metrics.LoadingLoadedBytes = sizes
	return ret
}

// test ok
func AofFileExist(filename string) int {
	filepath := MakePath(AOFINFO.aof_dirname, filename)
	ret := FileExist(filepath)
	return ret
}

// 这里的time没写完· testok
func GetAppendOnlyFileSize(filename string, status *int) int64 {
	var size int64

	aofFilepath := MakePath(AOFINFO.aof_dirname, filename)
	//start := time.Now()

	stat, err := os.Stat(aofFilepath)
	if err != nil {
		if status != nil {
			if os.IsNotExist(err) {
				*status = AOF_NOT_EXIST
			} else {
				*status = AOF_OPEN_ERR
			}
		}
		fmt.Printf("Unable to obtain the AOF file %v length. stat: %v", filename, err.Error())
		log.Panicf("Unable to obtain the AOF file %v length. stat: %v", filename, err.Error())
		size = 0
	} else {
		if status != nil {
			*status = AOF_OK
		}
		size = stat.Size()
	}

	//latency := time.Since(start).Milliseconds()
	//latencyAddSampleIfNeeded("aof-fstat", latency) //延迟监控
	/*可以看到，条件部分包括两个判断：首先检查 server.latency_monitor_threshold 是否为非零值（即已配置阈值），
		然后判断 (var) 是否大于等于 server.latency_monitor_threshold。只有当这两个条件都为真时，才会调用 latencyAddSample 函数。

	这段代码的目的是确保只有当给定的 var 值超过了配置的阈值时，才会将样本添加到延迟监控中。*/

	return size
}

// testok
func GetBaseAndIncrAppendOnlyFilesSize(am *aofManifest, status *int) int64 {
	var size int64
	var ln *listNode = new(listNode)
	var li *listIter = new(listIter)
	if am.baseAofInfo != nil {
		if am.baseAofInfo.aofFileType != AofManifestFileTypeBase {
			fmt.Printf("File type must be base.")
			log.Panicf("File type must be base.")
		}
		size += GetAppendOnlyFileSize(am.baseAofInfo.fileName, status)
		if *status != AOF_OK {
			return 0
		}
	}

	am.incrAofList.ListsRewind(li)
	ln = ListNext(li)
	for ln != nil {
		ai := ln.value.(*aofInfo)
		if ai.aofFileType != AofManifestTypeIncr {
			fmt.Printf("File type must be Incr")
			log.Panicf("File type must be Incr")
		}
		size += GetAppendOnlyFileSize(ai.fileName, status)
		if *status != AOF_OK {
			return 0
		}
		ln = ListNext(li)
	}
	return size
}

// test ok
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

func (ld *Loader) LoadAppendOnlyFile(am *aofManifest, ch chan *entry.Entry) int {
	if am == nil {
		fmt.Printf("aofManifest is null")
		log.Panicf("aofManifest is null")
	}
	status := AOF_OK
	ret := AOF_OK
	var start int64
	var totalSize int64 = 0
	var baseSize int64 = 0
	var aofName string
	var totalNum, aofNum, lastFile int

	if AofFileExist(AOFINFO.aof_filename) == 1 {
		if DirExists(AOFINFO.aof_dirname) == 0 ||
			(am.baseAofInfo == nil && am.incrAofList.len == 0) ||
			(am.baseAofInfo != nil && am.incrAofList.len == 0 &&
				strings.Compare(am.baseAofInfo.fileName, AOFINFO.aof_filename) == 0 && AofFileExist(AOFINFO.aof_filename) == 0) {
			fmt.Printf("This is an old version of the AOF file") //原本这里是要升级
			log.Panicf("This is an old version of the AOF file") //原本这里是要升级
		}
	}

	if am.baseAofInfo == nil && am.incrAofList == nil {
		return AOF_NOT_EXIST
	}

	totalNum = GetBaseAndIncrAppendOnlyFilesNum(am)
	if totalNum <= 0 {
		fmt.Printf("Assertion failed: IncrAppendOnlyFilestotalNum > 0")
		log.Panicf("Assertion failed: IncrAppendOnlyFilestotalNum > 0")
	}

	totalSize = GetBaseAndIncrAppendOnlyFilesSize(am, &status)
	if status != AOF_OK {
		if status == AOF_NOT_EXIST {
			status = AOF_FAILED
		}
		return status
	} else if totalSize == 0 {
		return AOF_EMPTY
	}

	StartLoading(totalSize, RDBFLAGS_AOF_PREAMBLE, 0) //这个嗲放有问题
	//这段代码是一个函数 startLoading 的实现，用于在全局状态中标记正在进行加载，并设置用于提供加载统计信息的字段。

	if am.baseAofInfo != nil {
		if am.baseAofInfo.aofFileType != AofManifestFileTypeBase {
			aofName = string(am.baseAofInfo.fileName)
			UpdateLoadingFileName(aofName)
			baseSize = GetAppendOnlyFileSize(aofName, nil)
			lastFile = totalNum
			start = Ustime()
			ret = ld.LoadSingleAppendOnlyFile(aofName, ch)
			if ret == AOF_OK || (ret == AOF_TRUNCATED && lastFile == 1) {
				fmt.Printf("DB loaded from base file %v: %.3f seconds", aofName, float64(Ustime()-start)/1000000)
				log.Infof("DB loaded from base file %v: %.3f seconds", aofName, float64(Ustime()-start)/1000000)
			}

			if ret == AOF_EMPTY {
				ret = AOF_OK
			}

			if ret == AOF_TRUNCATED && lastFile == 0 {
				ret = AOF_FAILED
				fmt.Printf("Fatal error: the truncated file is not the last file")
				log.Infof("Fatal error: the truncated file is not the last file")
			}

			if ret == AOF_OPEN_ERR || ret == AOF_FAILED {
				if ret == AOF_OK || ret == AOF_TRUNCATED {
					fmt.Printf("The aof file was successfully loaded\n")
					log.Infof("The aof file was successfully loaded\n")
				} else {
					if ret == AOF_OPEN_ERR {
						fmt.Printf("There was an error opening the AOF file.\n")
						log.Infof("There was an error opening the AOF file.\n")
					} else {
						fmt.Printf("Failed to open AOF file.\n")
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
				fmt.Printf("The manifestType must be Incr")

				log.Panicf("The manifestType must be Incr")
			}
			aofName = ai.fileName
			UpdateLoadingFileName(aofName)
			lastFile = totalNum
			aofNum++
			start = Ustime()
			ret = ld.LoadSingleAppendOnlyFile(aofName, ch)
			if ret == AOF_OK || (ret == AOF_TRUNCATED && lastFile == 1) {
				fmt.Printf("DB loaded from incr file %v: %.3f seconds", aofName, float64(Ustime()-start)/1000000)
				log.Infof("DB loaded from incr file %v: %.3f seconds", aofName, float64(Ustime()-start)/1000000)
			}

			if ret == AOF_EMPTY {
				ret = AOF_OK
			}

			if ret == AOF_TRUNCATED && lastFile == 0 {
				ret = AOF_FAILED
				fmt.Printf("Fatal error: the truncated file is not the last file\n")
				log.Infof("Fatal error: the truncated file is not the last file\n")
			}

			if ret == AOF_OPEN_ERR || ret == AOF_FAILED {
				//to do stopLoading(ret == AOF_OK || ret == AOF_TRUNCATED) /*总体而言，这段代码的目的是在加载过程结束时，更新全局状态中的加载相关字段，并触发加载结束事件以通知相关模块。
				//具体这些字段和事件的含义和功能可能需要参考完整代码和相关函数的定义才能理解清楚*/
				if ret == AOF_OPEN_ERR {
					fmt.Printf("There was an error opening the AOF file.\n")
					log.Infof("There was an error opening the AOF file.\n")
				} else {
					fmt.Printf("Failed to open AOF file.\n")
					log.Infof("Failed to open AOF file.\n")
				}
				return ret
			}
			ln = ListNext(&li)
		}

	}

	AOFINFO.aof_current_size = totalSize
	AOFINFO.aof_rewrite_base_size = baseSize
	return ret

}
