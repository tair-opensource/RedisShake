package aof

import (
	"bytes"
	"encoding/binary"
	"fmt"
	"log"
	"strconv"
	"unicode"

	"github.com/alibaba/RedisShake/internal/entry"
	"golang.org/x/tools/cmd/getgo/server"
)

const (
	AofManifestFileTypeBase = "b" /* Base file */
	AofManifestTypeHist     = "h" /* History file */
	AofManifestTypeIncr     = "i" /* INCR file */
	RDB_FORMAT_SUFFIX       = ".rdb"
	AOF_FORMAT_SUFFIX       = ".aof"
)

/* AOF manifest definition */
type aofInfo struct {
	fileName    string
	fileSeq     int64
	aofFileType string
}

func IntToBytes(n int) []byte {
	data := int64(n)
	bytebuf := bytes.NewBuffer([]byte{})
	binary.Write(bytebuf, binary.BigEndian, data)
	return bytebuf.Bytes()
}

func aofInfoCreate() *aofInfo {
	return new(aofInfo)
}

var Aof_Info aofInfo = *aofInfoCreate()

func (a *aofInfo) get_AofInfo_Name() string {
	return a.fileName
}

func stringNeedsRepr(s string) int {
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
func hexDigitToInt(c byte) int {
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

func splitArgs(line string) ([]string, int) {
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

			if current == "" {
				current = ""
			}
			for !done {
				if inq {
					_, err1 := strconv.ParseInt(string(p[i+2]), 16, 64)
					_, err2 := strconv.ParseInt(string(p[i+3]), 16, 64)
					if p[i] == '\\' && (p[i+1]) == 'x' && err1 == nil && err2 == nil {
						int16 := (hexDigitToInt((p[i+2])) * 16) + hexDigitToInt(p[i+3])
						//var bytes []byte
						//bytes=IntToBytes(int16)
						//current = stringcatlen(current,bytes,1)
						current += string(int16)
						i += 3

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
						if i+1 < lens && unicode.IsSpace((rune(p[i+1]))) {
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
					case ' ', '\n', '\r', '\t':
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

func stringcatlen(s string, t []byte, lent int) string {
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

func aofInfoDup(orig *aofInfo) *aofInfo {
	if orig == nil {
		log.Fatal("Assertion failed: orig != nil")
	}
	ai := aofInfoCreate()
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

func listCreate() *lists {
	lists := &lists{}
	lists.head = nil
	lists.tail = nil
	lists.len = 0
	return lists
}
func listNext(iter *listIter) *listNode {
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

func listAddNodeTail(lists *lists, value interface{}) *lists {
	node := &listNode{
		value: value,
		prev:  nil,
		next:  nil,
	}
	listLinkNodeTail(lists, node)
	return lists
}

func listLinkNodeTail(lists *lists, node *listNode) {
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

// TODO：完成checAofMain后写单测进行测试
func (ld *Loader) ParseRDB() int {
	// 加载aof目录
	// 进行check_aof， aof
	CheckAofMain(ld.filPath)
	// TODO：执行加载
	return 0
}

func stringcatprintf(s string, fmtStr string, args ...interface{}) string {
	result := fmt.Sprintf(fmtStr, args...)
	return s + result
}

func stringcatrepr(s string, p string, length int) string {
	s = s + string("\"")
	for i := 0; i < length; i++ {
		switch p[i] {
		case '\\', '"':
			s = stringcatprintf(s, "\\%c", p[i])
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

func aofInfoFormat(buf string, ai *aofInfo) string {
	var filenameRepr string
	if stringNeedsRepr(ai.fileName) == 1 {
		filenameRepr = stringcatrepr("", ai.fileName, len(ai.fileName))
	}
	var ret string
	if filenameRepr != "" {
		ret = stringcatprintf(buf, "%s %s %s %d %s %c\n", AOF_MANIFEST_KEY_FILE_NAME, filenameRepr, AOF_MANIFEST_KEY_FILE_SEQ, ai.fileSeq, AOF_MANIFEST_KEY_FILE_TYPE, ai.aofFileType)
	} else {
		ret = stringcatprintf(buf, "%s %s %s %d %s %c\n", AOF_MANIFEST_KEY_FILE_NAME, ai.fileName, AOF_MANIFEST_KEY_FILE_SEQ, ai.fileSeq, AOF_MANIFEST_KEY_FILE_TYPE, ai.aofFileType)
	}
	return ret
}

func aofManifestcreate() *aofManifest {
	am := &aofManifest{
		incrAofList: listCreate(),
		historyList: listCreate(),
	}
	return am
}

func listDup(orig *lists) *lists {
	var copy *lists
	var iter listIter
	var node *listNode
	copy = listCreate()
	if copy == nil {
		return nil
	}
	copy.ListsRewind(&iter)
	node = listNext(&iter)
	var value interface{}
	for node != nil {
		value = node.value
	}

	if listAddNodeTail(copy, value) == nil {
		return nil
	}
	return copy
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
		am.baseAofInfo = aofInfoDup(orig.baseAofInfo)
	}

	am.incrAofList = listDup(orig.incrAofList)
	am.historyList = listDup(orig.historyList)

	if am.incrAofList == nil || am.historyList == nil {
		panic("IncrAOFlist or HistoryAOFlist is nil")
	}
	return am
}

func getAofManifestAsString(am *aofManifest) string {
	if am == nil {
		panic("am is nil")
	}
	var buf string
	var ln *listNode
	var li listIter

	if am.baseAofInfo != nil {
		buf = aofInfoFormat(buf, am.baseAofInfo)
	}
	am.historyList.ListsRewind(&li)
	ln = listNext(&li)
	for ln != nil {
		ai, ok := ln.value.(*aofInfo)
		if ok {
			buf = aofInfoFormat(buf, ai)
		}
		ln = listNext(&li)
	}

	am.incrAofList.ListsRewind(&li)
	ln = listNext(&li)
	for ln != nil {
		ai, ok := ln.value.(*aofInfo)
		if ok {
			buf = aofInfoFormat(buf, ai)
		}
		ln = listNext(&li)
	}

	return buf

}

func getNewBaseFileNameAndMarkPreAsHistory(am *aofManifest) string {
	if am == nil {
		log.Fatal("aofManifest is nil")
	}
	if am.baseAofInfo != nil {
		if am.baseAofInfo.aofFileType != AofManifestFileTypeBase {
			log.Fatal("base_aof_info has invalid file_type")
		}
		am.baseAofInfo.aofFileType = AofManifestTypeHist
	}
	var formatSuffix string
	if server.aofUseRdbPreamble {
		formatSuffix = RDB_FORMAT_SUFFIX
	} else {
		formatSuffix = AOF_FORMAT_SUFFIX
	}
	ai := aofInfoCreate()
	ai.fileName = fmt.Sprintf("%s.%d%s%d", Aof_Info.get_AofInfo_Name(), am.currBaseFileSeq+1, BASE_FILE_SUFFIX, formatSuffix)
	ai.fileSeq = am.currBaseFileSeq + 1
	ai.aofFileType = AofManifestFileTypeBase
	am.baseAofInfo = ai
	am.dirty = 1
	return am.baseAofInfo.fileName
}

// server 未处理
func aofLoadManifestFromDisk() {
	aof_manifest := aofManifestcreate()
	if !difExists(server.aof_dirname) {
		fmt.Printf("The AOF directory %s doesn't exist\n", server.AofDirname)
		return
	}

	am_name := getAofManifestFileName()
	am_filepath := makePath(server.aof_dirname, am_name)
	if !fileExist(am_filepath) {
		fmt.Printf("The AOF directory %s doesn't exist\n", server.AofDirname)
		return
	}

	am := aofLoadManifestFromFile(am_filepath)
	if am != nil {
		aofManifestFreeAndUpdate(am)
	}

}
