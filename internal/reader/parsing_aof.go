package reader

import (
	"RedisShake/internal/aof"
	"RedisShake/internal/entry"
	"RedisShake/internal/log"
	"bytes"

	"os"
	"path"

	"bufio"
	"fmt"
	"io"
	"path/filepath"
	"strconv"
	"strings"
	"time"
	"unicode"
)

const (
	AOFManifestFileTypeBase = "b" /* Base File */
	AOFManifestTypeHist     = "h" /* History File */
	AOFManifestTypeIncr     = "i" /* INCR File */
	RDBFormatSuffix         = ".RDB"
	AOFFormatSuffix         = ".AOF"
	BaseFileSuffix          = ".Base"
	IncrFileSuffix          = ".incr"
	TempFileNamePrefix      = "temp-"
	COK                     = 1
	CERR                    = -1
	EINTR                   = 4
	ManifestNameSuffix      = ".manifest"
	AOFNotExist             = 1
	AOFOpenErr              = 3
	AOFOK                   = 0
	AOFEmpty                = 2
	AOFFailed               = 4
	AOFTruncated            = 5
	SizeMax                 = 128
	RDBFlagsAOFPreamble     = 1 << 0
	AOFManifestKeyFileName  = "File"
	AOFManifestKeyFileSeq   = "seq"
	AOFManifestKeyFileType  = "type"
)

var RDBFileBeingLoaded string = ""

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

type listIter struct {
	next      *listNode
	Direction int
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
	Current := iter.next

	if Current != nil {
		if iter.Direction == 0 {
			iter.next = Current.next
		} else {
			iter.next = Current.prev
		}
	}
	return Current
}

func (list *lists) ListsRewind(li *listIter) {
	li.next = list.head
	li.Direction = 0
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
	li.Direction = 1
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

func ListAddNodeHead(list *lists, Value interface{}) *lists {
	node := &listNode{
		value: Value,
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

/* AOF manifest definition */
type AOFInfo struct {
	FileName    string
	FileSeq     int64
	AOFFileType string
}

func AOFInfoCreate() *AOFInfo {
	return new(AOFInfo)
}

var AOF_Info AOFInfo = *AOFInfoCreate()

func AOFInfoDup(orig *AOFInfo) *AOFInfo {
	if orig == nil {
		log.Panicf("Assertion failed: orig != nil")
	}
	AI := AOFInfoCreate()
	AI.FileName = orig.FileName
	AI.FileSeq = orig.FileSeq
	AI.AOFFileType = orig.AOFFileType
	return AI
}

func AOFInfoFormat(buf string, AI *AOFInfo) string {
	var AOFManifestcreate string
	if StringNeedsRepr(AI.FileName) == 1 {
		AOFManifestcreate = Stringcatrepr("", AI.FileName, len(AI.FileName))
	}
	var ret string
	if AOFManifestcreate != "" {
		ret = Stringcatprintf(buf, "%s %s %s %d %s %s\n", AOFManifestKeyFileName, AOFManifestcreate, AOFManifestKeyFileSeq, AI.FileSeq, AOFManifestKeyFileType, AI.AOFFileType)
	} else {
		ret = Stringcatprintf(buf, "%s %s %s %d %s %s\n", AOFManifestKeyFileName, AI.FileName, AOFManifestKeyFileSeq, AI.FileSeq, AOFManifestKeyFileType, AI.AOFFileType)
	}
	return ret
}

type AOFManifest struct {
	BaseAOFInfo     *AOFInfo
	incrAOFList     *lists
	HistoryList     *lists
	CurrBaseFileSeq int64
	CurrIncrFileSeq int64
	Dirty           int64
}

func AOFManifestcreate() *AOFManifest {
	AM := &AOFManifest{
		incrAOFList: ListCreate(),
		HistoryList: ListCreate(),
	}
	return AM
}

func AOFManifestDup(orig *AOFManifest) *AOFManifest {
	if orig == nil {
		log.Panicf("The AOFManifest file is empty.")
	}

	AM := &AOFManifest{
		CurrBaseFileSeq: orig.CurrBaseFileSeq,
		CurrIncrFileSeq: orig.CurrIncrFileSeq,
		Dirty:           orig.Dirty,
	}

	if orig.BaseAOFInfo != nil {
		AM.BaseAOFInfo = AOFInfoDup(orig.BaseAOFInfo)
	}

	AM.incrAOFList = ListDup(orig.incrAOFList)
	AM.HistoryList = ListDup(orig.HistoryList)

	if AM.incrAOFList == nil || AM.HistoryList == nil {
		log.Panicf("IncrAOFlist or HistoryAOFlist is nil")
	}
	return AM
}

func GetAOFManifestAsString(AM *AOFManifest) string {
	if AM == nil {
		log.Panicf("The AOFManifest file is empty.")
	}
	var buf string
	var ln *listNode
	var li listIter

	if AM.BaseAOFInfo != nil {
		buf = AOFInfoFormat(buf, AM.BaseAOFInfo)
	}
	AM.HistoryList.ListsRewind(&li)
	ln = ListNext(&li)
	for ln != nil {
		AI, ok := ln.value.(*AOFInfo)
		if ok {
			buf = AOFInfoFormat(buf, AI)
		}
		ln = ListNext(&li)
	}

	AM.incrAOFList.ListsRewind(&li)
	ln = ListNext(&li)
	for ln != nil {
		AI, ok := ln.value.(*AOFInfo)
		if ok {
			buf = AOFInfoFormat(buf, AI)
		}
		ln = ListNext(&li)
	}

	return buf

}

func GetNewBaseFileNameAndMarkPreAsHistory(AM *AOFManifest) string {
	if AM == nil {
		log.Panicf("The AOFManifest file is empty.")
	}
	if AM.BaseAOFInfo != nil {
		if AM.BaseAOFInfo.AOFFileType != AOFManifestFileTypeBase {
			log.Panicf("Base_AOF_info has invalid File_type")
		}
		AM.BaseAOFInfo.AOFFileType = AOFManifestTypeHist
	}
	var formatSuffix string
	if AOFFileInfo.AOFUseRDBPreamble == 1 {
		formatSuffix = RDBFormatSuffix
	} else {
		formatSuffix = AOFFormatSuffix
	}
	AI := AOFInfoCreate()
	AI.FileName = Stringcatprintf("%s.%d%s%d", AOF_Info.GetAOFInfoName(), AM.CurrBaseFileSeq+1, BaseFileSuffix, formatSuffix)
	AI.FileSeq = AM.CurrBaseFileSeq + 1
	AI.AOFFileType = AOFManifestFileTypeBase
	AM.BaseAOFInfo = AI
	AM.Dirty = 1
	return AM.BaseAOFInfo.FileName
}

func AOFLoadManifestFromFile(AM_Filepath string) *AOFManifest {
	var maxseq int64
	AM := AOFManifestcreate()
	fp, err := os.Open(AM_Filepath)
	if err != nil {
		log.Panicf("Fatal error:can't open the AOF manifest %v for reading: %v", AM_Filepath, err)
	}
	var argv []string
	var AI *AOFInfo
	var line string
	linenum := 0
	reader := bufio.NewReader(fp)
	for {
		buf, err := reader.ReadString('\n')
		if err != nil {
			if err == io.EOF {
				if linenum == 0 {
					log.Infof("Found an empty AOF manifest")
					AM = nil
					return AM
				} else {
					break
				}

			} else {
				log.Infof("Read AOF manifest failed")
				AM = nil
				return AM

			}
		}

		linenum++
		if buf[0] == '#' {
			continue
		}
		if !strings.Contains(buf, "\n") {
			log.Infof("The AOF manifest File contains too long line")
			return nil
		}
		line = strings.Trim(buf, " \t\r\n")
		if len(line) == 0 {
			log.Infof("Invalid AOF manifest File format")
			return nil
		}
		argc := 0
		argv, argc = SplitArgs(line)

		if argc < 6 || argc%2 != 0 {
			log.Infof("Invalid AOF manifest File format")
			AM = nil
			return AM
		}
		AI = AOFInfoCreate()
		for i := 0; i < argc; i += 2 {
			if strings.EqualFold(argv[i], AOFManifestKeyFileName) {
				AI.FileName = string(argv[i+1])
				if !PathIsBaseName(string(AI.FileName)) {
					log.Panicf("File can't be a path, just a Filename")
				}
			} else if strings.EqualFold(argv[i], AOFManifestKeyFileSeq) {
				AI.FileSeq, _ = strconv.ParseInt(argv[i+1], 10, 64)
			} else if strings.EqualFold(argv[i], AOFManifestKeyFileType) {
				AI.AOFFileType = string(argv[i+1][0])
			}
		}
		if AI.FileName == "" || AI.FileSeq == 0 || AI.AOFFileType == "" {
			log.Panicf("Invalid AOF manifest File format")
		}
		if AI.AOFFileType == AOFManifestFileTypeBase {
			if AM.BaseAOFInfo != nil {
				log.Panicf("Found duplicate Base File information")
			}
			AM.BaseAOFInfo = AI
			AM.CurrBaseFileSeq = AI.FileSeq
		} else if AI.AOFFileType == AOFManifestTypeHist {
			AM.HistoryList = ListAddNodeTail(AM.HistoryList, AI)
		} else if AI.AOFFileType == AOFManifestTypeIncr {
			if AI.FileSeq <= maxseq {
				log.Panicf("Found a non-monotonic sequence number")
			}
			AM.incrAOFList = ListAddNodeTail(AM.HistoryList, AI)
			AM.CurrIncrFileSeq = AI.FileSeq
			maxseq = AI.FileSeq
		} else {
			log.Panicf("Unknown AOF File type")
		}
		line = " "
		AI = nil
	}
	fp.Close()
	return AM
}

func AOFLoadManifestFromDisk() {
	AOFFileInfo.AOFManifest = AOFManifestcreate()
	if DirExists(AOFFileInfo.AOFDirName) == 0 {
		log.Infof("The AOF Directory %v doesn't exist\n", AOFFileInfo.AOFDirName)
		return
	}

	AMName := GetAOFManifestFileName()
	AMFilepath := path.Join(AOFFileInfo.AOFDirName, AMName)
	if FileExist(AMFilepath) == 0 {
		log.Infof("The AOF Directory %v doesn't exist\n", AOFFileInfo.AOFDirName)
		return
	}

	AM := AOFLoadManifestFromFile(AMFilepath)
	AOFFileInfo.AOFManifest = AM

}

func GetNewIncrAOFName(AM *AOFManifest) string {
	AI := AOFInfoCreate()
	AI.AOFFileType = AOFManifestTypeIncr
	AI.FileName = Stringcatprintf("", "%s.%d%s%s", AOFFileInfo.AOFFileName, AM.CurrIncrFileSeq+1, IncrFileSuffix, AOFFormatSuffix)
	AI.FileSeq = AM.CurrIncrFileSeq + 1
	ListAddNodeTail(AM.incrAOFList, AI)
	AM.Dirty = 1
	return AI.FileName
}

func GetTempIncrAOFNanme() string {
	return Stringcatprintf("", "%s%s%s", TempFileNamePrefix, AOFFileInfo.AOFFileName, IncrFileSuffix)
}

func GetLastIncrAOFName(AM *AOFManifest) string {
	if AM == nil {
		log.Panicf(("AOFManifest is nil"))
	}

	if AM.incrAOFList.len == 0 {
		return GetNewIncrAOFName(AM)
	}

	lastnode := ListIndex(AM.incrAOFList, -1)

	AI, ok := lastnode.value.(AOFInfo)
	if !ok {
		log.Panicf("Failed to convert lastnode.value to AOFInfo")
	}
	return AI.FileName
}

func GetAOFManifestFileName() string {
	return AOFFileInfo.AOFFileName
}

func GetTempAOFManifestFileName() string {
	return Stringcatprintf("", "%s%s", TempFileNamePrefix, AOFFileInfo.AOFFileName)
}

func StartLoading(size int64, RDBflags int, async int) {
	/* Load the DB */
	log.Infof("The AOF File starts loading.\n")
}

func StopLoading(ret int) {

	if ret == AOFOK || ret == AOFTruncated {
		log.Infof("The AOF File was successfully loaded\n")
	} else {
		log.Infof("There was an error opening the AOF File.\n")
	}
}

func AOFFileExist(FileName string) int {
	Filepath := path.Join(AOFFileInfo.AOFDirName, FileName)
	ret := FileExist(Filepath)
	return ret
}

func GetAppendOnlyFileSize(FileName string, status *int) int64 {
	var size int64

	AOFFilePath := path.Join(AOFFileInfo.AOFDirName, FileName)

	stat, err := os.Stat(AOFFilePath)
	if err != nil {
		if status != nil {
			if os.IsNotExist(err) {
				*status = AOFNotExist
			} else {
				*status = AOFOpenErr
			}
		}
		log.Panicf("Unable to obtain the AOF File %v length. stat: %v", FileName, err.Error())
		size = 0
	} else {
		if status != nil {
			*status = AOFOK
		}
		size = stat.Size()
	}
	return size
}

func GetBaseAndIncrAppendOnlyFilesSize(AM *AOFManifest, status *int) int64 {
	var size int64
	var ln *listNode = new(listNode)
	var li *listIter = new(listIter)
	if AM.BaseAOFInfo != nil {
		if AM.BaseAOFInfo.AOFFileType != AOFManifestFileTypeBase {
			log.Panicf("File type must be Base.")
		}
		size += GetAppendOnlyFileSize(AM.BaseAOFInfo.FileName, status)
		if *status != AOFOK {
			return 0
		}
	}

	AM.incrAOFList.ListsRewind(li)
	ln = ListNext(li)
	for ln != nil {
		AI := ln.value.(*AOFInfo)
		if AI.AOFFileType != AOFManifestTypeIncr {
			log.Panicf("File type must be Incr")
		}
		size += GetAppendOnlyFileSize(AI.FileName, status)
		if *status != AOFOK {
			return 0
		}
		ln = ListNext(li)
	}
	return size
}

func GetBaseAndIncrAppendOnlyFilesNum(AM *AOFManifest) int {
	Num := 0
	if AM.BaseAOFInfo != nil {
		Num++
	}
	if AM.incrAOFList != nil {
		Num += int(AM.incrAOFList.len)
	}
	return Num
}

type INFO struct {
	AOFDirName         string
	AOFUseRDBPreamble  int
	AOFManifest        *AOFManifest
	AOFFileName        string
	AOFCurrentSize     int64
	AOFRewriteBaseSize int64
}

var AOFFileInfo INFO

func (a *INFO) GetAOFDirName() string {
	return a.AOFDirName
}

func NewAOFFileInfo(aofFilePath string) *INFO {
	return &INFO{
		AOFDirName:         filepath.Dir(aofFilePath),
		AOFUseRDBPreamble:  0,
		AOFManifest:        nil,
		AOFFileName:        filepath.Base(aofFilePath),
		AOFCurrentSize:     0,
		AOFRewriteBaseSize: 0,
	}
}

func (a *INFO) SetAOFDirName(AOFDirName string) {
	a.AOFDirName = AOFDirName
}

func (a *INFO) GetAOFUseRDBPreamble() int {
	return a.AOFUseRDBPreamble
}

func (a *INFO) SetAOFUseRDBPreamble(AOFUseRDBPreamble int) {
	a.AOFUseRDBPreamble = AOFUseRDBPreamble
}

func (a *INFO) GetAOFManifest() *AOFManifest {
	return a.AOFManifest
}
func (a *INFO) SetAOFManifest(Manifest *AOFManifest) {
	a.AOFManifest = Manifest
}

func (a *INFO) GetAOFFileName() string {
	return a.AOFFileName
}

func (a *INFO) SetAOFFileName(AOFFileName string) {
	a.AOFFileName = AOFFileName
}

func (a *INFO) GetAOFCurrentSize() int64 {
	return a.AOFCurrentSize
}

func (a *INFO) SetAOFCurrentSize(size int64) {
	a.AOFCurrentSize = size
}

func (a *INFO) GetAOFRewriteBaseSize() int64 {
	return a.AOFRewriteBaseSize
}

func (a *INFO) SetAOFRewriteBaseSize(size int64) {
	a.AOFRewriteBaseSize = size
}

type Loader struct {
	filPath string
	ch      chan *entry.Entry
}

func NewLoader(FilPath string, ch chan *entry.Entry) *Loader {
	ld := new(Loader)
	ld.ch = ch
	ld.filPath = FilPath
	return ld
}

func (a *AOFInfo) GetAOFInfoName() string {
	return a.FileName
}

func DirExists(dName string) int {
	_, err := os.Stat(dName)
	if err != nil {
		return 0
	}

	return 1
}

func FileExist(FileName string) int {
	_, err := os.Stat(FileName)
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
	var Current string
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
							Current = Current + fmt.Sprint(int16)
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
						Current += string(c)
					} else if p[i] == '"' {
						if i+1 < lens && !unicode.IsSpace((rune(p[i+1]))) {
							return nil, 0
						}
						done = true
					} else if i >= lens {
						return nil, 0
					} else {
						Current += string(p[i])
					}
				} else if insq {
					if p[i] == '\\' && p[i+1] == '\'' {
						i++
						Current += "'"
					} else if p[i] == '\'' {
						if i+1 < lens && !unicode.IsSpace((rune(p[i+1]))) {
							return nil, 0
						}
						done = true
					} else if i >= lens {
						return nil, 0
					} else {
						Current += string(p[i])
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
						Current += string(p[i])
					}
				}
				if i < lens {
					i++
				}
				if i == lens {
					done = true
				}
			}

			vector = append(vector, Current)
			argc++
			Current = ""

		} else {
			return vector, argc
		}

	}
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

func UpdateLoadingFileName(FileName string) {
	RDBFileBeingLoaded = FileName
}

func PathIsBaseName(Path string) bool {
	return strings.IndexByte(Path, '/') == -1 && strings.IndexByte(Path, '\\') == -1
}

func (ld *Loader) LoadAppendOnlyFile(AM *AOFManifest, ch chan *entry.Entry, AOFTimeStamp int64) int {
	if AM == nil {
		log.Panicf("AOFManifest is null")
	}
	status := AOFOK
	ret := AOFOK
	var start int64
	var TotalSize int64 = 0
	var BaseSize int64 = 0
	var AOFName string
	var TotalNum, AOFNum, LastFile int

	if AOFFileExist(AOFFileInfo.AOFFileName) == 1 {
		if DirExists(AOFFileInfo.AOFDirName) == 0 ||
			(AM.BaseAOFInfo == nil && AM.incrAOFList.len == 0) ||
			(AM.BaseAOFInfo != nil && AM.incrAOFList.len == 0 &&
				strings.Compare(AM.BaseAOFInfo.FileName, AOFFileInfo.AOFFileName) == 0 && AOFFileExist(AOFFileInfo.AOFFileName) == 0) {
			log.Panicf("This is an old version of the AOF File")
		}
	}

	if AM.BaseAOFInfo == nil && AM.incrAOFList == nil {
		return AOFNotExist
	}

	TotalNum = GetBaseAndIncrAppendOnlyFilesNum(AM)
	if TotalNum <= 0 {
		log.Panicf("Assertion failed: IncrAppendOnlyFilestotalNum > 0")
	}

	TotalSize = GetBaseAndIncrAppendOnlyFilesSize(AM, &status)
	if status != AOFOK {
		if status == AOFNotExist {
			status = AOFFailed
		}
		return status
	} else if TotalSize == 0 {
		return AOFEmpty
	}

	StartLoading(TotalSize, RDBFlagsAOFPreamble, 0)
	if AM.BaseAOFInfo != nil {
		if AM.BaseAOFInfo.AOFFileType == AOFManifestFileTypeBase {
			AOFName = string(AM.BaseAOFInfo.FileName)
			UpdateLoadingFileName(AOFName)
			BaseSize = GetAppendOnlyFileSize(AOFName, nil)
			LastFile = TotalNum
			start = Ustime()
			ret = ld.ParsingSingleAppendOnlyFile(AOFName, ch, false, AOFTimeStamp)
			if ret == AOFOK || (ret == AOFTruncated && LastFile == 1) {
				log.Infof("DB loaded from Base File %v: %.3f seconds", AOFName, float64(Ustime()-start)/1000000)
			}

			if ret == AOFEmpty {
				ret = AOFOK
			}

			if ret == AOFTruncated && LastFile == 0 {
				ret = AOFFailed
				log.Infof("Fatal error: the truncated File is not the last File")
			}

			if ret == AOFOpenErr || ret == AOFFailed {
				if ret == AOFOK || ret == AOFTruncated {
					log.Infof("The AOF File was successfully loaded\n")
				} else {
					if ret == AOFOpenErr {
						log.Panicf("There was an error opening the AOF File.\n")
					} else {
						log.Panicf("Failed to open AOF File.\n")
					}
				}
				return ret
			}
		}
		TotalNum--
	}

	if AM.incrAOFList.len > 0 {
		var ln *listNode = new(listNode)
		var li listIter

		AM.incrAOFList.ListsRewind(&li)
		ln = ListNext(&li)
		for ln != nil {
			AI := ln.value.(*AOFInfo)
			if AI.AOFFileType != AOFManifestTypeIncr {
				log.Panicf("The manifestType must be Incr")
			}
			AOFName = AI.FileName
			UpdateLoadingFileName(AOFName)
			LastFile = TotalNum
			AOFNum++
			start = Ustime()
			if LastFile == 1 {
				ret = ld.ParsingSingleAppendOnlyFile(AOFName, ch, true, AOFTimeStamp)
			} else {
				ret = ld.ParsingSingleAppendOnlyFile(AOFName, ch, false, AOFTimeStamp)
			}
			if ret == AOFOK || (ret == AOFTruncated && LastFile == 1) {
				log.Infof("DB loaded from incr File %v: %.3f seconds", AOFName, float64(Ustime()-start)/1000000)
			}

			if ret == AOFEmpty {
				ret = AOFOK
			}

			if ret == AOFTruncated && LastFile == 0 {
				ret = AOFFailed
				log.Infof("Fatal error: the truncated File is not the last File\n")
			}

			if ret == AOFOpenErr || ret == AOFFailed {
				if ret == AOFOpenErr {
					log.Infof("There was an error opening the AOF File.\n")
				} else {
					log.Infof("Failed to open AOF File.\n")
				}
				return ret
			}
			ln = ListNext(&li)
		}
		TotalNum--
	}

	AOFFileInfo.AOFCurrentSize = TotalSize
	AOFFileInfo.AOFRewriteBaseSize = BaseSize
	return ret

}

func (ld *Loader) ParsingSingleAppendOnlyFile(FileName string, ch chan *entry.Entry, LastFile bool, AOFTimeStamp int64) int {
	ret := AOFOK
	AOFFilepath := path.Join(AOFFileInfo.AOFDirName, FileName)
	fp, err := os.Open(AOFFilepath)
	if err != nil {
		if os.IsNotExist(err) {
			if _, err := os.Stat(AOFFilepath); err == nil || !os.IsNotExist(err) {
				log.Infof("Fatal error: can't open the append log File %v for reading: %v", FileName, err.Error())
				return AOFOpenErr
			} else {
				log.Infof("The append log File %v doesn't exist: %v", FileName, err.Error())
				return AOFNotExist
			}

		}
		defer fp.Close()

		stat, _ := fp.Stat()
		if stat.Size() == 0 {
			return AOFEmpty
		}
	}
	sig := make([]byte, 5)
	if n, err := fp.Read(sig); err != nil || n != 5 || !bytes.Equal(sig, []byte("REDIS")) {
		if _, err := fp.Seek(0, 0); err != nil {
			log.Infof("Unrecoverable error reading the append only File %v: %v", FileName, err)
			ret = AOFFailed
			return ret
		}
	} else {
		log.Infof("Reading RDB Base File on AOF loading...")
		rdbOpt := RdbReaderOptions{Filepath: AOFFilepath}
		ldRDB := NewRDBReader(&rdbOpt)
		log.Infof("create RdbReader: %v", rdbOpt.Filepath)
		ldRDB.StartRead()
		return AOFOK
		//Skipped RDB checksum and has not been processed yet.
	}
	ret = aof.LoadSingleAppendOnlyFile(AOFFileInfo.AOFDirName, FileName, ld.ch, LastFile, AOFTimeStamp)
	return ret

}
