package aof

import (
	"bufio"
	"bytes"
	"encoding/binary"
	"fmt"
	"io"
	"log"
	"os"
	"strconv"
	"strings"
	"time"
	"unicode"

	"github.com/alibaba/RedisShake/internal/entry"
	"github.com/alibaba/RedisShake/internal/writer"
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
)

var rdbFileBeingLoaded string = ""

func updateLoadingFileName(filename string) {
	rdbFileBeingLoaded = filename
}

/* AOF manifest definition */
type aofInfo struct {
	fileName    string
	fileSeq     int64
	aofFileType string
}

type server struct {
	aof_dirname           string
	aofUseRdbPreamble     int
	aof_manifest          *aofManifest
	aof_filename          string
	aof_current_size      int64
	aof_rewrite_base_size int64
}

func IntToBytes(n int) []byte {
	data := int64(n)
	bytebuf := bytes.NewBuffer([]byte{})
	binary.Write(bytebuf, binary.BigEndian, data)
	return bytebuf.Bytes()
}

func ustime() int64 {
	tv := time.Now()
	ust := int64(tv.UnixNano()) / 1000
	return ust
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

func dirExists(dname string) int {
	_, err := os.Stat(dname)
	if err != nil {
		return 0
	}

	return 1
}

func fileExist(filename string) int {
	_, err := os.Stat(filename)
	if err != nil {
		return 0
	}

	return 1
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
	if server.aofUseRdbPreamble == 1 {
		formatSuffix = RDB_FORMAT_SUFFIX
	} else {
		formatSuffix = AOF_FORMAT_SUFFIX
	}
	ai := aofInfoCreate()
	ai.fileName = stringcatprintf("%s.%d%s%d", Aof_Info.get_AofInfo_Name(), am.currBaseFileSeq+1, BASE_FILE_SUFFIX, formatSuffix)
	ai.fileSeq = am.currBaseFileSeq + 1
	ai.aofFileType = AofManifestFileTypeBase
	am.baseAofInfo = ai
	am.dirty = 1
	return am.baseAofInfo.fileName
}

// server 未处理
func aofLoadManifestFromDisk() {
	server.aof_manifest = aofManifestcreate()
	if !dirExists(server.aof_dirname) {
		fmt.Printf("The AOF directory %s doesn't exist\n", server.aof_dirname)
		return
	}

	am_name := getAofManifestFileName()
	am_filepath := makePath(server.aof_dirname, am_name)
	if fileExist(am_filepath) == 0 {
		fmt.Printf("The AOF directory %s doesn't exist\n", server.aof_dirname)
		return
	}

	am := aofLoadManifestFromFile(am_filepath)
	if am != nil {
		server.aof_manifest = &am
	}

}

func getNewIncrAofName(am *aofManifest) string {
	ai := aofInfoCreate()
	ai.aofFileType = AofManifestTypeIncr
	ai.fileName = stringcatprintf("", "%s.%d%s%s", server.aof_filename, am.currIncrFIleSeq+1, INCR_FILE_SUFFIX, AOF_FORMAT_SUFFIX)
	ai.fileSeq = am.currIncrFIleSeq + 1
	listAddNodeTail(am.incrAofList, ai)
	am.dirty = 1
	return ai.fileName
}

func getTempIncrAofNanme() string {
	return stringcatprintf("", "%s%s%s", TEMP_FILE_NAME_PREFIX, server.aof_filename, INCR_FILE_SUFFIX)
}

func listIndex(list *lists, index int64) *listNode {
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

func listLinkNodeHead(list *lists, node *listNode) {
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

func listAddNodeHead(list *lists, value interface{}) *lists {
	node := &listNode{
		value: value,
	}
	listLinkNodeHead(list, node)

	return list
}

func listUnlinkNode(list *lists, node *listNode) {
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
func listDelNode(list *lists, node *listNode) {
	listUnlinkNode(list, node)

}

func getLastIncrAofName(am *aofManifest) string {
	if am == nil {
		log.Fatal(("aofManifest is nil"))
	}

	if am.incrAofList.len == 0 {
		return getNewIncrAofName(am)
	}

	lastnode := listIndex(am.incrAofList, -1)

	ai, ok := lastnode.value.(aofInfo)
	if !ok {
		log.Fatal("Failed to convert lastnode.value to aofInfo")
	}
	return ai.fileName
}

/*func markRewrittenIncrAofAsHistory(am *aofManifest) {
	if am == nil {
		log.Fatal("aofManifest is nil")
	}
	if am.incrAofList.len == 0 {
		return
	}
	var ln *listNode
	var li listIter

	ListsRewindTail(am.incrAofList, &li)

	// "server.aof_fd != -1" means AOF enabled, then we must skip the
	// last AOF, because this file is our currently writing.
	if server.aof_fd != -1 {
		ln = listNext(&li)
		if ln == nil {
			log.Fatal("List element is nil")
		}
	}

	// Move aofInfo from 'incr_aof_list' to 'history_aof_list'.
	for ln != nil {
		ai, ok := ln.value.(*aofInfo)
		if !ok {
			log.Fatal("Failed to convert ln.Value to *aofInfo")
		}
		if ai.aofFileType != AofManifestTypeIncr {
			log.Fatal("Unexpected file type")
		}

		hai := aofInfoDup(ai)
		hai.aofFileType = AofManifestTypeHist
		listAddNodeHead(am.historyList, hai)
		listDelNode(am.incrAofList, ln)
	}

	am.dirty = 1
}*/

func getAofManifestFileName() string {
	return stringcatprintf("", "%s%s", server.aof_filename, MANIFEST_NAME_SUFFIX)
}

func getTempAofManifestFileName() string {
	return stringcatprintf("", "%s%s%s", TEMP_FILE_NAME_PREFIX, server.aof_filename, MANIFEST_NAME_SUFFIX)
}

//serverLog 未处理
/*
func writeAofManifestFile(buf []byte)int{
	var ret =C_OK

	amName:=getAofManifestFileName()
	amFilePath:=makePath(server.aofDirname,amName)
	tmpAmName:=getTempAofManifestFileName()
	tmpAmFilePath:=makePath(server.aofDirname,tmpAmName)

	fd,err:=os.OpenFile(tmpAmFilePath,os.O_WRONLY|os.O_TRUNC|os.O_CREATE,0644)

	if err!=nil{
		fmt.Sprintf("Can't open the AOF manifest file %s: %s", tmpAmName, err)
		fd.Close()
		ret=C_ERR
		return ret
	}
	_, err = fd.Write(buf)
	if err != nil {
		fmt.Printf("Error trying to write the temporary AOF manifest file %s: %s\n", tmpAmName, err.Error())
		fd.Close()
		ret=C_ERR
		return ret
	}


	err=fd.Sync()
	if err!=nil{
	fmt.Sprintf("Fail to sync the temp AOF file %s: %s.",tmpAmName,err)
	fd.Close()
	ret=C_ERR
	return ret
	}

	if(rename(tmpamfilepath,amFilePath)!=0){
		fmt.Sprintf("Fail to fsync AOF directory %s:%s.",amFilePath,err)
		ret=C_ERR
		fd.Close()
		return ret
	}

	err=fsyncFileDir(amFilePath)
	if err!=nil{
	fmt.Sprintf("Fail to fsync AOF directory %s:%s.",amFilePath,err)
	fd.Close()
	ret=C_ERR
	return ret
	}

	return ret

}*/
func loadSingleAppendOnlyFile(filename string) int {
	var fargc int
	loops := 0
	ret := AOF_OK
	aof_filepath := makePath(server.aof_dirname, filename)
	fp, err := os.Open(aof_filepath)
	if err != nil {
		if os.IsNotExist(err) {
			if _, err := os.Stat(aof_filepath); err == nil || !os.IsNotExist(err) {
				fmt.Println("Fatal error: can't open the append log file %s for reading: %s", filename, err.Error())
				return AOF_OPEN_ERR
			} else {
				fmt.Println("The append log file %s doesn't exist: %s", filename, err.Error())
				return AOF_NOT_EXIST
			}
			fmt.Println("Fatal error: can't open the append log file %s for reading: %s", filename, err.Error())
			return AOF_OPEN_ERR
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
			fmt.Println("Unrecoverable error reading the append only file %s: %s", filename, err)
			ret = AOF_FAILED
			return ret
		}
	} else {
		//RDB

	}
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
					fmt.Println("Unrecoverable error reading the append only file %s: %s", filename, err)
					ret = AOF_FAILED
					return ret
				}
			}
			if line[0] == '#' {
				continue
			}
			if line[0] != '*' {
				fmt.Println("Bad file format reading the append only file %s:",
					"make a backup of your AOF file, then use ./redis-check-aof --fix <filename.manifest>", filename)
			}
			argc, _ := strconv.Atoi(string(line[1:]))
			if argc < 1 {
				fmt.Println("Bad file format reading the append only file %s:",
					"make a backup of your AOF file, then use ./redis-check-aof --fix <filename.manifest>", filename)
			}
			if argc > int(SIZE_MAX) {
				fmt.Println("Bad file format reading the append only file %s:",
					"make a backup of your AOF file, then use ./redis-check-aof --fix <filename.manifest>", filename)
			}
			e := entry.NewEntry()
			argv := []string{}

			for j := 0; j < argc; j++ {
				line, err := reader.ReadString('\n')
				if err != nil || line[0] != '$' {
					if err == io.EOF {
						fmt.Println("Unrecoverable error reading the append only file %s: %s", filename, err)
						ret = AOF_FAILED
						return ret
					} else {
						fmt.Println("Bad file format reading the append only file %s:",
							"make a backup of your AOF file, then use ./redis-check-aof --fix <filename.manifest>", filename)
					}
				}
				len, _ := strconv.ParseInt(line[1:], 10, 64)

				argstring := make([]byte, len)
				_, err = io.ReadFull(fp, argstring)
				if err != nil {
					fargc = j
					fmt.Println("Unrecoverable error reading the append only file %s: %s", filename, err)
					ret = AOF_FAILED
					return ret
				}
				//argv[j] = createObject(OBJ_STRING, argsds) //这里没写
				argv = append(argv, string(argstring))
				CRLF := make([]byte, 2)
				_, err = io.ReadFull(fp, CRLF)
				if err != nil {
					fargc = j + 1 // Free up to j.
					fmt.Println("Unrecoverable error reading the append only file %s: %s", filename, err)
					ret = AOF_FAILED
					return ret
				}
			}
			for _, value := range argv {
				ok := commands.lookupCommand(value) //包未导出 而且键值对判定问题
				if ok == 0 {
					fmt.Println("unknown command. argv=%v", argv)
					ret = AOF_FAILED
					return ret
				}
			}
			for _, value := range argv {
				e.Argv = append(e.Argv, value)
			}
			rw := writer.NewRedisWriter(address, username, password, isTls)
			rw.Write(e) //是否go携程

		}

	}
}
func aofFileExist(filename string) int {
	filepath := makePath(server.aof_dirname, filename)
	ret := fileExist(filepath)
	return ret
}

// 这里的time没写完·
func getAppendOnlyFileSize(filename string, status *int) int64 {
	var size int64

	aofFilepath := makePath(server.aof_dirname, filename)
	start := time.Now()

	stat, err := os.Stat(aofFilepath)
	if err != nil {
		if status != nil {
			if os.IsNotExist(err) {
				*status = AOF_NOT_EXIST
			} else {
				*status = AOF_OPEN_ERR
			}
		}
		log.Fatal("Unable to obtain the AOF file %s length. stat: %s", filename, err.Error())
		size = 0
	} else {
		if status != nil {
			*status = AOF_OK
		}
		size = stat.Size()
	}

	latency := time.Since(start).Milliseconds()
	latencyAddSampleIfNeeded("aof-fstat", latency) //延迟监控
	/*可以看到，条件部分包括两个判断：首先检查 server.latency_monitor_threshold 是否为非零值（即已配置阈值），
		然后判断 (var) 是否大于等于 server.latency_monitor_threshold。只有当这两个条件都为真时，才会调用 latencyAddSample 函数。

	这段代码的目的是确保只有当给定的 var 值超过了配置的阈值时，才会将样本添加到延迟监控中。*/

	return size
}

func getBaseAndIncrAppendOnlyFilesSize(am *aofManifest, status *int) int64 {
	var size int64
	var ln *listNode
	var li *listIter
	if am.baseAofInfo != nil {
		if am.baseAofInfo.aofFileType != AofManifestFileTypeBase {
			log.Fatal("File type must be base.")
		}
		size += getAppendOnlyFileSize(am.baseAofInfo.fileName, status)
		if *status != AOF_OK {
			return 0
		}
	}

	am.incrAofList.ListsRewind(li)
	ln = listNext(li)
	for ln != nil {
		ai := ln.value.(*aofInfo)
		if ai.aofFileType != AofManifestTypeIncr {
			log.Fatal("File type must be Incr")
		}
		size += getAppendOnlyFileSize(ai.fileName, status)
		if *status != AOF_OK {
			return 0
		}
	}
	return size
}

func getBaseAndIncrAppendOnlyFilesNum(am *aofManifest) int {
	num := 0
	if am.baseAofInfo != nil {
		num++
	}
	if am.incrAofList != nil {
		num += int(am.incrAofList.len)
	}
	return num
}

func loadAppendOnlyFile(am *aofManifest) int {
	if am == nil {
		log.Fatalf("aofManifest is null")
	}
	status := AOF_OK
	ret := AOF_OK
	var start int64
	var totalSize int64 = 0
	var baseSize int64 = 0
	var aofName string
	var totalNum, aofNum, lastFile int

	if aofFileExist(server.aof_filename) == 1 {
		if dirExists(server.aof_dirname) == 0 ||
			(am.baseAofInfo == nil && am.incrAofList.len == 0) ||
			(am.baseAofInfo != nil && am.incrAofList.len == 0 &&
				strings.Compare(am.baseAofInfo.fileName, server.aof_filename) == 0 && aofFileExist(server.aof_filename) == 0) {
			log.Fatalf("This is an old version of the AOF file") //原本这里是要升级
		}
	}

	if am.baseAofInfo == nil && am.incrAofList == nil {
		return AOF_NOT_EXIST
	}

	totalNum = getBaseAndIncrAppendOnlyFilesNum(am)
	if totalNum <= 0 {
		log.Fatalf("Assertion failed: IncrAppendOnlyFilestotalNum > 0")
	}

	totalSize = getBaseAndIncrAppendOnlyFilesSize(am, &status)
	if status != AOF_OK {
		if status == AOF_NOT_EXIST {
			status = AOF_FAILED
		}
		return status
	} else if totalSize == 0 {
		return AOF_EMPTY
	}

	startLoading(totalSize, RDBFLAGS_AOF_PREAMBLE, 0) //这个嗲放有问题
	//这段代码是一个函数 startLoading 的实现，用于在全局状态中标记正在进行加载，并设置用于提供加载统计信息的字段。

	if am.baseAofInfo != nil {
		if am.baseAofInfo.aofFileType != AofManifestFileTypeBase {
			aofName = string(am.baseAofInfo.fileName)
			updateLoadingFileName(aofName)
			baseSize = getAppendOnlyFileSize(aofName, nil)
			lastFile = totalNum
			start = ustime()
			ret = loadSingleAppendOnlyFile(aofName)
			if ret == AOF_OK || (ret == AOF_TRUNCATED && lastFile == 1) {
				fmt.Println("DB loaded from base file %s: %.3f seconds", aofName, float64(ustime()-start)/1000000)
			}

			if ret == AOF_EMPTY {
				ret = AOF_OK
			}

			if ret == AOF_TRUNCATED && lastFile == 0 {
				ret = AOF_FAILED
				fmt.Println("Fatal error: the truncated file is not the last file")
			}

			if ret == AOF_OPEN_ERR || ret == AOF_FAILED {
				stopLoading(ret == AOF_OK || ret == AOF_TRUNCATED)
				return ret
			}
		}
	}

	if am.incrAofList.len > 0 {
		var ln *listNode
		var li listIter

		am.incrAofList.ListsRewind(&li)
		ln = listNext(&li)
		for ln != nil {
			ai := ln.value.(*aofInfo)
			if ai.aofFileType != AofManifestTypeIncr {
				log.Fatalf("The manifestType must be Incr")
			}
			aofName = ai.fileName
			updateLoadingFileName(aofName)
			lastFile = totalNum
			aofNum++
			start = ustime()
			ret = loadSingleAppendOnlyFile(aofName)
			if ret == AOF_OK || (ret == AOF_TRUNCATED && lastFile == 1) {
				fmt.Println("DB loaded from incr file %s: %.3f seconds", aofName, float64(ustime()-start)/1000000)
			}

			if ret == AOF_EMPTY {
				ret = AOF_OK
			}

			if ret == AOF_TRUNCATED && lastFile == 0 {
				ret = AOF_FAILED
				fmt.Println("Fatal error: the truncated file is not the last file")
			}

			if ret == AOF_OPEN_ERR || ret == AOF_FAILED {
				stopLoading(ret == AOF_OK || ret == AOF_TRUNCATED) /*总体而言，这段代码的目的是在加载过程结束时，更新全局状态中的加载相关字段，并触发加载结束事件以通知相关模块。
				具体这些字段和事件的含义和功能可能需要参考完整代码和相关函数的定义才能理解清楚*/
				return ret
			}
		}

	}

	server.aof_current_size = totalSize
	server.aof_rewrite_base_size = baseSize
}
