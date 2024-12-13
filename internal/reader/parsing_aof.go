package reader

import (
	"bufio"
	"bytes"
	"container/list"
	"context"
	"fmt"
	"io"
	"os"
	"path"
	"path/filepath"
	"strconv"
	"strings"
	"time"
	"unicode"

	"RedisShake/internal/aof"
	"RedisShake/internal/entry"
	"RedisShake/internal/log"
)

const (
	AOFManifestFileTypeBase = "b" /* Base File */
	AOFManifestTypeHist     = "h" /* History File */
	AOFManifestTypeIncr     = "i" /* INCR File */
	AOFNotExist             = 1
	AOFOpenErr              = 3
	AOFOk                   = 0
	AOFEmpty                = 2
	AOFFailed               = 4
	AOFTruncated            = 5
	AOFManifestKeyFileName  = "File"
	AOFManifestKeyFileSeq   = "seq"
	AOFManifestKeyFileType  = "type"
)

func Ustime() int64 {
	tv := time.Now()
	ust := int64(tv.UnixNano()) / 1000
	return ust

}

func MakePath(Paths string, FileName string) string {
	return path.Join(Paths, FileName)
}

func StringNeedsRepr(s string) int {
	sLen := len(s)
	point := 0
	for sLen > 0 {
		if s[point] == '\\' || s[point] == '"' || s[point] == '\n' || s[point] == '\r' ||
			s[point] == '\t' || s[point] == '\a' || s[point] == '\b' || !unicode.IsPrint(rune(s[point])) || unicode.IsSpace(rune(s[point])) {
			return 1
		}
		sLen--
		point++
	}

	return 0
}

type INFO struct {
	AOFDirName         string
	AOFUseRDBPreamble  int // TODO:not support parsing rdb preamble
	AOFManifest        *AOFManifest
	AOFFileName        string
	AOFCurrentSize     int64
	AOFRewriteBaseSize int64
	updateLoadingFile  string
	ch                 chan *entry.Entry
}

func (aofInfo *INFO) GetAOFDirName() string {
	return aofInfo.AOFDirName
}

func NewAOFFileInfo(aofFilePath string, ch chan *entry.Entry) *INFO {
	return &INFO{
		AOFDirName:         filepath.Dir(aofFilePath),
		AOFUseRDBPreamble:  0,
		AOFManifest:        nil,
		AOFFileName:        filepath.Base(aofFilePath),
		AOFCurrentSize:     0,
		AOFRewriteBaseSize: 0,
		ch:                 ch,
	}
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
	var p = line
	var Current string
	var vector []string
	argc := 0
	i := 0
	lens := len(p)
	for { //SKIP BLANKS
		for i < lens && unicode.IsSpace(rune(p[i])) {
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
							hexadecimal := (HexDigitToInt(p[i+2]) * 16) + HexDigitToInt(p[i+3])
							Current = Current + fmt.Sprint(hexadecimal)
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
						if i+1 < lens && !unicode.IsSpace(rune(p[i+1])) {
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
						if i+1 < lens && !unicode.IsSpace(rune(p[i+1])) {
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

func StringCatPrintf(s string, fmtStr string, args ...interface{}) string {
	result := fmt.Sprintf(fmtStr, args...)
	if s == "" {
		return result
	} else {
		return s + result
	}
}

func StringCatRepr(s string, p string, length int) string {
	s = s + ("\"")
	for i := 0; i < length; i++ {
		switch p[i] {
		case '\\', '"':
			s = StringCatPrintf(s, "\\%c", p[i])
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

func (aofInfo *INFO) UpdateLoadingFileName(FileName string) {
	aofInfo.updateLoadingFile = FileName
}

// AOFInfo AOF manifest definition
type AOFInfo struct {
	FileName    string
	FileSeq     int64
	AOFFileType string
}

func AOFInfoCreate() *AOFInfo {
	return new(AOFInfo)
}

func AOFInfoFormat(buf string, ai *AOFInfo) string {
	var aofManifestTostring string
	if StringNeedsRepr(ai.FileName) == 1 {
		aofManifestTostring = StringCatRepr("", ai.FileName, len(ai.FileName))
	}
	var ret string
	if aofManifestTostring != "" {
		ret = StringCatPrintf(buf, "%s %s %s %d %s %s\n", AOFManifestKeyFileName, aofManifestTostring, AOFManifestKeyFileSeq, ai.FileSeq, AOFManifestKeyFileType, ai.AOFFileType)
	} else {
		ret = StringCatPrintf(buf, "%s %s %s %d %s %s\n", AOFManifestKeyFileName, ai.FileName, AOFManifestKeyFileSeq, ai.FileSeq, AOFManifestKeyFileType, ai.AOFFileType)
	}
	return ret
}

func PathIsBaseName(Path string) bool {
	return strings.IndexByte(Path, '/') == -1 && strings.IndexByte(Path, '\\') == -1
}

func AOFLoadManifestFromFile(amFilepath string) *AOFManifest {
	var maxSeq int64
	am := AOFManifestCreate()
	fp, err := os.Open(amFilepath)
	if err != nil {
		log.Panicf("Fatal error:can't open the AOF manifest %v for reading: %v", amFilepath, err)
	}
	defer fp.Close()
	var argv []string
	var ai *AOFInfo
	var line string
	lineNum := 0
	fpReader := bufio.NewReader(fp)
	for {
		buf, err := fpReader.ReadString('\n')
		if err != nil {
			if err == io.EOF {
				if lineNum == 0 {
					log.Infof("Found an empty AOF manifest")
					am = nil
					return am
				} else {
					break
				}

			} else {
				log.Infof("Reading the manifest file, at line %d", lineNum)
				log.Infof("Read AOF manifest failed")
				am = nil
				return am

			}
		}

		lineNum++
		if buf[0] == '#' {
			continue
		}
		if !strings.Contains(buf, "\n") {
			log.Infof("Reading the manifest file, at line %d", lineNum)
			log.Infof("The AOF manifest File contains too long line")
			return nil
		}
		line = strings.Trim(buf, " \t\r\n")
		if len(line) == 0 {
			log.Infof("Reading the manifest file, at line %d", lineNum)
			log.Infof("Invalid AOF manifest File format")
			return nil
		}
		argc := 0
		argv, argc = SplitArgs(line)

		if argc < 6 || argc%2 != 0 {
			log.Infof("Reading the manifest file, at line %d", lineNum)
			log.Infof("Invalid AOF manifest File format")
			am = nil
			return am
		}
		ai = AOFInfoCreate()
		for i := 0; i < argc; i += 2 {
			if strings.EqualFold(argv[i], AOFManifestKeyFileName) {
				ai.FileName = argv[i+1]
				if !PathIsBaseName(ai.FileName) {
					log.Infof("Reading the manifest file, at line %d", lineNum)
					log.Panicf("File can't be a path, just a Filename")
				}
			} else if strings.EqualFold(argv[i], AOFManifestKeyFileSeq) {
				ai.FileSeq, _ = strconv.ParseInt(argv[i+1], 10, 64)
			} else if strings.EqualFold(argv[i], AOFManifestKeyFileType) {
				ai.AOFFileType = string(argv[i+1][0])
			}
		}
		if ai.FileName == "" || ai.FileSeq == 0 || ai.AOFFileType == "" {
			log.Infof("Reading the manifest file, at line %d", lineNum)
			log.Panicf("Invalid AOF manifest File format")
		}
		if ai.AOFFileType == AOFManifestFileTypeBase {
			if am.BaseAOFInfo != nil {
				log.Infof("Reading the manifest file, at line %d", lineNum)
				log.Panicf("Found duplicate Base File information")
			}
			am.BaseAOFInfo = ai
			am.CurrBaseFileSeq = ai.FileSeq
		} else if ai.AOFFileType == AOFManifestTypeHist {
			if !strings.Contains(ai.FileName, "base.rdb") {
				am.HistoryList.PushBack(ai)
			}
		} else if ai.AOFFileType == AOFManifestTypeIncr {
			if ai.FileSeq <= maxSeq {
				log.Infof("Reading the manifest file, at line %d", lineNum)
				log.Panicf("Found a non-monotonic sequence number")
			}
			am.incrAOFList.PushBack(ai)
			am.CurrIncrFileSeq = ai.FileSeq
			maxSeq = ai.FileSeq
		} else {
			log.Infof("Reading the manifest file, at line %d", lineNum)
			log.Panicf("Unknown AOF File type")
		}
		ai = nil
	}
	return am
}

type AOFManifest struct {
	BaseAOFInfo     *AOFInfo
	incrAOFList     *list.List
	HistoryList     *list.List
	CurrBaseFileSeq int64
	CurrIncrFileSeq int64
	Dirty           int64
}

func AOFManifestCreate() *AOFManifest {
	am := &AOFManifest{
		incrAOFList: list.New(),
		HistoryList: list.New(),
	}
	return am
}

func GetAOFManifestAsString(am *AOFManifest) string {
	if am == nil {
		panic("am is nil")
	}
	var buf string
	if am.BaseAOFInfo != nil {
		buf = AOFInfoFormat(buf, am.BaseAOFInfo)
	}
	for ln := am.HistoryList.Front(); ln != nil; ln = ln.Next() {
		buf = AOFInfoFormat(buf, ln.Value.(*AOFInfo))
	}
	for ln := am.incrAOFList.Front(); ln != nil; ln = ln.Next() {
		buf = AOFInfoFormat(buf, ln.Value.(*AOFInfo))
	}
	return buf

}

func (aofInfo *INFO) AOFLoadManifestFromDisk() {
	if DirExists(aofInfo.AOFDirName) == 0 {
		log.Infof("The AOF Directory %v doesn't exist", aofInfo.AOFDirName)
		return
	}
	aofInfo.AOFManifest = AOFManifestCreate()
	amFilepath := path.Join(aofInfo.AOFDirName, aofInfo.AOFFileName)
	if FileExist(amFilepath) == 0 {
		log.Infof("The AOF Directory %v doesn't exist", aofInfo.AOFDirName)
		return
	}

	am := AOFLoadManifestFromFile(amFilepath)
	aofInfo.AOFManifest = am
}

func (aofInfo *INFO) GetAOFManifestFileName() string {
	return aofInfo.AOFFileName
}

func (aofInfo *INFO) AOFFileExist(FileName string) int {
	Filepath := path.Join(aofInfo.AOFDirName, FileName)
	ret := FileExist(Filepath)
	return ret
}

func (aofInfo *INFO) GetAppendOnlyFileSize(FileName string, status *int) int64 {
	var size int64

	AOFFilePath := path.Join(aofInfo.AOFDirName, FileName)

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
			*status = AOFOk
		}
		size = stat.Size()
	}
	return size
}

func (aofInfo *INFO) GetBaseAndIncrAppendOnlyFilesSize(am *AOFManifest, status *int) int64 {
	var size int64
	if am.BaseAOFInfo != nil {
		if am.BaseAOFInfo.AOFFileType != AOFManifestFileTypeBase {
			log.Panicf("File type must be Base.")
		}
		size += aofInfo.GetAppendOnlyFileSize(am.BaseAOFInfo.FileName, status)
		if *status != AOFOk {
			return 0
		}
	}

	for ln := am.HistoryList.Front(); ln != nil; ln = ln.Next() {
		ai := ln.Value.(*AOFInfo)
		if ai.AOFFileType != AOFManifestTypeIncr {
			log.Panicf("File type must be Incr")
		}
		size += aofInfo.GetAppendOnlyFileSize(ai.FileName, status)
		if *status != AOFOk {
			return 0
		}
	}
	return size
}

func GetBaseAndIncrAppendOnlyFilesNum(am *AOFManifest) int {
	num := 0
	if am.BaseAOFInfo != nil {
		num++
	}
	if am.incrAOFList != nil {
		num += am.incrAOFList.Len()
	}
	return num
}

func GetHistoryAndIncrAppendOnlyFilesNum(am *AOFManifest) int {
	num := 0
	if am.HistoryList != nil {
		num += am.HistoryList.Len()
	}
	if am.incrAOFList != nil {
		num += am.incrAOFList.Len()
	}
	return num
}

func (aofInfo *INFO) LoadAppendOnlyFile(ctx context.Context, am *AOFManifest, AOFTimeStamp int64) int {
	if am == nil {
		log.Panicf("AOFManifest is null")
	}
	status := AOFOk
	ret := AOFOk
	var start int64
	var totalSize int64
	var BaseSize int64 = 0
	var AOFName string
	var totalNum, AOFNum int

	if am.BaseAOFInfo == nil && am.incrAOFList == nil {
		return AOFNotExist
	}

	totalNum = GetBaseAndIncrAppendOnlyFilesNum(am)
	if totalNum <= 0 {
		log.Panicf("Assertion failed: IncrAppendOnlyFilestotalNum > 0")
	}

	totalSize = aofInfo.GetBaseAndIncrAppendOnlyFilesSize(am, &status)
	if status != AOFOk {
		if status == AOFNotExist {
			status = AOFFailed
		}
		return status
	} else if totalSize == 0 {
		return AOFEmpty
	}

	log.Infof("The AOF File starts loading.")
	if am.BaseAOFInfo != nil {
		if am.BaseAOFInfo.AOFFileType == AOFManifestFileTypeBase {
			AOFName = am.BaseAOFInfo.FileName
			aofInfo.UpdateLoadingFileName(AOFName)
			BaseSize = aofInfo.GetAppendOnlyFileSize(AOFName, nil)
			start = Ustime()
			ret = aofInfo.ParsingSingleAppendOnlyFile(ctx, AOFName, 0) //Currently, RDB files cannot be restored at a point in time.
			if ret == AOFOk || (ret == AOFTruncated) {
				log.Infof("DB loaded from Base File %v: %.3f seconds", AOFName, float64(Ustime()-start)/1000000)
			}
			if ret == AOFEmpty {
				ret = AOFOk
			}
			if ret == AOFOk || ret == AOFTruncated {
				log.Infof("The AOF File was successfully loaded")
			}
			if ret == AOFOpenErr || ret == AOFFailed {
				if ret == AOFOpenErr {
					log.Panicf("There was an error opening the AOF File.")
				} else {
					log.Panicf("Failed to open AOF File.")
				}
				return ret
			}
		}
		totalNum--
	} else {
		totalNum = GetHistoryAndIncrAppendOnlyFilesNum(am)
		log.Infof("The BaseAOF file does not exist. Start loading the HistoryAOF and IncrAOF files.")
		if am.HistoryList.Len() > 0 {
			for ln := am.HistoryList.Front(); ln != nil; ln = ln.Next() {
				ai := ln.Value.(*AOFInfo)
				if ai.AOFFileType != AOFManifestTypeHist {
					log.Panicf("The manifestType must be Hist")
				}
				AOFName = ai.FileName
				aofInfo.UpdateLoadingFileName(AOFName)
				AOFNum++
				start = Ustime()
				ret = aofInfo.ParsingSingleAppendOnlyFile(ctx, AOFName, AOFTimeStamp)
				if ret == AOFOk || (ret == AOFTruncated) {
					log.Infof("DB loaded from History File %v: %.3f seconds", AOFName, float64(Ustime()-start)/1000000)
					return ret
				}
				if ret == AOFEmpty {
					ret = AOFOk
				}
				if ret == AOFOpenErr || ret == AOFFailed {
					if ret == AOFOpenErr {
						log.Panicf("There was an error opening the AOF File.")
					} else {
						log.Infof("Failed to open AOF File.")
					}
					return ret
				}
				totalNum--
			}
		}

	}

	if am.incrAOFList.Len() > 0 {
		for ln := am.incrAOFList.Front(); ln != nil; ln = ln.Next() {
			ai := ln.Value.(*AOFInfo)
			if ai.AOFFileType != AOFManifestTypeIncr {
				log.Panicf("The manifestType must be Incr")
			}
			AOFName = ai.FileName
			aofInfo.UpdateLoadingFileName(AOFName)
			AOFNum++
			start = Ustime()
			ret = aofInfo.ParsingSingleAppendOnlyFile(ctx, AOFName, AOFTimeStamp)
			if ret == AOFOk || (ret == AOFTruncated) {
				log.Infof("DB loaded from incr File %v: %.3f seconds", AOFName, float64(Ustime()-start)/1000000)
				return ret
			}
			if ret == AOFEmpty {
				ret = AOFOk
			}
			if ret == AOFOpenErr || ret == AOFFailed {
				if ret == AOFOpenErr {
					log.Panicf("There was an error opening the AOF File.")
				} else {
					log.Infof("Failed to open AOF File.")
				}
				return ret
			}
			totalNum--
		}
	}
	if totalNum == 0 {
		log.Infof("All AOF files have been sent.")
	} else {
		log.Panicf("There are still %d AOF files that were not successfully sent.", totalNum)
	}
	aofInfo.AOFCurrentSize = totalSize
	aofInfo.AOFRewriteBaseSize = BaseSize

	log.Infof("The AOF File loading end.")
	return ret

}

func (aofInfo *INFO) ParsingSingleAppendOnlyFile(ctx context.Context, FileName string, AOFTimeStamp int64) int {
	AOFFilepath := path.Join(aofInfo.AOFDirName, FileName)
	println(AOFFilepath)
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

		stat, _ := fp.Stat()
		if stat.Size() == 0 {
			return AOFEmpty
		}
	}
	defer fp.Close()
	sig := make([]byte, 5)
	if n, err := fp.Read(sig); err != nil || n != 5 || !bytes.Equal(sig, []byte("REDIS")) {
		if _, err := fp.Seek(0, 0); err != nil {
			log.Infof("Unrecoverable error reading the append only File %v: %v", FileName, err)
			return AOFFailed
		}
	} else { //Skipped RDB checksum and has not been processed yet.
		log.Infof("Reading RDB Base File on AOF loading...")
		rdbOpt := RdbReaderOptions{Filepath: AOFFilepath}
		ldRDB := NewRDBReader(&rdbOpt)
		ldRDB.StartRead(ctx)
		return AOFOk
	}
	// load single aof file
	aofSingleReader := aof.NewLoader(MakePath(aofInfo.AOFDirName, FileName), aofInfo.ch)
	return aofSingleReader.LoadSingleAppendOnlyFile(ctx, AOFTimeStamp)
}
