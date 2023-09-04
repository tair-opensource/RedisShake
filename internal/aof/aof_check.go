package aof

import (
	"bufio"
	"fmt"
	"io"

	"math"
	"os"
	"path"
	"path/filepath"
	"strconv"
	"strings"

	"github.com/alibaba/RedisShake/internal/config"
	"github.com/alibaba/RedisShake/internal/log"
	"github.com/alibaba/RedisShake/internal/rdb"
)

type AOFFileType string

const (
	AOFResp                    AOFFileType = "AOF_RESP"
	AOFRdbPreamble             AOFFileType = "AOF_RDB_PREAMBLE"
	AOFMultiPart               AOFFileType = "AOF_MULTI_PART"
	ManifestMaxLine                        = 1024
	AOFCheckOk                             = 0
	AOFCheckEmpty                          = 1
	AOFCheckTruncated                      = 2
	AOFCheckTimeStampTruncated             = 3
	AOFManifestKeyFileName                 = "File"
	AOFManifestKeyFileSeq                  = "seq"
	AOFManifestKeyFileType                 = "type"
	AOFAnnoTationLineMaxLen                = 1024
)

type CheckAOFINFOF struct {
	line        int64
	fp          *os.File
	pos         int64
	toTimestamp int64
}

func NewCheckAOFINFOF() *CheckAOFINFOF {
	return &CheckAOFINFOF{
		line:        0,
		fp:          nil,
		pos:         0,
		toTimestamp: 0,
	}
}

var CheckAOFInfof = NewCheckAOFINFOF()

// check 里面的主函数
func CheckAOFMain(AOFFilePath string) (checkResult bool, FileType AOFFileType, err error) {
	var Filepaths string
	var dirpath string
	fix := 1
	Filepaths = AOFFilePath
	dirpath = filepath.Dir(string(Filepaths))

	FileType = GetInputAOFFileType(Filepaths)
	switch FileType {
	case "AOF_MULTI_PART":
		CheckMultipartAOF(dirpath, Filepaths, fix)
	case "AOF_RESP":
		CheckOldStyleAOF(Filepaths, fix, false)
	case "AOF_RDB_PREAMBLE":
		CheckOldStyleAOF(Filepaths, fix, true)
	}
	return true, AOFMultiPart, nil
}

func GetInputAOFFileType(AOFFilePath string) AOFFileType {
	if FilelsManifest(AOFFilePath) {
		return "AOF_MULTI_PART"
	} else if FileIsRDB(AOFFilePath) {
		return "AOF_RDB_PREAMBLE"
	} else {
		return "AOF_RESP"
	}
}

func FilelsManifest(AOFFilePath string) bool {
	var is_manifest bool = false
	log.Infof("FIleLsMainifest:%v", AOFFilePath)
	fp, err := os.Open(AOFFilePath)
	if err != nil {
		log.Panicf("Cannot open File %v:%v\n", AOFFilePath, err.Error())
	}
	sb, err := os.Stat(AOFFilePath)
	if err != nil {
		log.Panicf("cannot stat File: %v\n", AOFFilePath)
	}
	size := sb.Size()
	if size == 0 {
		fp.Close()
		return false
	}
	reader := bufio.NewReader(fp)
	for {
		lines, err := reader.ReadString('\n')
		if err != nil {
			if err == io.EOF {
				break
			} else {
				log.Panicf("cannot read File: %v\n", AOFFilePath)
			}
		}
		if lines[0] == '#' || len(lines) < 4 {
			continue
		} else if lines[:4] == "file" {
			is_manifest = true
		}
	}
	fp.Close()
	return is_manifest
}

func FileIsRDB(AOFFilePath string) bool {
	fp, err := os.Open(AOFFilePath)

	if err != nil {
		log.Panicf("Cannot open File %v:%v\n", AOFFilePath, err.Error())
	}

	defer fp.Close()
	sb, err := os.Stat(AOFFilePath)
	if err != nil {
		log.Panicf("cannot stat File: %v\n", AOFFilePath)
	}
	size := sb.Size()
	if size == 0 {
		return false
	}
	if size >= 8 {
		sig := make([]byte, 5)
		_, err := fp.Read(sig)
		if err == nil && string(sig) == "REDIS" {
			return true
		}
	}
	return false
}

func OutPutAOFStyle(ret int, AOFFileName string, AOFType string) {
	switch ret {
	case AOFCheckOk:
		log.Infof("%v %v is valid\n", AOFType, AOFFileName)
	case AOFCheckEmpty:
		log.Infof("%v %v is empty\n", AOFType, AOFFileName)
	case AOFCheckTimeStampTruncated:
		log.Infof("Successfully truncated AOF %v to timestamp %d\n", AOFFileName, CheckAOFInfof.toTimestamp)
	case AOFCheckTruncated:
		log.Infof("Successfully truncated AOF %v\n", AOFFileName)
	}

}

func MakePath(Paths string, FileName string) string {
	return path.Join(Paths, FileName)
}

func PathIsBaseName(Path string) bool {
	return strings.IndexByte(Path, '/') == -1 && strings.IndexByte(Path, '\\') == -1
}

func ReadArgc(rd *bufio.Reader, target *int64) int {
	return ReadLong(rd, ' ', target)
}

func ReadString(rd *bufio.Reader, target *string) int {
	var len int64
	*target = ""
	if ReadLong(rd, '$', &len) == 0 {
		return 0
	}

	if len < 0 || len > math.MaxInt64-2 {
		log.Infof("Expected to read string of %d bytes, which is not in the suitable range\n", len)
		return 0
	}
	len += 2
	// Increase length to also consume \r\n
	data := make([]byte, len)
	if ReadBytes(rd, &data, len) == 0 {
		return 0
	}

	if ConsumeNewline(data[len-2:]) == 0 {
		return 0
	}
	data = data[:len-2] //\r\n
	*target = string(data)
	return 1
}

func ReadBytes(rd *bufio.Reader, target *[]byte, length int64) int {
	var err error
	*target, err = rd.ReadBytes('\n')
	if err != nil || (*target)[length-1] != '\n' {
		log.Infof("AOF format error:%s", *target)
		return 0
	}
	CheckAOFInfof.pos += length
	return 1
}

func ConsumeNewline(buf []byte) int {
	if buf[0] != '\r' || buf[1] != '\n' {
		log.Infof("Expected \\r\\n, got: %02x%02x", buf[0], buf[1])
		return 0
	}
	CheckAOFInfof.line += 1
	return 1
}

func ReadLong(rd *bufio.Reader, prefix byte, target *int64) int {

	var err error
	var value int64
	buf, err := rd.ReadBytes('\n')
	if err != nil {
		log.Infof("Failed to read line from File")
		return 0
	}
	CheckAOFInfof.pos += int64(len(buf))
	if prefix != ' ' {
		if buf[0] != prefix {
			log.Infof("Expected prefix '%c', got: '%c'\n", prefix, buf[0])
			return 0
		}
		value, err = strconv.ParseInt(string(buf[1:len(buf)-2]), 10, 64) //Removed line breaks\r\n
		if err != nil {
			log.Infof("Failed to parse long value")
			return 0
		}
	} else {
		value, err = strconv.ParseInt(string(buf[0:len(buf)-2]), 10, 64) //Removed line breaks\r\n
		if err != nil {
			log.Infof("Failed to parse long value")
			return 0
		}
	}
	*target = value
	CheckAOFInfof.line += 1
	return 1

}

func AOFLoadManifestFromFile(am_Filepath string) *AOFManifest {
	var maxseq int64
	am := AOFManifestcreate()
	fp, err := os.Open(am_Filepath)
	if err != nil {
		log.Panicf("Fatal error:can't open the AOF manifest %v for reading: %v", am_Filepath, err)
	}
	var argv []string
	var ai *AOFInfo
	var line string
	linenum := 0
	reader := bufio.NewReader(fp)
	for {
		buf, err := reader.ReadString('\n')
		if err != nil {
			if err == io.EOF {
				if linenum == 0 {
					log.Infof("Found an empty AOF manifest")
					am = nil
					return am
				} else {
					break
				}

			} else {
				log.Infof("Read AOF manifest failed")
				am = nil
				return am

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
			am = nil
			return am
		}
		ai = AOFInfoCreate()
		for i := 0; i < argc; i += 2 {
			if strings.EqualFold(argv[i], AOFManifestKeyFileName) {
				ai.FileName = string(argv[i+1])
				if !PathIsBaseName(string(ai.FileName)) {
					log.Panicf("File can't be a path, just a Filename")
				}
			} else if strings.EqualFold(argv[i], AOFManifestKeyFileSeq) {
				ai.FileSeq, _ = strconv.ParseInt(argv[i+1], 10, 64)
			} else if strings.EqualFold(argv[i], AOFManifestKeyFileType) {
				ai.AOFFileType = string(argv[i+1][0])
			}
		}
		if ai.FileName == "" || ai.FileSeq == 0 || ai.AOFFileType == "" {
			log.Panicf("Invalid AOF manifest File format")
		}
		if ai.AOFFileType == AOFManifestFileTypeBase {
			if am.BaseAOFInfo != nil {
				log.Panicf("Found duplicate Base File information")
			}
			am.BaseAOFInfo = ai
			am.CurrBaseFileSeq = ai.FileSeq
		} else if ai.AOFFileType == AOFManifestTypeHist {
			am.HistoryList = ListAddNodeTail(am.HistoryList, ai)
		} else if ai.AOFFileType == AOFManifestTypeIncr {
			if ai.FileSeq <= maxseq {
				log.Panicf("Found a non-monotonic sequence number")
			}
			am.incrAOFList = ListAddNodeTail(am.HistoryList, ai)
			am.CurrIncrFileSeq = ai.FileSeq
			maxseq = ai.FileSeq
		} else {
			log.Panicf("Unknown AOF File type")
		}
		line = " "
		ai = nil
	}
	fp.Close()
	return am
}

func ProcessRESP(rd *bufio.Reader, Filename string, outMulti *int) int {
	var argc int64
	var str string

	if ReadArgc(rd, &argc) == 0 {
		return 0
	}

	for i := int64(0); i < argc; i++ {
		if ReadString(rd, &str) == 0 {
			return 0
		}
		if i == 0 {
			if strings.EqualFold(str, "multi") {
				if (*outMulti) != 0 {
					log.Infof("Unexpected MULTI in AOF %v", Filename)
					return 0
				}
				(*outMulti)++
			} else if strings.EqualFold(str, "exec") {
				(*outMulti)--
				if (*outMulti) != 0 {
					log.Infof("Unexpected EXEC in AOF %v", Filename)
					return 0
				}
			}
		}
	}

	return 1
}

func ProcessAnnotations(rd *bufio.Reader, Filename string, lastFile bool) int {
	buf, _, err := rd.ReadLine()
	if err != nil {
		log.Panicf("Failed to read annotations from AOF %v, aborting...\n", Filename)
	}
	if CheckAOFInfof.toTimestamp != 0 && strings.HasPrefix(string(buf), "TS:") {
		var ts int64
		ts, err = strconv.ParseInt(strings.TrimPrefix(string(buf), "TS:"), 10, 64)
		if err != nil {
			log.Panicf("Invalid timestamp annotation")
		}

		if ts <= CheckAOFInfof.toTimestamp {
			CheckAOFInfof.pos += int64(len(buf)) + 2
			return 1
		}

		if CheckAOFInfof.pos == 0 {
			log.Panicf("AOF %v has nothing before timestamp %d, aborting...\n", Filename, CheckAOFInfof.toTimestamp)
		}

		if !lastFile {
			log.Infof("Failed to truncate AOF %v to timestamp %d to offset %d because it is not the last File.\n", Filename, CheckAOFInfof.toTimestamp, CheckAOFInfof.pos)
			log.Panicf("If you insist, please delete all Files after this File according to the manifest File and delete the corresponding records in manifest File manually. Then re-run redis-check-AOF.")
		}

		// Truncate remaining AOF if exceeding 'toTimestamp'
		/*if err := CheckAOFInfof.fp.Truncate(CheckAOFInfof.pos); err != nil {
			log.Panicf("Failed to truncate AOF %v to timestamp %d\n", Filename, CheckAOFInfof.toTimestamp)
		} else {*/
		//CheckAOFInfof.pos += int64(len(buf)) + 2
		return 0
		//}
	}
	CheckAOFInfof.pos += int64(len(buf)) + 2
	return 1
}

func CheckMultipartAOF(DirPath string, ManifestFilePath string, fix int) {
	totalNum := 0
	AOFNum := 0
	var ret int
	am := AOFLoadManifestFromFile(ManifestFilePath)
	if am.BaseAOFInfo != nil {
		totalNum++
	}
	if am.incrAOFList != nil {
		totalNum += int(am.incrAOFList.len)
	}
	if am.BaseAOFInfo != nil {
		AOFFileName := am.BaseAOFInfo.FileName
		AOFFilePath := MakePath(DirPath, AOFFileName)
		AOFNum++
		lastFile := AOFNum == totalNum
		AOFPreable := FileIsRDB(AOFFilePath)
		if AOFPreable {
			log.Infof("Start to check Base AOF (RDB format).\n")
		} else {
			log.Infof("Start to check Base AOF (AOF format).\n")
		}
		ret = CheckSingleAOF(AOFFileName, AOFFilePath, lastFile, fix, AOFPreable)
		OutPutAOFStyle(ret, AOFFileName, "Base AOF")

	}
	if am.incrAOFList.len != 0 {
		log.Infof("start to check INCR INCR Files.\n")
		var ln *listNode
		ln = am.incrAOFList.head
		for ln != nil {
			ai := ln.value.(*AOFInfo)
			AOFFileName := ai.FileName
			AOFFilePath := MakePath(DirPath, AOFFileName)
			AOFNum++
			lastFile := AOFNum == totalNum
			ret = CheckSingleAOF(AOFFileName, AOFFilePath, lastFile, fix, false)
			OutPutAOFStyle(ret, AOFFileName, "INCR AOF")
			ln = ln.next
		}
	}

	log.Infof("All AOF Files and manifest are vaild")
}

func CheckOldStyleAOF(AOFFilePath string, fix int, preamble bool) {
	log.Infof("Start checking Old-Style AOF\n")
	var ret = CheckSingleAOF(AOFFilePath, AOFFilePath, true, fix, preamble)
	OutPutAOFStyle(ret, AOFFilePath, "AOF")

}
func CheckSingleAOF(AOFFileName, AOFFilePath string, lastFile bool, fix int, preamble bool) int {
	var rdbpos int64 = 0
	CheckAOFInfof.toTimestamp = config.Config.Source.AOFTruncateToTimestamp
	multi := 0
	CheckAOFInfof.pos = 0
	buf := make([]byte, 1)
	var err error
	CheckAOFInfof.fp, err = os.OpenFile(AOFFilePath, os.O_RDWR, 0666)
	if err != nil {
		log.Panicf("Cannot open File %v:%v,aborting...\n", AOFFilePath, err)
	}
	sb, err := CheckAOFInfof.fp.Stat()
	if err != nil {
		log.Panicf("Cannot stat File: %v,aborting...\n", AOFFileName)
	}
	size := sb.Size()
	if size == 0 {
		return AOFCheckEmpty
	}
	rd := bufio.NewReader(CheckAOFInfof.fp)
	if preamble {

		rdbpos = rdb.RedisCheckRDBMain(AOFFilePath, CheckAOFInfof.fp)
		rdbpos += 8 //The RDB checksum has not been processed yet.
		if rdbpos == -1 {
			log.Panicf("RDB preamble of AOF File is not sane, aborting.\n")
		} else {
			log.Infof("RDB preamble is OK, proceeding with AOF tail...\n")
			_, err = CheckAOFInfof.fp.Seek(rdbpos, io.SeekStart)
			if err != nil {

				log.Panicf(("Failed to seek in AOF %v: %v"), AOFFileName, err)
			}
			CheckAOFInfof.pos = rdbpos
		}
	}

	for {

		if _, err := rd.Read(buf); err != nil {

			if err == io.EOF {

				break
			}
			log.Panicf("Failed to read from AOF %v, aborting...\n", AOFFileName)
		}
		CheckAOFInfof.pos += int64(len(buf))
		if buf[0] == '#' {
			if ProcessAnnotations(rd, AOFFilePath, lastFile) == 0 {
				CheckAOFInfof.fp.Close()
				return AOFCheckTimeStampTruncated
			}
		} else if buf[0] == '*' {
			if ProcessRESP(rd, AOFFilePath, &multi) == 0 {
				break
			}
		} else {
			log.Infof("AOF %v format error\n", AOFFileName)
			break
		}
	}

	diff := size - CheckAOFInfof.pos
	if diff == 0 && CheckAOFInfof.toTimestamp == 1 {
		log.Infof("Truncate nothing in AOF %v to timestamp %d\n", AOFFileName, CheckAOFInfof.toTimestamp)
		return AOFCheckOk
	}
	log.Infof("AOF analyzed: Filename=%v, size=%d, ok_up_to=%d, ok_up_to_line=%d, diff=%d\n", AOFFileName, size, CheckAOFInfof.pos, CheckAOFInfof.line, diff)
	if diff > 0 {
		if fix == 1 {
			if !lastFile {
				log.Panicf("Failed to truncate AOF %v because it is not the last File\n", AOFFileName)
				os.Exit(1)
			}

			fmt.Printf("this will shrink the AOF %v from %d bytes,with %d bytes,to %d bytes\n", AOFFileName, size, diff, CheckAOFInfof.pos)
			fmt.Print("Continue? [y/N]: ")
			reader := bufio.NewReader(os.Stdin)
			input, err := reader.ReadString('\n')
			if err != nil || strings.ToLower(string(input[0])) != "y" {
				log.Panicf("Aborting...")

			}

			if err := CheckAOFInfof.fp.Truncate(CheckAOFInfof.pos); err != nil {
				log.Panicf("Failed to truncate AOF %v\n", AOFFileName)

			} else {
				return AOFCheckTruncated
			}
		} else {
			log.Panicf("AOF %v is not valid.Use the --fix potion to try fixing it.\n", AOFFileName)
		}
	}
	CheckAOFInfof.fp.Close()

	return AOFCheckOk
}
