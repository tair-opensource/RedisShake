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

	"github.com/alibaba/RedisShake/internal/log"
	"github.com/alibaba/RedisShake/internal/rdb"
)

type AofFileType string

var line int64 = 1
var fp *os.File
var pos int64

const (
	aofResp                    AofFileType = "AOF_RESP"
	aofRdbPreamble             AofFileType = "AOF_RDB_PREAMBLE"
	aofMultiPart               AofFileType = "AOF_MULTI_PART"
	rdbCheckMode                           = 1
	ManifestMaxLine                        = 1024
	AofCheckOk                             = 0
	AofCheckEmpty                          = 1
	AofCheckTruncated                      = 2
	AofCheckTimeStampTruncated             = 3
	toTimestamp                            = 0
	AofManifestKeyFileName                 = "file"
	AofManifestKeyFileSeq                  = "seq"
	AofManifestKeyFileType                 = "type"
	AofAnnoTationLineMaxLen                = 1024
)

// check 里面的主函数
func CheckAofMain(aofFilePath string) (checkResult bool, fileType AofFileType, err error) {
	var filepaths string
	var tempFilepath [1025]byte
	var dirpath string
	fix := 1
	filepaths = aofFilePath

	copy(tempFilepath[:], filepaths)
	dirpath = filepath.Dir(string(tempFilepath[:]))

	fileType = GetInputAofFileType(filepaths)
	switch fileType {
	case "AOF_MULTI_PART":
		CheckMultipartAof(dirpath, filepaths, fix)
	case "AOF_RESP":
		CheckOldStyleAof(filepaths, fix, false)
	case "AOF_RDB_PREAMBLE":
		CheckOldStyleAof(filepaths, fix, true)
	}
	return true, aofMultiPart, nil
}

func GetInputAofFileType(aofFilepath string) AofFileType {
	if FilelsManifest(aofFilepath) {
		return "AOF_MULTI_PART"
	} else if FileIsRDB(aofFilepath) {
		return "AOF_RDB_PREAMBLE"
	} else {
		return "AOF_RESP"
	}
}

func FilelsManifest(aofFilepath string) bool {
	var is_manifest bool = false
	fp, err := os.Open(aofFilepath)
	if err != nil {
		log.Infof("Cannot open file %v:%v\n", aofFilepath, err.Error())
		os.Exit(1)
	}
	sb, err := os.Stat(aofFilepath)
	if err != nil {
		log.Infof("cannot stat file: %v\n", aofFilepath)
		os.Exit(1)
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
				log.Panicf("cannot read file: %v\n", aofFilepath)
			}
		}
		if lines[0] == '#' {
			continue
		} else if lines[:4] == "file" {
			is_manifest = true
		}
	}
	fp.Close()
	return is_manifest
}

func FileIsRDB(aofFilepath string) bool {
	fp, err := os.Open(aofFilepath)
	if err != nil {
		log.Panicf("Cannot open file %v:%v\n", aofFilepath, err.Error())
	}
	sb, err := os.Stat(aofFilepath)
	if err != nil {
		log.Panicf("cannot stat file: %v\n", aofFilepath)
	}
	size := sb.Size()
	if size == 0 {
		fp.Close()
		return false
	}
	if size >= 8 {
		sig := make([]byte, 5)
		_, err := fp.Read(sig)
		if err == nil && string(sig) == "REDIS" {
			fp.Close()
			return true
		}
	}
	fp.Close()
	return false
}

func PrintAofStyle(ret int, aofFileName string, aofType string) {
	switch ret {
	case AofCheckOk:
		log.Infof("%v %v is valid\n", aofType, aofFileName)
	case AofCheckEmpty:
		log.Infof("%v %v is empty\n", aofType, aofFileName)
	case AofCheckTimeStampTruncated:
		log.Infof("Successfully truncated AOF %v to timestamp %d\n", aofFileName, toTimestamp)
	case AofCheckTruncated:
		log.Infof("Successfully truncated AOF %v\n", aofFileName)
	}

}

func MakePath(paths string, filename string) string {
	return path.Join(paths, filename)
}

func PathIsBaseName(path string) bool {
	return strings.IndexByte(path, '/') == -1 && strings.IndexByte(path, '\\') == -1
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

	// Increase length to also consume \r\n
	len += 2
	data := make([]byte, len)
	if ReadBytes(rd, &data, len) == 0 {
		return 0
	}

	if ConsumeNewline(data[len-2:]) == 0 {
		return 0
	}

	*target = string(data[:len-2])
	return 1
}

func ReadBytes(rd *bufio.Reader, target *[]byte, length int64) int {
	var real int64
	n, err := rd.Read(*target)
	real = int64(n)
	if err != nil || real != length {
		log.Infof("Expected to read %d bytes, got %d bytes\n", length, real)
		return 0
	}
	pos += real
	return 1
}

func ConsumeNewline(buf []byte) int {
	if buf[0] != '\r' || buf[1] != '\n' {
		log.Infof("Expected \\r\\n, got: %02x%02x", buf[0], buf[1])
		return 0
	}
	line += 1
	return 1
}

func ReadLong(rd *bufio.Reader, prefix byte, target *int64) int {

	var err error
	var value int64
	buf, err := rd.ReadBytes('\n')
	if err != nil {
		log.Infof("Failed to read line from file")
		return 0
	}
	pos += int64(len(buf))
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
	line += 1
	return 1

}

func AofLoadManifestFromFile(am_filepath string) *aofManifest {
	var maxseq int64
	am := AofManifestcreate()
	fp, err := os.Open(am_filepath)
	if err != nil {
		log.Panicf("Fatal error:can't open the AOF manifest %v for reading: %v", am_filepath, err)
	}
	var argv []string
	var ai *aofInfo
	var line string
	linenum := 0
	reader := bufio.NewReader(fp)
	for {
		buf, err := reader.ReadString('\n')
		if err != nil {
			if err == io.EOF {
				if linenum == 0 {
					log.Panicf("Found an empty AOF manifest")
				} else {
					break
				}

			} else {
				log.Panicf("Read AOF manifest failed")
			}
		}

		linenum++
		if buf[0] == '#' {
			continue
		}
		if !strings.Contains(buf, "\n") {
			log.Panicf("The AOF manifest file contains too long line")
		}
		line = strings.Trim(buf, " \t\r\n")
		if len(line) == 0 {
			log.Panicf("Invalid AOF manifest file format")
		}
		argc := 0
		argv, argc = SplitArgs(line)

		if argc < 6 || argc%2 != 0 {
			log.Panicf("Invalid AOF manifest file format")
		}
		ai = AofInfoCreate()
		for i := 0; i < argc; i += 2 {
			if strings.EqualFold(argv[i], AofManifestKeyFileName) {
				ai.fileName = string(argv[i+1])
				if !PathIsBaseName(string(ai.fileName)) {
					log.Panicf("File can't be a path, just a filename")
				}
			} else if strings.EqualFold(argv[i], AofManifestKeyFileSeq) {
				ai.fileSeq, _ = strconv.ParseInt(argv[i+1], 10, 64)
			} else if strings.EqualFold(argv[i], AofManifestKeyFileType) {
				ai.aofFileType = string(argv[i+1][0])
			}
		}
		if ai.fileName == "" || ai.fileSeq == 0 || ai.aofFileType == "" {
			log.Panicf("Invalid AOF manifest file format")
		}
		if ai.aofFileType == AofManifestFileTypeBase {
			if am.baseAofInfo != nil {
				log.Panicf("Found duplicate base file information")
			}
			am.baseAofInfo = ai
			am.currBaseFileSeq = ai.fileSeq
		} else if ai.aofFileType == AofManifestTypeHist {
			am.historyList = ListAddNodeTail(am.historyList, ai)
		} else if ai.aofFileType == AofManifestTypeIncr {
			if ai.fileSeq <= maxseq {
				log.Panicf("Found a non-monotonic sequence number")
			}
			am.incrAofList = ListAddNodeTail(am.historyList, ai)
			am.currIncrFIleSeq = ai.fileSeq
			maxseq = ai.fileSeq
		} else {
			log.Panicf("Unknown AOF file type")
		}
		line = " "
		ai = nil
	}
	fp.Close()
	return am
}

func ProcessRESP(rd *bufio.Reader, filename string, outMulti *int) int {
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
					log.Infof("Unexpected MULTI in AOF %v", filename)
					return 0
				}
				(*outMulti)++
			} else if strings.EqualFold(str, "exec") {
				(*outMulti)--
				if (*outMulti) != 0 {
					log.Infof("Unexpected EXEC in AOF %v", filename)
					return 0
				}
			}
		}
	}

	return 1
}

func ProcessAnnotations(rd *bufio.Reader, filename string, lastFile bool) int {
	buf, _, err := rd.ReadLine()
	if err != nil {
		log.Panicf("Failed to read annotations from AOF %v, aborting...\n", filename)
	}
	pos += int64(len(buf)) + 2

	if toTimestamp != 0 && strings.HasPrefix(string(buf), "TS:") {
		var ts int64
		ts, err = strconv.ParseInt(strings.TrimPrefix(string(buf), "TS:"), 10, 64)
		if err != nil {
			log.Panicf("Invalid timestamp annotation")
		}

		if ts <= toTimestamp {
			return 1
		}

		if pos == 0 {
			log.Panicf("AOF %v has nothing before timestamp %d, aborting...\n", filename, toTimestamp)
		}

		if !lastFile {
			log.Infof("Failed to truncate AOF %v to timestamp %d to offset %d because it is not the last file.\n", filename, toTimestamp, pos)
			log.Panicf("If you insist, please delete all files after this file according to the manifest file and delete the corresponding records in manifest file manually. Then re-run redis-check-aof.")
		}

		// Truncate remaining AOF if exceeding 'toTimestamp'
		if err := fp.Truncate(pos); err != nil {
			log.Panicf("Failed to truncate AOF %v to timestamp %d\n", filename, toTimestamp)
		} else {

			return 0
		}
	}

	return 1
}

func CheckMultipartAof(dirpath string, manifestFilepath string, fix int) {
	totalNum := 0
	aofNum := 0
	var ret int
	am := AofLoadManifestFromFile(manifestFilepath)
	if am.baseAofInfo != nil {
		totalNum++
	}
	if am.incrAofList != nil {
		totalNum += int(am.incrAofList.len)
	}
	if am.baseAofInfo != nil {
		aofFilename := am.baseAofInfo.fileName
		aofFilepath := MakePath(dirpath, aofFilename)
		lastFile := (aofNum + 1) == totalNum
		aofPreable := FileIsRDB(aofFilepath)
		if aofPreable {
			log.Infof("Start to check BASE AOF (RDB format).\n")
		} else {
			log.Infof("Start to check BASE AOF (AOF format).\n")
		}
		ret = CheckSingleAof(aofFilename, aofFilepath, lastFile, fix, aofPreable)
		PrintAofStyle(ret, aofFilename, "BASE AOF")

	}
	if am.incrAofList.len != 0 {
		log.Infof("start to check INCR INCR files.\n")
		var ln *listNode
		ln = am.incrAofList.head
		for ln != nil {
			ai := ln.value.(*aofInfo)
			aofFilename := ai.fileName
			aofFilepath := MakePath(dirpath, aofFilename)
			lastFile := (aofNum + 1) == totalNum
			ret = CheckSingleAof(aofFilename, aofFilepath, lastFile, fix, false)
			PrintAofStyle(ret, aofFilename, "INCR AOF")
			ln = ln.next
		}
	}

	log.Infof("All AOF files and manifest are vaild")
}

func CheckOldStyleAof(aofFilepath string, fix int, preamble bool) {
	log.Infof("Start checking Old-Style AOF\n")
	var ret = CheckSingleAof(aofFilepath, aofFilepath, true, fix, preamble)
	PrintAofStyle(ret, aofFilepath, "AOF")

}
func CheckSingleAof(aofFilename, aofFilepath string, lastFile bool, fix int, preamble bool) int {
	var rdbpos int64 = 0
	multi := 0
	pos = 0
	buf := make([]byte, 1)
	var err error
	fp, err = os.OpenFile(aofFilepath, os.O_RDWR, 0666)
	if err != nil {
		log.Panicf("Cannot open file %v:%v,aborting...\n", aofFilepath, err)
	}
	sb, err := fp.Stat()
	if err != nil {
		log.Panicf("Cannot stat file: %v,aborting...\n", aofFilename)
	}
	size := sb.Size()
	if size == 0 {
		return AofCheckEmpty
	}
	rd := bufio.NewReader(fp)
	if preamble {

		rdbpos = rdb.RedisCheckRDBMain(aofFilepath, fp)
		if rdbpos == -1 {
			log.Panicf("RDB preamble of AOF file is not sane, aborting.\n")
		} else {
			log.Infof("RDB preamble is OK, proceeding with AOF tail...\n")
			_, err = fp.Seek(rdbpos, io.SeekStart)
			if err != nil {

				log.Panicf(("Failed to seek in AOF %v: %v"), aofFilename, err)
			}
			pos = rdbpos
		}
	}

	for {

		if _, err := rd.Read(buf); err != nil {

			if err == io.EOF {

				break
			}
			log.Panicf("Failed to read from AOF %v, aborting...\n", aofFilename)
		}
		pos += int64(len(buf))
		if buf[0] == '#' {
			if ProcessAnnotations(rd, aofFilepath, lastFile) == 0 {
				fp.Close()
				return AofCheckTimeStampTruncated
			}
		} else if buf[0] == '*' {
			if ProcessRESP(rd, aofFilepath, &multi) == 0 {
				break
			}
		} else {
			log.Infof("AOF %v format error\n", aofFilename)
			break
		}
	}

	diff := size - pos
	if diff == 0 && toTimestamp == 1 {
		log.Infof("Truncate nothing in AOF %v to timestamp %d\n", aofFilename, toTimestamp)
		return AofCheckOk
	}
	log.Infof("AOF analyzed: filename=%v, size=%d, ok_up_to=%d, ok_up_to_line=%d, diff=%d\n", aofFilename, size, pos, line, diff)
	if diff > 0 {
		if fix == 1 {
			if !lastFile {
				log.Panicf("Failed to truncate AOF %v because it is not the last file\n", aofFilename)
				os.Exit(1)
			}

			fmt.Printf("this will shrink the AOF %v from %d bytes,with %d bytes,to %d bytes\n", aofFilename, size, diff, pos)
			fmt.Print("Continue? [y/N]: ")
			reader := bufio.NewReader(os.Stdin)
			input, err := reader.ReadString('\n')
			if err != nil || strings.ToLower(string(input[0])) != "y" {
				log.Panicf("Aborting...")

			}

			if err := fp.Truncate(pos); err != nil {
				log.Panicf("Failed to truncate AOF %v\n", aofFilename)

			} else {
				return AofCheckTruncated
			}
		} else {
			log.Panicf("AOF %v is not valid.Use the --fix potion to try fixing it.\n", aofFilename)
		}
	}
	fp.Close()

	return AofCheckOk
}
