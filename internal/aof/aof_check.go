package aof

import (
	"bufio"
	"fmt"
	"io"
	"log"
	"math"
	"os"
	"path"
	"strconv"
	"strings"
	//"time"
)

type AofFileType string

var errors [1044]byte
var line int64 = 1
var epos int64

const (
	aofResp                       AofFileType = "AOF_RESP"
	aofRdbPreamble                AofFileType = "AOF_RDB_PREAMBLE"
	aofMultiPart                  AofFileType = "AOF_MULTI_PART"
	rdbCheckMode                              = 1
	MANIFEST_MAX_LINE                         = 1024
	AOF_CHECK_OK                              = 0
	AOF_CHECK_EMPTY                           = 1
	AOF_CHECK_TRUNCATED                       = 2
	AOF_CHECK_TIMESTAMP_TRUNCATED             = 3
	toTimestamp                               = 0
	AOF_MANIFEST_KEY_FILE_NAME                = "file"
	AOF_MANIFEST_KEY_FILE_SEQ                 = "seq"
	AOF_MANIFEST_KEY_FILE_TYPE                = "type"
	AOF_ANNOTATION_LINE_MAX_LEN               = 1024
)

// check 里面的主函数
func CheckAofMain(aofFilePath string) (checkResult bool, fileType AofFileType, err error) {
	/*
		getAofType := getInputAofFileTye(filePath) // 获取aof文件的类型
			switch getAOFType {
			case AOF_MULTI_PART:
				checkResult, err = checkMultiPartAof(dirpath, filepath, fix)
				return checkResult, aofMultiPart, nil
			case AOF_RESP:
				checkResult, err := checkOldStyleAof(filepath)
				return checkResult, aofResp, nil
				case AOF_RDB_PREAMBLE:
				checkResult, err := checkOldStyleAof(filepath)
				return checkResult, aofRdbPreamble, nil
				}
			return result, err
	*/
	//TODO: mock result
	return true, aofMultiPart, nil
}

func getInputAofFileType(aofFilepath string) AofFileType {
	if filelsManifest(aofFilepath) {
		return "AOF_MULTI_PART"
	} else if fileIsRDB(aofFilepath) {
		return "AOF_RDB_PREAMBLE"
	} else {
		return "AOF_RESP"
	}
}

func filelsManifest(aofFilepath string) bool {
	var is_manifest bool = false
	fp, err := os.Open(aofFilepath)
	if err != nil {
		fmt.Printf("Cannot open file %s:%s\n", aofFilepath, err.Error())
		os.Exit(1)
	}
	sb, err := os.Stat(aofFilepath)
	if err != nil {
		fmt.Printf("cannot stat file: %s\n", aofFilepath)
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
				fmt.Printf("cannot read file: %s\n", aofFilepath)
				os.Exit(1)
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

func fileIsRDB(aofFilepath string) bool {
	fp, err := os.Open(aofFilepath)
	if err != nil {
		fmt.Printf("Cannot open file %s:%s\n", aofFilepath, err.Error())
		os.Exit(1)
	}
	sb, err := os.Stat(aofFilepath)
	if err != nil {
		fmt.Printf("cannot stat file: %s\n", aofFilepath)
		os.Exit(1)
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

func printAofStyle(ret int, aofFileName string, aofType string) {
	switch ret {
	case AOF_CHECK_OK:
		fmt.Printf("%s %s is valid\n", aofType, aofFileName)
	case AOF_CHECK_EMPTY:
		fmt.Printf("%s %s is empty\n", aofType, aofFileName)
	case AOF_CHECK_TIMESTAMP_TRUNCATED:
		fmt.Printf("Successfully truncated AOF %s to timestamp %d\n", aofFileName, toTimestamp)
	case AOF_CHECK_TRUNCATED:
		fmt.Printf("Successfully truncated AOF %s\n", aofFileName)
	}

}

func makePath(paths string, filename string) string {
	return path.Join(paths, filename)
}

func pathIsBaseName(path string) bool {
	return strings.IndexByte(path, '/') == -1 && strings.IndexByte(path, '\\') == -1
}

func readArgc(fp *os.File, target *int64) int {
	return readLong(fp, '*', target)
}

func readString(fp *os.File, target *string) int {
	var len int64
	*target = ""
	if readLong(fp, '$', &len) == 0 {
		return 0
	}

	if len < 0 || len > math.MaxInt64-2 {
		fmt.Printf("Expected to read string of %d bytes, which is not in the suitable range\n", len)
		return 0
	}

	// Increase length to also consume \r\n
	len += 2
	data := make([]byte, len)
	if readBytes(fp, &data, len) == 0 {
		return 0
	}

	if consumeNewline(data[len-2:]) == 0 {
		return 0
	}

	*target = string(data[:len-2])
	return 1
}

func readBytes(fp *os.File, target *[]byte, length int64) int {
	var real int64
	epos, _ = fp.Seek(0, io.SeekCurrent)
	n, err := fp.Read(*target)
	real = int64(n)
	if err != nil || real != length {
		fmt.Printf("Expected to read %d bytes, got %d bytes\n", length, real)
		return 0
	}
	return 1
}

func consumeNewline(buf []byte) int {
	if buf[0] != '\r' || buf[1] != '\n' {
		fmt.Printf("Expected \\r\\n, got: %02x%02x", buf[0], buf[1])
		return 0
	}
	line += 1
	return 1
}

func readLong(fp *os.File, prefix byte, target *int64) int {
	buf := make([]byte, 128)
	var err error
	epos, err = fp.Seek(0, io.SeekCurrent)
	if err != nil {
		fmt.Printf("Failed to get current position, aborting...\n")
		os.Exit(1)
	}
	reader := bufio.NewReader(fp)
	if _, err := reader.ReadBytes('\n'); err != nil {
		return 0
	}
	buf, err = reader.ReadBytes('\n')
	if err != nil {
		fmt.Println("Failed to read line from file")
		return 0
	}
	if buf[0] != prefix {
		fmt.Printf("Expected prefix '%c', got: '%c'\n", prefix, buf[0])
		return 0
	}
	value, err := strconv.ParseInt(string(buf[1:len(buf)-1]), 10, 64) //去除了换行符*8*
	if err != nil {
		fmt.Println("Failed to parse long value")
		return 0
	}
	*target = value
	line += 1
	return 1

}
func aofLoadManifestFromFile(am_filepath string) *aofManifest {
	var maxseq int64
	am := aofManifestcreate()
	fp, err := os.Open(am_filepath)
	if err != nil {
		log.Fatalf("Fatal error:can't open the AOF manifest %s for reading: %s", am_filepath, err)
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
					log.Fatalf("Found an empty AOF manifest")
				} else {
					break
				}

			} else {
				log.Fatalf("Read AOF manifest failed")
			}
		}
		linenum++
		if buf[0] == '#' {
			continue
		}
		if !strings.Contains(buf, "\n") {
			log.Fatalf("The AOF manifest file contains too long line")
		}
		line = strings.Trim(buf, " \t\r\n")
		if len(line) == 0 {
			log.Fatalf("Invalid AOF manifest file format")
		}
		argc := 0
		argv, argc = splitArgs(line)

		if argc < 6 || argc%2 != 0 {
			log.Fatalf("Invalid AOF manifest file format")
		}
		ai = aofInfoCreate()
		for i := 0; i < argc; i += 2 {
			if strings.EqualFold(argv[i], AOF_MANIFEST_KEY_FILE_NAME) {
				ai.fileName = string(argv[i+1])
				if !pathIsBaseName(string(ai.fileName)) {
					log.Fatalf("File can't be a path, just a filename")
				}
			} else if strings.EqualFold(argv[i], AOF_MANIFEST_KEY_FILE_SEQ) {
				ai.fileSeq, _ = strconv.ParseInt(argv[i+1], 10, 64)
			} else if strings.EqualFold(argv[i], AOF_MANIFEST_KEY_FILE_TYPE) {
				ai.aofFileType = string(argv[i+1][0])
			}
		}
		if ai.fileName == "" || ai.fileSeq == 0 || ai.aofFileType == "" {
			log.Fatalf("Invalid AOF manifest file format")
		}
		//==nil
		if ai.aofFileType == AofManifestFileTypeBase {
			if am.baseAofInfo != nil {
				log.Fatalf("Found duplicate base file information")
			}
			am.baseAofInfo = ai
			am.currBaseFileSeq = ai.fileSeq
		} else if ai.aofFileType == AofManifestTypeHist {
			am.historyList = listAddNodeTail(am.historyList, ai)
		} else if ai.aofFileType == AofManifestTypeIncr {
			if ai.fileSeq <= maxseq {
				log.Fatalf("Found a non-monotonic sequence number")
			}
			am.incrAofList = listAddNodeTail(am.historyList, ai)
			am.currIncrFIleSeq = ai.fileSeq
			maxseq = ai.fileSeq
		} else {
			log.Fatalf("Unknown AOF file type")
		}
		line = " "
		ai = nil
	}
	fp.Close()
	return am
}

func processRESP(fp *os.File, filename string, outMulti *int) int {
	var argc int64
	var str string

	if readArgc(fp, &argc) == 0 {
		return 0
	}

	for i := int64(0); i < argc; i++ {
		if readString(fp, &str) == 0 {
			return 0
		}
		if i == 0 {
			if strings.EqualFold(str, "multi") {
				(*outMulti)++
				if (*outMulti) > 0 {
					err := fmt.Errorf("Unexpected MULTI in AOF %s", filename)
					fmt.Println(err.Error())
					return 0
				}
			} else if strings.EqualFold(str, "exec") {
				(*outMulti)--
				if (*outMulti) != 0 {
					err := fmt.Errorf("Unexpected EXEC in AOF %s", filename)
					fmt.Println(err.Error())
					return 0
				}
			}
		}
	}

	return 1
}

// 截断可能有问题
func processAnnotations(fp *os.File, filename string, lastFile bool) int {

	epos, err := fp.Seek(0, io.SeekCurrent)
	if err != nil {
		fmt.Printf("Failed to get current position, aborting...\n")
		os.Exit(1)
	}
	reader := bufio.NewReader(fp)
	buf, _, err := reader.ReadLine()
	if err != nil {
		fmt.Printf("Failed to read annotations from AOF %s, aborting...\n", filename)
		os.Exit(1)
	}

	if toTimestamp != 0 && strings.HasPrefix(string(buf), "#TS:") {
		var ts int64
		ts, err = strconv.ParseInt(strings.TrimPrefix(string(buf), "#TS:"), 10, 64)
		if err != nil {
			fmt.Println("Invalid timestamp annotation")
			os.Exit(1)
		}
		if ts <= toTimestamp {
			return 1
		}
		if epos == 0 {
			fmt.Printf("AOF %s has nothing before timestamp %ld, aborting...\n", filename, toTimestamp)
			os.Exit(1)
		}
		if lastFile == false {
			fmt.Printf("Failed to truncate AOF %s to timestamp %ld to offset %ld because it is not the last file.\n", filename, toTimestamp, epos)
			fmt.Println("If you insist, please delete all files after this file according to the manifest file and delete the corresponding records in manifest file manually. Then re-run redis-check-aof.")
			os.Exit(1)
		}
		// Truncate remaining AOF if exceeding 'toTimestamp'
		if err := fp.Truncate(epos); err != nil {
			fmt.Printf("Failed to truncate AOF %s to timestamp %ld\n", filename, toTimestamp)
			os.Exit(1)
		} else {
			return 0
		}
	}
	return 1
}

/*
func checkRdbMain(argc int, argv *[]string, fp *os.File) int {

		if argc != 2 && fp == nil {
			fmt.Fprintf(os.Stderr, "Usage: %s <rdb-file-name>\n", argv[0])
			os.Exit(1)
		} else if argv[1] == "-v" || argv[1] == "--version" {
			version := checkRdbVersion()
			fmt.Printf("redis-check-rdb %s\n", version)
			os.Exit(0)
		}

		tv := time.Now()
		sec := int64(tv.Unix())
		usec := int64(tv.Nanosecond()) / 1000
		pid := int64(os.Getpid())
		seed := ((sec * 1000000) + usec) ^ pid
		init_genrand64(seed)
		rdbCheckMode = 1
		rdbCheckInfo("Checking RDB file %s", args[1])
		rdbCheckSetupSignals()
		retval := redis_Check_RDB(args[1], fp)
		if retval == 0 {
			rdbCheckInfo("\\o/ RDB looks OK! \\o/")
			rdbShowGenericInfo()
		}
		if fp != nil {
			if retval == 0 {
				return 0
			} else {
				return 1
			}
		}
		os.Exit(retval)
	}
*/
func checkMultipartAof(dirpath string, manifestFilepath string, fix int) {
	totalNum := 0
	aofNum := 0
	var ret int
	am := aofLoadManifestFromFile(manifestFilepath)
	if am.baseAofInfo != nil {
		totalNum++
	}
	if am.incrAofList != nil {
		totalNum += int(am.incrAofList.len)
	}
	if am.baseAofInfo != nil {
		aofFilename := am.baseAofInfo.fileName
		aofFilepath := makePath(dirpath, aofFilename)
		lastFile := (aofNum + 1) == totalNum
		aofPreable := fileIsRDB(aofFilepath)
		if aofPreable {
			fmt.Printf("Start to check BASE AOF (RDB format).\n")
		} else {
			fmt.Printf("Start to check BASE AOF (AOF format).\n")
		}
		ret = checkSingleAof(aofFilename, aofFilepath, lastFile, fix, aofPreable)
		printAofStyle(ret, aofFilename, "BASE AOF")

	}
	if am.incrAofList.len != 0 {
		fmt.Printf("start to check INCR INCR files.")
		var ln *listNode
		ln = am.incrAofList.head
		for ln != nil {
			ai := ln.value.(*aofInfo)
			aofFilename := ai.fileName
			aofFilepath := makePath(dirpath, aofFilename)
			lastFile := (aofNum + 1) == totalNum
			ret = checkSingleAof(aofFilename, aofFilepath, lastFile, fix, false)
			printAofStyle(ret, aofFilename, "INCR AOF")
			//stringfree(aofFilepath)
			ln = ln.next
		}
	}

	//aofManifestFree(am)
	fmt.Println("All AOF files and manifest are vaild")
}

func checkOldStyleAof(aofFilepath string, fix int, preamble bool) {
	fmt.Printf("Start checking Old-Style AOF\n")
	var ret = checkSingleAof(aofFilepath, aofFilepath, true, fix, preamble)
	printAofStyle(ret, aofFilepath, "AOF")

}

func checkSingleAof(aofFilename, aofFilepath string, lastFile bool, fix int, preamble bool) int {
	var pos, diff int64
	multi := 0
	buf := make([]byte, 2)

	fp, err := os.OpenFile(aofFilepath, os.O_RDWR, 0666)
	if err != nil {
		log.Fatalf("Cannot open file %s:%s,aborting...\n", aofFilepath, err)
	}
	sb, err := fp.Stat()
	if err != nil {
		log.Fatalf("Cannot stat file: %s,aborting...\n", aofFilename)
	}
	size := sb.Size()
	if size == 0 {
		return AOF_CHECK_EMPTY
	}
	if preamble != false {
		argv := []string{aofFilepath}
		err := checkRdbMain(2, argv, fp)
		if err == -1 {
			log.Fatal("RDB preamble of AOF file is not sane, aborting.")
		} else {
			fmt.Println("RDB preamble is OK, proceeding with AOF tail...")
		}
	}
	for {
		if multi != 0 {
			var err error
			pos, err = fp.Seek(0, io.SeekCurrent)
			if err != nil {
				log.Fatalf(("Failed to seek in AOF %s: %s"), aofFilename, err)
			}
		}
		if _, err := fp.Read(buf); err != nil {
			if err == io.EOF {
				break
			}
			log.Fatalf("Failed to read from AOF %s, aborting...\n", aofFilename)
		}
		if _, err := fp.Seek(-1, io.SeekCurrent); err != nil {
			log.Fatalf("Failed to fseek in AOF %s: %s", aofFilename, err)
		}

		switch buf[0] {
		case '#':
			if processAnnotations(fp, aofFilepath, lastFile) == 0 {
				fp.Close()
				return AOF_CHECK_TIMESTAMP_TRUNCATED
			}
		case '*':
			if processRESP(fp, aofFilepath, &multi) == 0 {
				break
			}
		default:
			fmt.Printf("AOF %s format error\n", aofFilename)
			break
		}
	}
	if _, err := fp.Stat(); err == nil && multi == 1 && len(errors) == 0 {
		if _, err := fp.Seek(0, io.SeekEnd); err != nil {
			if _, err := fp.Seek(0, io.SeekCurrent); err == io.EOF {
				fmt.Println("Reached EOF before reading EXEC for MULTI")
			}
		}
	}

	if len(errors) > 0 {
		fmt.Println(errors)
	}

	diff = size - pos
	if diff == 0 && toTimestamp == 1 {
		fmt.Printf("Truncate nothing in AOF %s to timestamp %d\n", aofFilename, toTimestamp)
		return AOF_CHECK_OK
	}
	fmt.Printf("AOF analyzed: filename=%s, size=%d, ok_up_to=%d, ok_up_to_line=%d, diff=%d\n", aofFilename, size, pos, line, diff)
	if diff > 0 {
		if fix == 1 {
			if lastFile == false {
				fmt.Printf("Failed to truncate AOF %s because it is not the last file\n", aofFilename)
				os.Exit(1)
			}

			fmt.Printf("this will shrink the AOF %s from %d bytes,with %d bytes,to %d bytes\n", aofFilename, size, diff, pos)
			fmt.Print("Continue? [y/N]: ")
			reader := bufio.NewReader(os.Stdin)
			input, err := reader.ReadString('\n')
			if err != nil || strings.ToLower(string(input[0])) != "y" {
				fmt.Println("Aborting...")
				os.Exit(1)
			}

			if err := fp.Truncate(pos); err != nil {
				fmt.Printf("Failed to truncate AOF %s\n", aofFilename)
				os.Exit(1)
			} else {
				return AOF_CHECK_TRUNCATED
			}
		} else {
			fmt.Printf("AOF %s is not valid.Use the --fix potion to try fixing it.\n", aofFilename)
			os.Exit(1)
		}
	}
	fp.Close()
	return AOF_CHECK_OK
}
