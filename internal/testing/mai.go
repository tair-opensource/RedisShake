package main

import (
	"bufio"
	"fmt"
	"io"
	"math"
	"os"

	"strconv"
)

func ReadBytes(fp *os.File, target *[]byte, length int64) int {
	var real int64
	_, _ = fp.Seek(0, io.SeekCurrent)
	n, err := fp.Read(*target)

	real = int64(n)
	if err != nil || real != length {
		fmt.Printf("Expected to read %d bytes, got %d bytes\n", length, real)
		return 0
	}
	return 1
}
func readLong(fp *os.File, prefix byte, target *int64) int {

	var err error
	_, err = fp.Seek(0, io.SeekCurrent)
	if err != nil {
		fmt.Printf("Failed to get current position, aborting...\n")
		os.Exit(1)
	}
	reader := bufio.NewReader(fp)

	buf, err := reader.ReadBytes('\n')

	println(buf)
	if err != nil {
		fmt.Println("Failed to read line from file")
		return 0
	}
	if buf[0] != prefix {
		fmt.Printf("Expected prefix '%c', got: '%c'\n", prefix, buf[0])
		return 0
	}
	value, err := strconv.ParseInt(string(buf[1:len(buf)-2]), 10, 64) //去除了换行符/r/n
	if err != nil {
		fmt.Println("Failed to parse long value")
		return 0
	}
	*target = value

	return 1

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

// test ok
func readBytes(fp *os.File, target *[]byte, length int64) int {
	var real int64
	_, _ = fp.Seek(0, io.SeekCurrent)
	n, err := fp.Read(*target)
	real = int64(n)
	if err != nil || real != length {
		fmt.Printf("Expected to read %d bytes, got %d bytes\n", length, real)
		return 0
	}
	return 1
}

// testok
func consumeNewline(buf []byte) int {
	if buf[0] != '\r' || buf[1] != '\n' {
		fmt.Printf("Expected \\r\\n, got: %02x%02x", buf[0], buf[1])
		return 0
	}
	return 1
}
func readString(fp *os.File, target *string) int {
	var len int64
	*target = ""
	if readLong(fp, '$', &len) == 0 {
		return 0
	}
	println("readlong ok\n")
	println(len)
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

func main() {

	isManifest := fileIsRDB("appendonly.aof.2.base.rdb")
	fmt.Println("Is manifest:", isManifest)
}
