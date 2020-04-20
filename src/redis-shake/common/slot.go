package utils

import (
	"fmt"

	"pkg/libs/log"

	"github.com/vinllen/redis-go-cluster"
)

const (
	checkpointSuffixLen = 4
)

func KeyToSlot(key string) uint16 {
	hashtag := ""
	for i, s := range key {
		if s == '{' {
			for k := i; k < len(key); k++ {
				if key[k] == '}' {
					hashtag = key[i+1 : k]
					break
				}
			}
		}
	}
	if len(hashtag) > 0 {
		return crc16(hashtag) & 0x3fff
	}
	return crc16(key) & 0x3fff
}

// chose key(${prefix}-${suffix}) to let it hash in the given slot boundary: [left, right].
// return suffix
func ChoseSlotInRange(prefix string, left, right int) string {
	judge := func(slot int) bool {
		if slot >= left && slot <= right {
			return true
		}
		return false
	}

	prefix = fmt.Sprintf("%s-", prefix)
	_, suffix := pickSuffixDfs(0, judge, []byte(prefix))
	return suffix
}

func pickSuffixDfs(depth int, judge func(int) bool, prefix []byte) (bool, string) {
	if depth >= checkpointSuffixLen {
		slot, err := redis.GetSlot(prefix)
		if err != nil {
			log.Panicf("get slot failed: %v", err)
			return false, ""
		}

		if judge(int(slot)) {
			return true, string(prefix)
		}
		return false, ""
	}

	var i byte
	for i = 'a'; i <= 'z'; i++ {
		prefix = append(prefix, i)
		ok, ret := pickSuffixDfs(depth + 1, judge, prefix)
		if ok {
			return ok, ret
		}
		// backtrace
		prefix = prefix[:len(prefix) - 1]
	}
	return false, ""
}