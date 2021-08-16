package utils

import (
	"fmt"
	"testing"
)

// 单元测试go的与java的hash结果一致，风险不知道是不是还是存在。因为go里面的int64根据操作系统走的
func Test_Murmurhash_Key(t *testing.T) {
	key_hash := MurmurHash64A([]byte("key"))
	fmt.Println(key_hash)
}