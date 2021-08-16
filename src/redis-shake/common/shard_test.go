package utils

import (
	"fmt"
	"strconv"
	"testing"
)

// 单元测试与Java的输出一致
func Test_Shard_Key(t *testing.T) {
	shardNameList := []string{"shard1", "shard2", "shard3", "shard4"}
	consistentHashing, _ := NewConsistentHashing(shardNameList)
	for i := 0; i < 1000000; i = i + 10 {
		shardName := consistentHashing.GetShardIndex([]byte("key" + strconv.Itoa(i)))
		fmt.Println("key" + strconv.Itoa(i) + "----" + shardName)
	}
}
