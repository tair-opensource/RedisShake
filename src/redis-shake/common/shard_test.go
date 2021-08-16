package utils

import (
	"fmt"
	"strconv"
	"testing"
)

// 单元测试与Java的输出一致
func Test_Shard_Key(t *testing.T) {
	shardNameList := []string{"shard01","shard02","shard03","shard04","shard05","shard06","shard07","shard08","shard09","shard10","shard11","shard12","shard13","shard14","shard15","shard16"}
	consistentHashing, _ := NewConsistentHashing(shardNameList)

	keyShardName := consistentHashing.GetShardIndex([]byte("dau_sync_first_5663779795_20210816"))
	fmt.Println(keyShardName)

	for i := 0; i < 1000000; i = i + 10 {
		shardName := consistentHashing.GetShardIndex([]byte("key" + strconv.Itoa(i)))
		fmt.Println("key" + strconv.Itoa(i) + "----" + shardName)
	}
}
