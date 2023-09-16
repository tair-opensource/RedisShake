package aof_test

import (
	"fmt"
	"os"
	"testing"

	"github.com/go-redis/redis"
)

const (
	AOFRestorePath    = "/test_aof_restore.toml"
	AppendOnlyAoFPath = "/appendonlydir/appendonly.aof.manifest"
)

// if you use this test you need start redis in port 6379s
func TestMainFunction(t *testing.T) {

	//	os.Args = []string{"redis-shake", "/home/hwy/kaiyuan/restore.toml"}
	wdPath, err := os.Getwd()
	if err != nil {
		panic(err)
	}

	configPath := wdPath + AOFRestorePath
	aofFilePath := wdPath + AppendOnlyAoFPath
	fmt.Printf("configPath:%v, aofFilepath:%v\n", configPath, aofFilePath)

	// AOFMain(configPath, aofFilePath) //restore aof

	client := redis.NewClient(&redis.Options{
		Addr:     "localhost:6379",
		Password: "",
		DB:       0,
	})

	pong, err := client.Ping().Result()
	if err != nil {
		t.Fatalf("Failed to connect to Redis: %v", err)
	}
	fmt.Println("Connected to Redis:", pong)
	/*
		for i := 11; i <= 10000; i++ {
			value := strconv.Itoa(i)
			score := float64(i)
			z := redis.Z{Score: score, Member: value}
			err := client.ZAdd("myzset", z).Err()
			fmt.Println(value)
			if err != nil {
				fmt.Println("Failed to write data to Redis:", err)
				return
			}
		}
	*/
	// 读取整个有序集合
	zsetValues, err := client.ZRangeWithScores("myzset", 0, -1).Result()
	if err != nil {
		fmt.Println("Failed to read data from Redis:", err)
		return
	}

	// 遍历有序集合中的元素和分数
	for _, z := range zsetValues {
		member := z.Member.(string)
		score := z.Score
		fmt.Printf("Member: %s, Score: %f\n", member, score)
	}
	/*
		expected := map[string]string{
			"kl":    "kl",
			"key0":  "2022-03-29 17:25:54.592593",
			"key1":  "2022-03-29 17:25:54.876326",
			"key2":  "2022-03-29 17:25:52.871918",
			"key3":  "2022-03-29 17:25:53.034060",
			"key4":  "2022-03-29 17:25:53.196913",
			"key5":  "2022-03-29 17:25:53.356234",
			"key6":  "2022-03-29 17:25:53.513544",
			"key7":  "2022-03-29 17:25:53.671556",
			"key8":  "2022-03-29 17:25:53.861237",
			"key9":  "2022-03-29 17:25:54.020518",
			"key10": "2022-03-29 17:25:54.177881",
			"key11": "2022-03-29 17:25:54.337640",
		}
	*/
	/*for key, value := range expected {
		result, err := client.Get(key).Result()
		if err != nil {
			t.Fatalf("Failed to read key %s from Redis: %v", key, err)
		}

		if result != value {
			t.Errorf("Value for key %s is incorrect. Expected: %s, Got: %s", key, value, result)
		}
	}

	for key := 11; key <= 10000; key++ {
		result, err := client.Get(strconv.Itoa(key)).Result()
		if err != nil {
			t.Fatalf("Failed to read key %v from Redis: %v", key, err)
		}

		if result != strconv.Itoa(key) {
			t.Errorf("Value for key %v is incorrect. Expected: %v, Got: %v", key, key, result)
		}
	}*/

	/*result, err := client.SMembers("superpowers").Result()
	if err != nil {
		t.Fatalf("Failed to read set from Redis: %v", err)
	}
	strings := result[0]
	if strings != "reflexes" {
		t.Errorf("read set wrong")
	}*/

}
