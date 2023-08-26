package main

import (
	"fmt"
	"os"
	"testing"

	"github.com/go-redis/redis"
)

func TestMainFunction(t *testing.T) {

	os.Args = []string{"redis-shake", "/home/hwy/kaiyuan/restore.toml"}
	main()

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
	for key, value := range expected {
		result, err := client.Get(key).Result()
		if err != nil {
			t.Fatalf("Failed to read key %s from Redis: %v", key, err)
		}

		if result != value {
			t.Errorf("Value for key %s is incorrect. Expected: %s, Got: %s", key, value, result)
		}
	}

	result, err := client.SMembers("superpowers").Result()
	if err != nil {
		t.Fatalf("Failed to read set from Redis: %v", err)
	}
	strings := result[0]
	if strings != "reflexes" {
		t.Errorf("read set wrong")
	}

}
