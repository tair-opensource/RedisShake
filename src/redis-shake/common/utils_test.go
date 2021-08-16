package utils

import (
	"fmt"
	redigo "github.com/garyburd/redigo/redis"
	"testing"
)

func Test_OpenRedisConn(t *testing.T) {
	shardMap, _ := OpenRedisConnMap("10.10.26.7:26377", "auth", "", "stock-indicator-core")
	for key, value := range shardMap {
		val, err := redigo.String(value.Do("set", key, key))
		fmt.Println("Key:", key, "Value:", val, "Error:", err)
	}
	fmt.Println("=================================")
	for key, value := range shardMap {
		val, err := redigo.String(value.Do("get", key))
		fmt.Println("Key:", key, "Value:", val, "Error:", err)
	}
}

