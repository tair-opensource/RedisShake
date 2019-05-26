package utils

import (
	"testing"
	"fmt"
	"sort"

	"github.com/stretchr/testify/assert"
)

func TestGetAllClusterNode(t *testing.T) {
	var nr int
	{
		fmt.Printf("TestGetAllClusterNode case %d.\n", nr)
		nr++

		client := OpenRedisConn([]string{"10.1.1.1:21333"}, "auth", "123456", false)
		ret, err := GetAllClusterNode(client, "master")
		sort.Strings(ret)
		assert.Equal(t, nil, err, "should be equal")
		assert.Equal(t, 3, len(ret), "should be equal")
		assert.Equal(t, "10.1.1.1:21331", ret[0], "should be equal")
		assert.Equal(t, "10.1.1.1:21332", ret[1], "should be equal")
		assert.Equal(t, "10.1.1.1:21333", ret[2], "should be equal")
	}

	{
		fmt.Printf("TestGetAllClusterNode case %d.\n", nr)
		nr++

		client := OpenRedisConn([]string{"10.1.1.1:21333"}, "auth", "123456", false)
		ret, err := GetAllClusterNode(client, "slave")
		sort.Strings(ret)
		assert.Equal(t, nil, err, "should be equal")
		assert.Equal(t, 3, len(ret), "should be equal")
		assert.Equal(t, "10.1.1.1:21334", ret[0], "should be equal")
		assert.Equal(t, "10.1.1.1:21335", ret[1], "should be equal")
		assert.Equal(t, "10.1.1.1:21336", ret[2], "should be equal")
	}

	{
		fmt.Printf("TestGetAllClusterNode case %d.\n", nr)
		nr++

		client := OpenRedisConn([]string{"10.1.1.1:21333"}, "auth", "123456", false)
		ret, err := GetAllClusterNode(client, "all")
		sort.Strings(ret)
		assert.Equal(t, nil, err, "should be equal")
		assert.Equal(t, 6, len(ret), "should be equal")
		assert.Equal(t, "10.1.1.1:21331", ret[0], "should be equal")
		assert.Equal(t, "10.1.1.1:21332", ret[1], "should be equal")
		assert.Equal(t, "10.1.1.1:21333", ret[2], "should be equal")
		assert.Equal(t, "10.1.1.1:21334", ret[3], "should be equal")
		assert.Equal(t, "10.1.1.1:21335", ret[4], "should be equal")
		assert.Equal(t, "10.1.1.1:21336", ret[5], "should be equal")
	}
}