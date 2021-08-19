package checkpoint

import (
	"fmt"
	"testing"

	"redis-shake/common"
	"redis-shake/unit_test_common"

	"github.com/stretchr/testify/assert"
)

var (
	testAddr = unit_test_common.TestUrl
)

func TestCheckpoint(t *testing.T) {
	// test checkpoint

	var nr int

	// test fetchCheckpoint: empty
	{
		fmt.Printf("TestCheckpoint case %d.\n", nr)
		nr++

		c := utils.OpenRedisConn([]string{testAddr}, "auth", "", false, false)
		// clean all
		_, err := c.Do("flushall")
		assert.Equal(t, nil, err, "should be equal")

		runId, offset, version, err := fetchCheckpoint(testAddr, c, 0, utils.CheckpointKey)
		assert.Equal(t, nil, err, "should be equal")
		assert.Equal(t, "", runId, "should be equal")
		assert.Equal(t, int64(-1), offset, "should be equal")
		assert.Equal(t, int(-1), version, "should be equal")
	}

	// test fetchCheckpoint: empty
	{
		fmt.Printf("TestCheckpoint case %d.\n", nr)
		nr++

		c := utils.OpenRedisConn([]string{testAddr}, "auth", "", false, false)
		// clean all
		_, err := c.Do("flushall")
		assert.Equal(t, nil, err, "should be equal")

		_, err = c.Do("select", 5)
		assert.Equal(t, nil, err, "should be equal")

		_, err = c.Do("hset", utils.CheckpointKey, "meaningless", 123)
		assert.Equal(t, nil, err, "should be equal")

		runId, offset, version, err := fetchCheckpoint(testAddr, c, 0, utils.CheckpointKey)
		assert.Equal(t, nil, err, "should be equal")
		assert.Equal(t, "", runId, "should be equal")
		assert.Equal(t, int64(-1), offset, "should be equal")
		assert.Equal(t, int(-1), version, "should be equal")
	}

	// test fetchCheckpoint: not empty
	{
		fmt.Printf("TestCheckpoint case %d.\n", nr)
		nr++

		c := utils.OpenRedisConn([]string{testAddr}, "auth", "", false, false)
		// clean all
		_, err := c.Do("flushall")
		assert.Equal(t, nil, err, "should be equal")

		_, err = c.Do("select", 5)
		assert.Equal(t, nil, err, "should be equal")

		// only offset
		offsetKey := fmt.Sprintf("%s-%s", testAddr, utils.CheckpointOffset)
		_, err = c.Do("hset", utils.CheckpointKey, offsetKey, 123)
		assert.Equal(t, nil, err, "should be equal")

		// version
		versionKey := fmt.Sprintf("%s-%s", testAddr, utils.CheckpointVersion)
		_, err = c.Do("hset", utils.CheckpointKey, versionKey, 3)
		assert.Equal(t, nil, err, "should be equal")

		runId, offset, version, err := fetchCheckpoint(testAddr, c, 5, utils.CheckpointKey)
		assert.Equal(t, nil, err, "should be equal")
		assert.Equal(t, "?", runId, "should be equal")
		assert.Equal(t, int64(123), offset, "should be equal")
		assert.Equal(t, int(3), version, "should be equal")

		// with runId
		runIdKey := fmt.Sprintf("%s-%s", testAddr, utils.CheckpointRunId)
		_, err = c.Do("hset", utils.CheckpointKey, runIdKey, "test_run_id")
		assert.Equal(t, nil, err, "should be equal")

		runId, offset, _, err = fetchCheckpoint(testAddr, c, 5, utils.CheckpointKey)
		assert.Equal(t, nil, err, "should be equal")
		assert.Equal(t, "test_run_id", runId, "should be equal")
		assert.Equal(t, int64(123), offset, "should be equal")
	}

	// test LoadCheckpoint
	{
		fmt.Printf("TestCheckpoint case %d.\n", nr)
		nr++

		c := utils.OpenRedisConn([]string{testAddr}, "auth", "", false, false)
		// clean all
		_, err := c.Do("flushall")
		assert.Equal(t, nil, err, "should be equal")

		// insert into db5
		_, err = c.Do("select", 5)
		assert.Equal(t, nil, err, "should be equal")

		offsetKey := fmt.Sprintf("%s-%s", testAddr, utils.CheckpointOffset)
		runIdKey := fmt.Sprintf("%s-%s", testAddr, utils.CheckpointRunId)
		_, err = c.Do("hmset", utils.CheckpointKey, offsetKey, 8910, runIdKey, "test_run_id")
		assert.Equal(t, nil, err, "should be equal")

		// insert into db6
		_, err = c.Do("select", 6)
		assert.Equal(t, nil, err, "should be equal")
		_, err = c.Do("hmset", utils.CheckpointKey, offsetKey, 8920, runIdKey, "test_run_id")
		assert.Equal(t, nil, err, "should be equal")

		// insert into db7, only offset
		_, err = c.Do("select", 7)
		assert.Equal(t, nil, err, "should be equal")
		_, err = c.Do("hmset", utils.CheckpointKey, offsetKey, 8930)
		assert.Equal(t, nil, err, "should be equal")

		// insert into db7, only runId
		_, err = c.Do("select", 8)
		assert.Equal(t, nil, err, "should be equal")
		_, err = c.Do("hmset", utils.CheckpointKey, runIdKey, "test_run_id")
		assert.Equal(t, nil, err, "should be equal")

		// run
		runId, offset, db, err := LoadCheckpoint(0, testAddr, []string{testAddr}, "auth", "", utils.CheckpointKey, false, false)
		assert.Equal(t, nil, err, "should be equal")
		assert.Equal(t, "?", runId, "should be equal")
		assert.Equal(t, int64(8930), offset, "should be equal")
		assert.Equal(t, -1, db, "should be equal")

		// test all checkpoint exist
		// should all empty
		dbList := []int{5, 6, 7, 8}
		for _, db := range dbList {
			_, err = c.Do("select", db)
			assert.Equal(t, nil, err, "should be equal")

			reply, err := c.Do("exists", utils.CheckpointKey)
			assert.Equal(t, nil, err, "should be equal")
			assert.Equal(t, int64(0), reply.(int64), "should be equal")
		}
	}

	// test LoadCheckpoint
	{
		fmt.Printf("TestCheckpoint case %d.\n", nr)
		nr++

		c := utils.OpenRedisConn([]string{testAddr}, "auth", "", false, false)
		// clean all
		_, err := c.Do("flushall")
		assert.Equal(t, nil, err, "should be equal")

		// insert into db5
		_, err = c.Do("select", 5)
		assert.Equal(t, nil, err, "should be equal")

		offsetKey := fmt.Sprintf("%s-%s", testAddr, utils.CheckpointOffset)
		runIdKey := fmt.Sprintf("%s-%s", testAddr, utils.CheckpointRunId)
		_, err = c.Do("hmset", utils.CheckpointKey, offsetKey, 8910, runIdKey, "test_run_id")
		assert.Equal(t, nil, err, "should be equal")

		// insert into db6, only offset
		_, err = c.Do("select", 6)
		assert.Equal(t, nil, err, "should be equal")
		_, err = c.Do("hmset", utils.CheckpointKey, offsetKey, 8920)
		assert.Equal(t, nil, err, "should be equal")

		// insert into db7, only offset
		_, err = c.Do("select", 7)
		assert.Equal(t, nil, err, "should be equal")
		_, err = c.Do("hmset", utils.CheckpointKey, offsetKey, 8930, runIdKey, "test_run_id")
		assert.Equal(t, nil, err, "should be equal")

		// insert into db7, only runId
		_, err = c.Do("select", 8)
		assert.Equal(t, nil, err, "should be equal")
		_, err = c.Do("hmset", utils.CheckpointKey, runIdKey, "test_run_id")
		assert.Equal(t, nil, err, "should be equal")

		// run
		runId, offset, db, err := LoadCheckpoint(0, testAddr, []string{testAddr}, "auth", "", utils.CheckpointKey, false, false)
		assert.Equal(t, nil, err, "should be equal")
		assert.Equal(t, "test_run_id", runId, "should be equal")
		assert.Equal(t, int64(8930), offset, "should be equal")
		assert.Equal(t, 7, db, "should be equal")

		// test all checkpoint exist
		dbList := []int{5, 6, 8}
		for _, db := range dbList {
			_, err = c.Do("select", db)
			assert.Equal(t, nil, err, "should be equal")

			reply, err := c.Do("exists", utils.CheckpointKey)
			assert.Equal(t, nil, err, "should be equal")
			assert.Equal(t, int64(0), reply.(int64), "should be equal")
		}

		// db7 should have checkpoint
		_, err = c.Do("select", 7)
		assert.Equal(t, nil, err, "should be equal")

		reply, err := c.Do("exists", utils.CheckpointKey)
		assert.Equal(t, nil, err, "should be equal")
		assert.Equal(t, int64(1), reply.(int64), "should be equal")
	}
}
