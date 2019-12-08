package checkpoint

import (
	"redis-shake/common"
	"redis-shake/configure"

	redigo "github.com/garyburd/redigo/redis"
	"fmt"
	"strconv"
	"strings"
	"pkg/libs/log"
)

func LoadCheckpoint(sourceAddr string, target []string, authType, passwd string, isCluster bool, tlsEnable bool) (string, uint64, error) {
	c := utils.OpenRedisConn(target, authType, passwd, isCluster, tlsEnable)

	// fetch logical db list
	ret, err := c.Do("info", "keyspace")
	if err != nil {
		return "", 0, err
	}

	// TODO, for some kind of redis type, like codis, tencent cloud, the keyspace result may not be accurate
	// so there maybe some problems.
	mp, err := utils.ParseKeyspace(ret.([]byte))
	if err != nil {
		return "", 0, err
	}

	for db := range mp {
		log.Infof("load checkpoint check db[%v]", db)
		runId, offsetBegin, offsetEnd, err := fetchCheckpoint(sourceAddr, c, db)
		if err != nil {
			return "", -1, err
		}

		if offsetBegin != offsetEnd {
			log.Warnf("offsetBegin[%v] != offsetEnd[%v]", offsetBegin, offsetEnd)
			continue
		}
	}
}

/*
 * fetch checkpoint from give address
 * @return:
 *     string: runid
 *     int64: offset-begin
 *     int64: offset-end
 *     error
 */
func fetchCheckpoint(sourceAddr string, c redigo.Conn, db string) (string, int64, int64, error) {
	_, err := c.Do("select", db)
	if err != nil {
		return "", -1, -1, fmt.Errorf("fetch checkpoint do select db[%v] failed[%v]", db, err)
	}

	// judge checkpoint exists
	if reply, err := c.Do("exists", utils.CheckpointKey); err != nil {
		return "", -1, -1, fmt.Errorf("fetch checkpoint do judge checkpoint exists failed[%v]", err)
	} else {
		if reply.(byte) == byte(0) {
			// not exist
			return "", -1, -1, nil
		}
	}

	// hgetall
	if reply, err := c.Do("hgetall", utils.CheckpointKey); err != nil {
		return "", -1, -1, fmt.Errorf("fetch checkpoint do hgetall failed[%v]", err)
	} else {
		runId := ""
		var offsetBegin int64 = -1
		var offsetEnd int64 = -1
		replyList := reply.([]interface{})
		for i := 0; i < len(replyList); i += 2 {
			line := replyList[i].([]byte)
			lineS := utils.Bytes2String(line)
			if strings.HasPrefix(lineS, sourceAddr) {
				if strings.Contains(lineS, utils.CheckpointOffsetBegin) {
					next := utils.Bytes2String(replyList[i + 1].([]byte))
					offsetBegin, err = strconv.ParseInt(next, 10, 64)
					if err != nil {
						return "", -1, -1, fmt.Errorf("fetch checkpoint do parse offset-begin[%v] failed[%v]",
							next, err)
					}
				}

				if strings.Contains(lineS, utils.CheckpointOffsetEnd) {
					next := utils.Bytes2String(replyList[i + 1].([]byte))
					offsetEnd, err = strconv.ParseInt(next, 10, 64)
					if err != nil {
						return "", -1, -1, fmt.Errorf("fetch checkpoint do parse offset-end[%v] failed[%v]",
							next, err)
					}
				}

				if strings.Contains(lineS, utils.CheckpointRunId) {
					runId = utils.Bytes2String(replyList[i + 1].([]byte))
				}
			}
		}

		return runId, offsetBegin, offsetEnd, nil
	}
}

func ClearCheckpoint(target []string, authType, passwd string, isCluster bool, tlsEnable bool) {

}