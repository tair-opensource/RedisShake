package checkpoint

import (
	"redis-shake/common"
	redigo "github.com/garyburd/redigo/redis"
	"fmt"
	"strconv"
	"strings"
	"pkg/libs/log"
)

func LoadCheckpoint(dbSyncerId int, sourceAddr string, target []string, authType, passwd string,
		checkpointName string, isCluster bool, tlsEnable bool) (string, int64, int, error) {
	c := utils.OpenRedisConn(target, authType, passwd, isCluster, tlsEnable)

	// fetch logical db list
	ret, err := c.Do("info", "keyspace")
	if err != nil {
		return "", 0, 0, err
	}

	// TODO, for some kind of redis type, like codis, tencent cloud, the keyspace result may not be accurate
	// so there maybe some problems.
	mp, err := utils.ParseKeyspace(ret.([]byte))
	if err != nil {
		return "", 0, 0, err
	}

	var newestOffset int64 = -1
	var recRunId string
	var recDb int32
	var recVersion = -1
	for db := range mp {
		log.Infof("DbSyncer[%d] load checkpoint check db[%v]", dbSyncerId, db)
		runId, offset, version, err := fetchCheckpoint(sourceAddr, c, int(db), checkpointName)
		if err != nil {
			return "", 0, 0, err
		}

		// pick the biggest offset
		if offset > newestOffset {
			newestOffset = offset
			recRunId = runId
			recDb = db // which db
			recVersion = version
		}
	}

	if recVersion != -1 && recVersion < utils.FcvCheckpoint.FeatureCompatibleVersion {
		return "", 0, 0, fmt.Errorf("current required checkpoint version[%v] > input[%v], please upgrade RedisShake to version >= %v",
			utils.FcvCheckpoint.FeatureCompatibleVersion, recVersion,
			utils.LowestCheckpointVersion[utils.FcvCheckpoint.FeatureCompatibleVersion])
	}

	// do not set recDb when runId == "?" which means all checkpoint should be clean
	if recRunId == "?" {
		recDb = -1
	}

	log.Infof("DbSyncer[%d] newestOffset[%v], recordDb[%v]", dbSyncerId, newestOffset, recDb)
	if err := ClearCheckpoint(dbSyncerId, c, recDb, mp, sourceAddr, checkpointName); err != nil {
		log.Warnf("DbSyncer[%d] clear old checkpoint failed[%v]", dbSyncerId, err)
	}
	return recRunId, newestOffset, int(recDb), nil
}

/*
 * fetch checkpoint from give address
 * @return:
 *     string: runid
 *     int64: offset
 *     error
 */
func fetchCheckpoint(sourceAddr string, c redigo.Conn, db int, checkpointName string) (string, int64, int, error) {
	_, err := c.Do("select", db)
	if err != nil {
		return "", -1, -1, fmt.Errorf("fetch checkpoint do select db[%v] failed[%v]", db, err)
	}

	// judge checkpoint exists
	if reply, err := c.Do("exists", checkpointName); err != nil {
		return "", -1, -1, fmt.Errorf("fetch checkpoint do judge checkpoint exists failed[%v]", err)
	} else {
		if reply.(int64) == 0 {
			// not exist
			return "", -1, -1, nil
		}
	}

	// hgetall
	if reply, err := c.Do("hgetall", checkpointName); err != nil {
		return "", -1, -1, fmt.Errorf("fetch checkpoint do hgetall failed[%v]", err)
	} else {
		runId := "?"
		var offset int64 = -1
		var version int64 = 0
		replyList := reply.([]interface{})
		// read line by line and parse the offset
		for i := 0; i < len(replyList); i += 2 {
			line := replyList[i].([]byte)
			lineS := utils.Bytes2String(line)
			if strings.HasPrefix(lineS, sourceAddr) {
				if strings.Contains(lineS, utils.CheckpointOffset) {
					next := utils.Bytes2String(replyList[i + 1].([]byte))
					offset, err = strconv.ParseInt(next, 10, 64)
					if err != nil {
						return "", -1, -1, fmt.Errorf("fetch checkpoint do parse offset[%v] failed[%v]",
							next, err)
					}
				}

				if strings.Contains(lineS, utils.CheckpointRunId) {
					runId = utils.Bytes2String(replyList[i + 1].([]byte))
				}

				if strings.Contains(lineS, utils.CheckpointVersion) {
					str := utils.Bytes2String(replyList[i + 1].([]byte))
					version, err = strconv.ParseInt(str, 10, 64)
					if err != nil {
						return "", -1, -1, fmt.Errorf("fetch checkpoint do parse version[%v] failed[%v]",
							str, err)
					}
				}
			}
		}

		return runId, offset, int(version), nil
	}
}

func ClearCheckpoint(dbSyncerId int, c redigo.Conn, exceptDb int32, dbKeyMap map[int32]int64, sourceAddr string,
	checkpointName string) error {
	runId := fmt.Sprintf("%s-%s", sourceAddr, utils.CheckpointRunId)
	offset := fmt.Sprintf("%s-%s", sourceAddr, utils.CheckpointOffset)

	for db := range dbKeyMap {
		if db == exceptDb {
			continue
		}

		if _, err := c.Do("select", db); err != nil {
			return fmt.Errorf("do select db[%v] failed[%v]", db, err)
		}

		if ret, err := c.Do("hdel", checkpointName, runId, offset); err != nil {
			return err
		} else {
			log.Debugf("DbSyncer[%d] db[%v] remove checkpoint[%v] field[%v %v] with return[%v]",
				db, dbSyncerId, checkpointName, runId, offset, ret)
		}

		log.Infof("DbSyncer[%d] clear checkpoint of logical db[%v]", dbSyncerId, db)
	}
	return nil
}
