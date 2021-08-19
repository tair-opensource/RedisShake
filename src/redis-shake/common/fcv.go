package utils

import (
	"bufio"
	"fmt"
	"os"
	"strconv"
	"strings"
)

var (
	FcvCheckpoint = Checkpoint{
		CurrentVersion:           1,
		FeatureCompatibleVersion: 1,
	}
	FcvConfiguration = Configuration{
		CurrentVersion:           1,
		FeatureCompatibleVersion: 0,
	}

	LowestCheckpointVersion = map[int]string{
		0: "1.0.0",
		1: "2.0.0",
	}
	LowestConfigurationVersion = map[int]string{
		0: "1.0.0",
		1: "2.0.0",
	}
)

type Fcv interface {
	IsCompatible(int) bool
}

// for checkpoint
type Checkpoint struct {
	/*
	 * version: 0(or set not), MongoShake < 2.4, fcv == 0
	 * version: 1, MongoShake == 2.4, 0 < fcv <= 1
	 */
	CurrentVersion           int
	FeatureCompatibleVersion int
}

func (c Checkpoint) IsCompatible(v int) bool {
	return v >= c.FeatureCompatibleVersion && v <= c.CurrentVersion
}

// for configuration
type Configuration struct {
	/*
	 * version: 0(or set not), MongoShake < 2.4.0, fcv == 0
	 * version: 1, MongoShake == 2.4.0, 0 <= fcv <= 1
	 */
	CurrentVersion           int
	FeatureCompatibleVersion int
}

func (c Configuration) IsCompatible(v int) bool {
	return v >= c.FeatureCompatibleVersion && v <= c.CurrentVersion
}

/*--------------------------------------------------*/
func CheckFcv(file string, fcv int) (int, error) {
	// read line by line and parse the version

	f, err := os.Open(file)
	if err != nil {
		return -1, err
	}

	scanner := bufio.NewScanner(f)
	versionName := "conf.version"
	version := 0
	for scanner.Scan() {
		field := strings.Split(scanner.Text(), "=")
		if len(field) >= 2 && strings.HasPrefix(field[0], versionName) {
			if value, err := strconv.Atoi(strings.Trim(field[1], " ")); err != nil {
				return 0, fmt.Errorf("illegal value[%v]", field[1])
			} else {
				version = value
				break
			}
		}
	}

	if version < fcv {
		return version, fmt.Errorf("current required configuration version[%v] > input[%v], please upgrade RedisShake to version >= %v",
			fcv, version, LowestConfigurationVersion[fcv])
	}
	return version, nil
}
