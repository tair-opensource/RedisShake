package scanner

import (
	"redis-shake/configure"

	"github.com/garyburd/redigo/redis"
	"os"
	"pkg/libs/log"
	"bufio"
)

// scanner used to scan keys
type Scanner interface {
	// return scanned keys
	ScanKey() ([]string, error) // return the scanned keys

	// end current node
	EndNode() bool

	Close()
}

func NewScanner(client redis.Conn, tencentNodeId string, aliyunNodeId int) Scanner {
	if conf.Options.ScanSpecialCloud != "" {
		return &SpecialCloudScanner{
			client:        client,
			cursor:        0,
			tencentNodeId: tencentNodeId,
			aliyunNodeId:  aliyunNodeId,
		}
	} else if conf.Options.ScanKeyFile != "" {
		if f, err := os.Open(conf.Options.ScanKeyFile); err != nil {
			log.Errorf("open scan-key-file[%v] error[%v]", conf.Options.ScanKeyFile, err)
			return nil
		} else {
			return &KeyFileScanner{
				f:       f,
				bufScan: bufio.NewScanner(f),
				cnt:     -1,
			}
		}
	} else {
		return &NormalScanner{
			client: client,
			cursor: 0,
		}
	}
}
