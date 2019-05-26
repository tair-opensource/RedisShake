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
	/*
	 * get db node info.
	 * return:
	 *     int: node number. Used in aliyun_cluster
	 */
	NodeCount() (int, error)

	// return scanned keys
	ScanKey(node interface{}) ([]string, error) // return the scanned keys

	// end current node
	EndNode() bool

	Close()
}

func NewScanner(client []redis.Conn) []Scanner {
	if conf.Options.ScanSpecialCloud != "" {
		return []Scanner{
			&SpecialCloudScanner{
				client: client[0],
				cursor: 0,
			},
		}
	} else if conf.Options.ScanKeyFile != "" {
		if f, err := os.Open(conf.Options.ScanKeyFile); err != nil {
			log.Errorf("open scan-key-file[%v] error[%v]", conf.Options.ScanKeyFile, err)
			return nil
		} else {
			return []Scanner{
				&KeyFileScanner{
					f:       f,
					bufScan: bufio.NewScanner(f),
					cnt:     -1,
				},
			}
		}
	} else {
		ret := make([]Scanner, 0, len(client))
		for _, c := range client {
			ret = append(ret, &NormalScanner{
				client: c,
				cursor: 0,
			})
		}
		return ret
	}
}
