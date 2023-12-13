package utils

import (
	"fmt"
	"strconv"
	"strings"

	"RedisShake/internal/client"
	"RedisShake/internal/log"
)

func GetRedisClusterNodes(address string, username string, password string, Tls bool, perferReplica bool) (addresses []string, slots [][]int) {
	c := client.NewRedisClient(address, username, password, Tls)
	reply := c.DoWithStringReply("cluster", "nodes")
	reply = strings.TrimSpace(reply)
	slotsCount := 0
	log.Infof("address=%v, reply=%v", address, reply)
	masters := make(map[string]string)
	replicas := make(map[string][]string)
	for _, line := range strings.Split(reply, "\n") {
		line = strings.TrimSpace(line)
		words := strings.Split(line, " ")
		isMaster := strings.Contains(words[2], "master")
		if len(words) < 8 {
			log.Panicf("invalid cluster nodes line: %s", line)
		}

		// address
		address := strings.Split(words[1], "@")[0]
		// handle ipv6 address
		tok := strings.Split(address, ":")
		if len(tok) > 2 {
			// ipv6 address
			port := tok[len(tok)-1]

			ipv6Addr := strings.Join(tok[:len(tok)-1], ":")
			address = fmt.Sprintf("[%s]:%s", ipv6Addr, port)
		}
		if isMaster && len(words) < 9 {
			log.Warnf("the current master node does not hold any slots. address=[%v]", address)
			continue
		}

		nodeId := words[0]
		if isMaster {
			masters[nodeId] = address
		} else {
			if strings.Contains(words[2], "fail") || strings.Contains(words[2], "noaddr") {
				continue
			}
			masterId := words[3]
			replicas[masterId] = append(replicas[masterId], address)
			continue
		}

		// parse slots
		slot := make([]int, 0)
		for i := 8; i < len(words); i++ {
			words[i] = strings.TrimSpace(words[i])
			if strings.HasPrefix(words[i], "[") {
				// issue: https://github.com/tair-opensource/RedisShake/issues/730
				// [****] appear at the end of each line of "cluster nodes",
				// indicating data migration between nodes is in progress.
				break
			}
			var start, end int
			var err error
			if strings.Contains(words[i], "-") {
				seg := strings.Split(words[i], "-")
				start, err = strconv.Atoi(seg[0])
				if err != nil {
					log.Panicf(err.Error())
				}
				end, err = strconv.Atoi(seg[1])
				if err != nil {
					log.Panicf(err.Error())
				}
			} else {
				start, err = strconv.Atoi(words[i])
				if err != nil {
					log.Panicf(err.Error())
				}
				end = start
			}
			for j := start; j <= end; j++ {
				slot = append(slot, j)
				slotsCount++
			}
		}
		slots = append(slots, slot)
	}
	if slotsCount != 16384 {
		log.Panicf("invalid cluster nodes slots. slots_count=%v, address=%v", slotsCount, address)
	}

	if perferReplica && len(replicas) > 0 {
		for masterId, replicaAddr := range replicas {
			if len(replicaAddr) > 0 {
				addresses = append(addresses, replicaAddr[0])
			} else {
				addresses = append(addresses, masters[masterId])
			}
		}
	} else {
		for _, v := range masters {
			addresses = append(addresses, v)
		}
	}

	return addresses, slots
}
