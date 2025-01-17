package utils

import (
	"context"
	"fmt"
	"strconv"
	"strings"

	"RedisShake/internal/client"
	"RedisShake/internal/log"
)

func GetRedisClusterNodes(ctx context.Context, address string, username string, password string, Tls bool, tlsConfig client.TlsConfig, perferReplica bool) (addresses []string, slots [][]int) {
	c := client.NewRedisClient(ctx, address, username, password, Tls, tlsConfig, false)
	reply := c.DoWithStringReply("cluster", "nodes")
	reply = strings.TrimSpace(reply)
	slotsCount := 0
	// map of master's nodeId to address
	masters := make(map[string]string)
	// map of master's nodeId to replica addresses
	replicas := make(map[string][]string)
	// keep nodeID sort by slots
	nodeIds := make([]string, 0)
	log.Infof("address=%v, reply=%v", address, reply)
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
			// execlude invalid replicas node
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
		nodeIds = append(nodeIds, nodeId)
	}
	if slotsCount != 16384 {
		log.Panicf("invalid cluster nodes slots. slots_count=%v, address=%v", slotsCount, address)
	}

	for _, id := range nodeIds {
		if replicaAddr, exist := replicas[id]; exist && perferReplica {
			addresses = append(addresses, replicaAddr[0])
		} else if masterAddr, exist := masters[id]; exist {
			addresses = append(addresses, masterAddr)
		} else {
			log.Panicf("unknown id=%s", id)
		}
	}

	return addresses, slots
}
