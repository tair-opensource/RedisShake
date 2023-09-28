package utils

import (
	"RedisShake/internal/client"
	"RedisShake/internal/log"
	"fmt"
	"strconv"
	"strings"
)

func GetRedisClusterNodes(address string, username string, password string, Tls bool) (addresses []string, slots [][]int) {
	c := client.NewRedisClient(address, username, password, Tls)
	reply := c.DoWithStringReply("cluster", "nodes")
	reply = strings.TrimSpace(reply)
	slotsCount := 0
	for _, line := range strings.Split(reply, "\n") {
		line = strings.TrimSpace(line)
		words := strings.Split(line, " ")
		if !strings.Contains(words[2], "master") {
			continue
		}
		if len(words) < 8 {
			log.Panicf("invalid cluster nodes line: %s", line)
		}
		log.Infof("redisClusterWriter load cluster nodes. line=%v", line)

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
		if len(words) < 9 {
			log.Warnf("the current master node does not hold any slots. address=[%v]", address)
			continue
		}
		addresses = append(addresses, address)

		// parse slots
		slot := make([]int, 0)
		for i := 8; i < len(words); i++ {
			words[i] = strings.TrimSpace(words[i])
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
			slots = append(slots, slot)
		}
	}
	if slotsCount != 16384 {
		log.Panicf("invalid cluster nodes slots. slots_count=%v, address=%v", slotsCount, address)
	}
	return addresses, slots
}
