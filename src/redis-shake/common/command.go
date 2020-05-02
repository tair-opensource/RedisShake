package utils

import (
	"bytes"
	"fmt"
	"strconv"
	"strings"
	"sort"

	"redis-shake/configure"

	redigo "github.com/garyburd/redigo/redis"
)

const (
	ReplayString = "string"
	ReplayInt64s = "int64s"
)

type ClusterNodeInfo struct {
	Id          string
	Address     string
	Flags       string
	Master      string
	PingSent    string
	PongRecv    string
	ConfigEpoch string
	LinkStat    string
	Slot        string
}

// parse single info field: "info server", "info keyspace"
func ParseRedisInfo(content []byte) map[string]string {
	result := make(map[string]string, 10)
	lines := bytes.Split(content, []byte("\r\n"))
	for i := 0; i < len(lines); i++ {
		items := bytes.SplitN(lines[i], []byte(":"), 2)
		if len(items) != 2 {
			continue
		}
		result[string(items[0])] = string(items[1])
	}
	return result
}

// cut segment
func CutRedisInfoSegment(content []byte, field string) []byte {
	field1 := strings.ToLower(field)
	field2 := "# " + field1
	segmentSplitter := []byte{13, 10, 35, 32} // \r\n#
	lineSplitter := []byte{13, 10}
	segments := bytes.Split(content, segmentSplitter)
	for i, segment := range segments {
		lines := bytes.Split(segment, lineSplitter)
		if len(lines) == 0 {
			continue
		}

		cmd := strings.ToLower(string(lines[0]))
		if cmd == field1 || cmd == field2 {
			// match
			var newSeg []byte
			if i != 0 {
				newSeg = []byte{35, 32}
			}
			newSeg = append(newSeg, segment...)
			return newSeg
		}
	}
	return nil
}

func ParseKeyspace(content []byte) (map[int32]int64, error) {
	if bytes.HasPrefix(content, []byte("# Keyspace")) == false {
		return nil, fmt.Errorf("invalid info Keyspace: %s", string(content))
	}

	lines := bytes.Split(content, []byte("\n"))
	reply := make(map[int32]int64)
	for _, line := range lines {
		line = bytes.TrimSpace(line)
		if bytes.HasPrefix(line, []byte("db")) == true {
			// line "db0:keys=18,expires=0,avg_ttl=0"
			items := bytes.Split(line, []byte(":"))
			db, err := strconv.Atoi(string(items[0][2:]))
			if err != nil {
				return nil, err
			}
			nums := bytes.Split(items[1], []byte(","))
			if bytes.HasPrefix(nums[0], []byte("keys=")) == false {
				return nil, fmt.Errorf("invalid info Keyspace: %s", string(content))
			}
			keysNum, err := strconv.ParseInt(string(nums[0][5:]), 10, 0)
			if err != nil {
				return nil, err
			}
			reply[int32(db)] = int64(keysNum)
		} // end true
	} // end for
	return reply, nil
}

/*
 * 10.1.1.1:21331> cluster nodes
 * d49a4c7b516b8da222d46a0a589b77f381285977 10.1.1.1:21333@31333 master - 0 1557996786000 3 connected 10923-16383
 * f23ba7be501b2dcd4d6eeabd2d25551513e5c186 10.1.1.1:21336@31336 slave d49a4c7b516b8da222d46a0a589b77f381285977 0 1557996785000 6 connected
 * 75fffcd521738606a919607a7ddd52bcd6d65aa8 10.1.1.1:21331@31331 myself,master - 0 1557996784000 1 connected 0-5460
 * da3dd51bb9cb5803d99942e0f875bc5f36dc3d10 10.1.1.1:21332@31332 master - 0 1557996786260 2 connected 5461-10922
 * eff4e654d3cc361a8ec63640812e394a8deac3d6 10.1.1.1:21335@31335 slave da3dd51bb9cb5803d99942e0f875bc5f36dc3d10 0 1557996787261 5 connected
 * 486e081f8d47968df6a7e43ef9d3ba93b77d03b2 10.1.1.1:21334@31334 slave 75fffcd521738606a919607a7ddd52bcd6d65aa8 0 1557996785258 4 connected
 */
func ParseClusterNode(content []byte) []*ClusterNodeInfo {
	lines := bytes.Split(content, []byte("\n"))
	ret := make([]*ClusterNodeInfo, 0, len(lines))
	for _, line := range lines {
		if bytes.Compare(line, []byte{}) == 0 {
			continue
		}

		items := bytes.Split(line, []byte(" "))

		address := bytes.Split(items[1], []byte{'@'})
		flag := bytes.Split(items[2], []byte{','})
		var role string
		if len(flag) > 1 {
			role = string(flag[1])
		} else {
			role = string(flag[0])
		}
		var slot string
		if len(items) > 7 {
			slot = string(items[7])
		}
		ret = append(ret, &ClusterNodeInfo{
			Id:          string(items[0]),
			Address:     string(address[0]),
			Flags:       role,
			Master:      string(items[3]),
			PingSent:    string(items[4]),
			PongRecv:    string(items[5]),
			ConfigEpoch: string(items[6]),
			LinkStat:    string(items[7]),
			Slot:        slot,
		})
	}
	return ret
}

// needMaster: true(master), false(slave)
func ClusterNodeChoose(input []*ClusterNodeInfo, role string) []*ClusterNodeInfo {
	ret := make([]*ClusterNodeInfo, 0, len(input))
	for _, ele := range input {
		if ele.Flags == conf.StandAloneRoleMaster && role == conf.StandAloneRoleMaster ||
			ele.Flags == conf.StandAloneRoleSlave && role == conf.StandAloneRoleSlave ||
			role == conf.StandAloneRoleAll {
			ret = append(ret, ele)
		}
	}
	return ret
}

// return id list if  choose == "id", otherwise address
func GetAllClusterNode(client redigo.Conn, role string, choose string) ([]string, error) {
	ret, err := client.Do("cluster", "nodes")
	if err != nil {
		return nil, err
	}

	nodeList := ParseClusterNode(ret.([]byte))
	nodeListChoose := ClusterNodeChoose(nodeList, role)

	result := make([]string, 0, len(nodeListChoose))
	for _, ele := range nodeListChoose {
		if choose == "id" {
			result = append(result, ele.Id)
		} else {
			result = append(result, ele.Address)
		}
	}

	return result, nil
}

/***************************************/
type SlotOwner struct {
	Master            string
	Slave             []string
	SlotLeftBoundary  int
	SlotRightBoundary int
}

func GetSlotDistribution(target, authType, auth string, tlsEnable bool) ([]SlotOwner, error) {
	c := OpenRedisConn([]string{target}, authType, auth, false, tlsEnable)
	defer c.Close()

	content, err := c.Do("cluster", "slots")
	if err != nil {
		return nil, err
	}

	ret := make([]SlotOwner, 0, 3)
	// fetch each shard info
	for _, shard := range content.([]interface{}) {
		shardVar := shard.([]interface{})
		left := shardVar[0].(int64)
		right := shardVar[1].(int64)

		// iterator each role
		var master string
		slave := make([]string, 0, 2)
		for i := 2; i < len(shardVar); i++ {
			roleVar := shardVar[i].([]interface{})
			ip := roleVar[0]
			port := roleVar[1]
			combine := fmt.Sprintf("%s:%d", ip, port)
			if i == 2 {
				master = combine
			} else {
				slave = append(slave, combine)
			}
		}

		ret = append(ret, SlotOwner{
			Master:            master,
			Slave:             slave,
			SlotLeftBoundary:  int(left),
			SlotRightBoundary: int(right),
		})
	}

	// sort by the slot range
	sort.Slice(ret, func(i, j int) bool {
		return ret[i].SlotLeftBoundary < ret[j].SlotLeftBoundary
	})
	return ret, nil
}

func CheckSlotDistributionEqual(src, dst []SlotOwner) bool {
	if len(src) != len(dst) {
		return false
	}

	for i := 0; i < len(src); i++ {
		if src[i].SlotLeftBoundary != src[i].SlotLeftBoundary ||
			src[i].SlotRightBoundary != src[i].SlotRightBoundary {
			return false
		}
	}
	return true
}

// return -1, -1 means not found
func GetSlotBoundary(shardList []SlotOwner, address string) (int, int) {
	if shardList == nil || len(shardList) == 0 {
		return -1, -1
	}

	for _, shard := range shardList {
		if address == shard.Master {
			return shard.SlotLeftBoundary, shard.SlotRightBoundary
		}

		// if match one of the slave list
		match := false
		for _, slave := range shard.Slave {
			if address == slave {
				match = true
				break
			}
		}
		if match {
			return shard.SlotLeftBoundary, shard.SlotRightBoundary
		}
	}
	return -1, -1
}
