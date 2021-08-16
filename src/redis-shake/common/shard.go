package utils

import (
	"fmt"
	"pkg/libs/log"
	"strconv"

	"github.com/emirpasic/gods/maps/treemap"
	"github.com/emirpasic/gods/utils"
)

const (
	Weight = 1
)

type ShardInfo struct {
	index  int // targetClientList里的偏移，初始化时，顺序需要与targetClientList保持一致
	weight int
	name   string
}

type ConsistentHashing struct {
	nodes *treemap.Map
}

func NewConsistentHashing(shardNameList []string) (*ConsistentHashing, error) {
	mappingAlgorithm := new(ConsistentHashing)
	root := treemap.NewWith(utils.Int64Comparator)

	for i, shardName := range shardNameList {
		if len(shardName) == 0 {
			return nil, fmt.Errorf("shard name is empty")
		}

		shardInfo := ShardInfo{
			index:  i,
			weight: Weight,
			name:   shardName,
		}

		for n := 0; n < 160*shardInfo.weight; n = n + 1 {
			root.Put(MurmurHash64A([]byte(shardInfo.name+"*"+strconv.Itoa(shardInfo.weight)+strconv.Itoa(n))), shardInfo)
		}
	}

	mappingAlgorithm.nodes = root
	return mappingAlgorithm, nil
}

func (p *ConsistentHashing) GetShardIndex(key []byte) string {
	_, node := p.nodes.Ceiling(MurmurHash64A(key))
	if node == nil {
		_, head := p.nodes.Min()
		shardInfo, err := head.(ShardInfo)
		if !err {
			log.Errorf("ShardInfo conversion error")
		}
		return shardInfo.name
	}

	shardInfo, err := node.(ShardInfo)
	if !err {
		log.Errorf("ShardInfo conversion error")
	}
	return shardInfo.name
}
