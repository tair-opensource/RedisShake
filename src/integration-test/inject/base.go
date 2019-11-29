package inject

import (
	redigo "github.com/garyburd/redigo/redis"
	"fmt"
)

const (
	String = "string"
	Hash   = "hash"
	List   = "list"
	Set    = "set"
	Zset   = "zset"
)

type Base interface {
	Run() (map[string]interface{}, error) // return key map
	GetType() string
}

func InjectData(conn redigo.Conn) (map[string]interface{}, error) {
	dataList := []Base{
		NewTpString(conn, 1000),
		NewTpHash(conn, 100, 20),
	}

	ret := make(map[string]interface{})
	for _, data := range dataList {
		mp, err := data.Run()
		if err != nil {
			return nil, fmt.Errorf("run type[%v] failed[%v]", data.GetType(), err)
		}

		ret[data.GetType()] = mp
	}

	return ret, nil
}