package inject

import (
	redigo "github.com/garyburd/redigo/redis"
	"fmt"
	"math/rand"
	"time"
)

type TpHash struct {
	client redigo.Conn
	keyNum int
	fieldNum int
	r *rand.Rand
}

func NewTpHash(conn redigo.Conn, keyNum, fieldNum int) *TpHash {
	return &TpHash{
		client: conn,
		keyNum: keyNum,
		fieldNum: fieldNum,
		r: rand.New(rand.NewSource(time.Now().UnixNano())),
	}
}

func (th *TpHash) GetType() string {
	return Hash
}

func (th *TpHash) Run() (map[string]interface{}, error) {
	mp := make(map[string]interface{})
	preDb := 0
	for i := 1; i <= th.keyNum; i++ {
		key := fmt.Sprintf("hash_%d", i)
		sonMp := make(map[string]int)
		for j := 1; j <= th.fieldNum; j++ {
			field := fmt.Sprintf("field_%d", j)
			val := th.r.Intn(th.keyNum)
			db := th.r.Intn(16) // random db
			if db != preDb {
				if _, err := th.client.Do("select", db); err != nil {
					return nil, fmt.Errorf("select db[%v] failed[%v]", db, err)
				}
				preDb = db
			}

			if _, err := th.client.Do("hset", key, field, val); err != nil {
				return nil, fmt.Errorf("hset key[%v] value[%v] field[%v] failed[%v]", key, val, field, err)
			}
			sonMp[field] = val
		}
		mp[key] = sonMp
	}

	return mp, nil
}