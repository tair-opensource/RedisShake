package inject

import (
	redigo "github.com/garyburd/redigo/redis"
	"fmt"
	"math/rand"
	"time"
)

type TpString struct {
	client redigo.Conn
	keyNum int
	r *rand.Rand
}

func NewTpString(conn redigo.Conn, keyNum int) *TpString {
	return &TpString{
		client: conn,
		keyNum: keyNum,
		r: rand.New(rand.NewSource(time.Now().UnixNano())),
	}
}

func (ts *TpString) GetType() string {
	return String
}

func (ts *TpString) Run() (map[string]interface{}, error) {
	mp := make(map[string]interface{})
	preDb := 0
	for i := 1; i <= ts.keyNum; i++ {
		key := fmt.Sprintf("string_%d", i)
		val := ts.r.Intn(ts.keyNum)
		db := ts.r.Intn(16) // random db
		if db != preDb {
			if _, err := ts.client.Do("select", db); err != nil {
				return nil, fmt.Errorf("select db[%v] failed[%v]", db, err)
			}
			preDb = db
		}

		if _, err := ts.client.Do("set", key, val); err != nil {
			return nil, fmt.Errorf("set key[%v] value[%v] failed[%v]", key, val, err)
		}
		mp[key] = val
	}
	return mp, nil
}
