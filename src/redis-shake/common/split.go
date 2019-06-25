package utils

// big key split in rump
import (
	"pkg/rdb"
	"pkg/libs/log"

	redigo "github.com/garyburd/redigo/redis"
)

func RestoreBigkey(client redigo.Conn, key string, value string, pttl int64, db int) {
	if _, err := client.Do("select", db); err != nil {
		log.Panicf("send select db[%v] failed[%v]", db, err)
	}

	entry := rdb.BinEntry{
		DB:              uint32(db),
		Key:             String2Bytes(key),
		Type:            0, // uselss
		Value:           String2Bytes(value),
		ExpireAt:        0, // useless here
		RealMemberCount: 0,
		NeedReadLen:     1,
		IdleTime:        0,
		Freq:            0,
	}

	restoreBigRdbEntry(client, &entry)

	// pttl
	if _, err := client.Do("pexpire", key, pttl); err != nil {
		log.Panicf("send key[%v] pexpire failed[%v]", key, err)
	}
}
