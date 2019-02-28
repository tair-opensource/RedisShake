package oplog

import (
	"bytes"
)

/******  整个方法从 twemproxy 中借鉴过来 ******/

type RedisCmdCode int32

const (
	// redis
	GET RedisCmdCode = iota
	SET
	SETNX
	SETEX
	OPINFO
	PSETEX
	APPEND
	STRLEN
	DEL
	EXISTS
	SETBIT
	GETBIT
	BITFIELD
	SETRANGE
	GETRANGE
	SUBSTR
	INCR
	DECR
	MGET
	RPUSH
	LPUSH
	RPUSHX
	LPUSHX
	LINSERT
	RPOP
	LPOP
	BRPOP
	BRPOPLPUSH
	BLPOP
	LLEN
	LINDEX
	LSET
	LRANGE
	LTRIM
	LREM
	RPOPLPUSH
	SADD
	SREM
	SMOVE
	SISMEMBER
	SCARD
	SPOP
	SRANDMEMBER
	SINTER
	SINTERSTORE
	SUNION
	SUNIONSTORE
	SDIFF
	SDIFFSTORE
	SMEMBERS
	SSCAN
	ZADD
	ZINCRBY
	ZREM
	ZREMRANGEBYSCORE
	ZREMRANGEBYRANK
	ZREMRANGEBYLEX
	ZUNIONSTORE
	ZINTERSTORE
	ZRANGE
	ZRANGEBYSCORE
	ZREVRANGEBYSCORE
	ZRANGEBYLEX
	ZREVRANGEBYLEX
	ZCOUNT
	ZLEXCOUNT
	ZREVRANGE
	ZCARD
	ZSCORE
	ZRANK
	ZREVRANK
	ZSCAN
	HSET
	HSETNX
	HGET
	HMSET
	HMGET
	HINCRBY
	HINCRBYFLOAT
	HDEL
	HLEN
	HSTRLEN
	HKEYS
	HVALS
	HGETALL
	HEXISTS
	HSCAN
	INCRBY
	DECRBY
	INCRBYFLOAT
	GETSET
	MSET
	MSETNX
	RANDOMKEY
	SELECT
	MOVE
	RENAME
	RENAMENX
	EXPIRE
	EXPIREAT
	PEXPIRE
	PEXPIREAT
	KEYS
	SCAN
	ISCAN
	DBSIZE
	AUTH
	PING
	ECHO
	// SAVE
	// BGSAVE
	// BGREWRITEAOF
	// SHUTDOWN
	// LASTSAVE
	TYPE
	MULTI
	EXEC
	DISCARD
	// SYNC
	// PSYNC
	// REPLCONF
	FLUSHDB
	FLUSHALL
	SORT
	INFO
	IINFO
	// MONITOR
	TTL
	PTTL
	PERSIST
	// SLAVEOF
	// ROLE
	// DEBUG
	CONFIG
	SUBSCRIBE
	UNSUBSCRIBE
	PSUBSCRIBE
	PUNSUBSCRIBE
	PUBLISH
	PUBSUB
	WATCH
	UNWATCH
	// CLUSTER
	RESTORE
	// RESTORE-ASKING
	// MIGRATE
	// ASKING
	// READONLY
	// READWRITE
	DUMP
	OBJECT
	// CLIENT
	EVAL
	EVALSHA
	SLOWLOG
	SCRIPT
	TIME
	BITOP
	BITCOUNT
	BITPOS
	// WAIT
	// COMMAND
	GEOADD
	GEORADIUS
	GEORADIUSBYMEMBER
	GEOHASH
	GEOPOS
	GEODIST
	// PFSELFTEST
	PFADD
	PFCOUNT
	PFMERGE
	// PFDEBUG
	// LATENCY

	QUIT
	UNKOWN
	CMD_CODE_END
)

type RedisCmd struct {
	CmdCode RedisCmdCode
	Args    [][]byte
}

func (p RedisCmd) String() string {
	var buf bytes.Buffer
	for _, args := range p.Args {
		buf.Write(args)
		buf.WriteByte(' ')
	}
	return buf.String()
}

func str3icmp(m []byte, c0, c1, c2 byte) bool {
	return (m[0] == c0 || m[0] == (c0^0x20)) &&
		(m[1] == c1 || m[1] == (c1^0x20)) &&
		(m[2] == c2 || m[2] == (c2^0x20))
}

func str4icmp(m []byte, c0, c1, c2, c3 byte) bool {
	return (m[0] == c0 || m[0] == (c0^0x20)) &&
		(m[1] == c1 || m[1] == (c1^0x20)) &&
		(m[2] == c2 || m[2] == (c2^0x20)) &&
		(m[3] == c3 || m[3] == (c3^0x20))
}

func str5icmp(m []byte, c0, c1, c2, c3, c4 byte) bool {
	return (m[0] == c0 || m[0] == (c0^0x20)) &&
		(m[1] == c1 || m[1] == (c1^0x20)) &&
		(m[2] == c2 || m[2] == (c2^0x20)) &&
		(m[3] == c3 || m[3] == (c3^0x20)) &&
		(m[4] == c4 || m[4] == (c4^0x20))
}

func str6icmp(m []byte, c0, c1, c2, c3, c4, c5 byte) bool {
	return (m[0] == c0 || m[0] == (c0^0x20)) &&
		(m[1] == c1 || m[1] == (c1^0x20)) &&
		(m[2] == c2 || m[2] == (c2^0x20)) &&
		(m[3] == c3 || m[3] == (c3^0x20)) &&
		(m[4] == c4 || m[4] == (c4^0x20)) &&
		(m[5] == c5 || m[5] == (c5^0x20))
}

func str7icmp(m []byte, c0, c1, c2, c3, c4, c5, c6 byte) bool {
	return (m[0] == c0 || m[0] == (c0^0x20)) &&
		(m[1] == c1 || m[1] == (c1^0x20)) &&
		(m[2] == c2 || m[2] == (c2^0x20)) &&
		(m[3] == c3 || m[3] == (c3^0x20)) &&
		(m[4] == c4 || m[4] == (c4^0x20)) &&
		(m[5] == c5 || m[5] == (c5^0x20)) &&
		(m[6] == c6 || m[6] == (c6^0x20))
}

func str8icmp(m []byte, c0, c1, c2, c3, c4, c5, c6, c7 byte) bool {
	return (m[0] == c0 || m[0] == (c0^0x20)) &&
		(m[1] == c1 || m[1] == (c1^0x20)) &&
		(m[2] == c2 || m[2] == (c2^0x20)) &&
		(m[3] == c3 || m[3] == (c3^0x20)) &&
		(m[4] == c4 || m[4] == (c4^0x20)) &&
		(m[5] == c5 || m[5] == (c5^0x20)) &&
		(m[6] == c6 || m[6] == (c6^0x20)) &&
		(m[7] == c7 || m[7] == (c7^0x20))
}

func str9icmp(m []byte, c0, c1, c2, c3, c4, c5, c6, c7, c8 byte) bool {
	return (m[0] == c0 || m[0] == (c0^0x20)) &&
		(m[1] == c1 || m[1] == (c1^0x20)) &&
		(m[2] == c2 || m[2] == (c2^0x20)) &&
		(m[3] == c3 || m[3] == (c3^0x20)) &&
		(m[4] == c4 || m[4] == (c4^0x20)) &&
		(m[5] == c5 || m[5] == (c5^0x20)) &&
		(m[6] == c6 || m[6] == (c6^0x20)) &&
		(m[7] == c7 || m[7] == (c7^0x20)) &&
		(m[8] == c8 || m[8] == (c8^0x20))
}

func str10icmp(m []byte, c0, c1, c2, c3, c4, c5, c6, c7, c8, c9 byte) bool {
	return (m[0] == c0 || m[0] == (c0^0x20)) &&
		(m[1] == c1 || m[1] == (c1^0x20)) &&
		(m[2] == c2 || m[2] == (c2^0x20)) &&
		(m[3] == c3 || m[3] == (c3^0x20)) &&
		(m[4] == c4 || m[4] == (c4^0x20)) &&
		(m[5] == c5 || m[5] == (c5^0x20)) &&
		(m[6] == c6 || m[6] == (c6^0x20)) &&
		(m[7] == c7 || m[7] == (c7^0x20)) &&
		(m[8] == c8 || m[8] == (c8^0x20)) &&
		(m[9] == c9 || m[9] == (c9^0x20))
}

func str11icmp(m []byte, c0, c1, c2, c3, c4, c5, c6, c7, c8, c9, c10 byte) bool {
	return (m[0] == c0 || m[0] == (c0^0x20)) &&
		(m[1] == c1 || m[1] == (c1^0x20)) &&
		(m[2] == c2 || m[2] == (c2^0x20)) &&
		(m[3] == c3 || m[3] == (c3^0x20)) &&
		(m[4] == c4 || m[4] == (c4^0x20)) &&
		(m[5] == c5 || m[5] == (c5^0x20)) &&
		(m[6] == c6 || m[6] == (c6^0x20)) &&
		(m[7] == c7 || m[7] == (c7^0x20)) &&
		(m[8] == c8 || m[8] == (c8^0x20)) &&
		(m[9] == c9 || m[9] == (c9^0x20)) &&
		(m[10] == c10 || m[10] == (c10^0x20))
}

func str12icmp(m []byte, c0, c1, c2, c3, c4, c5, c6, c7, c8, c9, c10, c11 byte) bool {
	return (m[0] == c0 || m[0] == (c0^0x20)) &&
		(m[1] == c1 || m[1] == (c1^0x20)) &&
		(m[2] == c2 || m[2] == (c2^0x20)) &&
		(m[3] == c3 || m[3] == (c3^0x20)) &&
		(m[4] == c4 || m[4] == (c4^0x20)) &&
		(m[5] == c5 || m[5] == (c5^0x20)) &&
		(m[6] == c6 || m[6] == (c6^0x20)) &&
		(m[7] == c7 || m[7] == (c7^0x20)) &&
		(m[8] == c8 || m[8] == (c8^0x20)) &&
		(m[9] == c9 || m[9] == (c9^0x20)) &&
		(m[10] == c10 || m[10] == (c10^0x20)) &&
		(m[11] == c11 || m[11] == (c11^0x20))
}

func str13icmp(m []byte, c0, c1, c2, c3, c4, c5, c6, c7, c8, c9, c10, c11, c12 byte) bool {
	return (str12icmp(m, c0, c1, c2, c3, c4, c5, c6, c7, c8, c9, c10, c11) &&
		(m[12] == c12 || m[12] == (c12^0x20)))
}

func str14icmp(m []byte, c0, c1, c2, c3, c4, c5, c6, c7, c8, c9, c10, c11, c12, c13 byte) bool {
	return (str13icmp(m, c0, c1, c2, c3, c4, c5, c6, c7, c8, c9, c10, c11, c12) &&
		(m[13] == c13 || m[13] == (c13^0x20)))
}

func str15icmp(m []byte, c0, c1, c2, c3, c4, c5, c6, c7, c8, c9, c10, c11, c12, c13, c14 byte) bool {
	return (str14icmp(m, c0, c1, c2, c3, c4, c5, c6, c7, c8, c9, c10, c11, c12, c13) &&
		(m[14] == c14 || m[14] == (c14^0x20)))
}

func str16icmp(m []byte, c0, c1, c2, c3, c4, c5, c6, c7, c8, c9, c10, c11, c12, c13, c14, c15 byte) bool {
	return (str15icmp(m, c0, c1, c2, c3, c4, c5, c6, c7, c8, c9, c10, c11, c12, c13, c14) &&
		(m[15] == c15 || m[15] == (c15^0x20)))
}

func str17icmp(m []byte, c0, c1, c2, c3, c4, c5, c6, c7, c8, c9, c10, c11, c12, c13, c14, c15, c16 byte) bool {
	return (str16icmp(m, c0, c1, c2, c3, c4, c5, c6, c7, c8, c9, c10, c11, c12, c13, c14, c15) &&
		(m[16] == c16 || m[16] == (c16^0x20)))
}

func ParseCommandStrToCode(cmd []byte) RedisCmdCode {
	switch len(cmd) {
	case 3:
		if str3icmp(cmd, 's', 'e', 't') {
			return SET
		}
		if str3icmp(cmd, 'd', 'e', 'l') {
			return DEL
		}
		if str3icmp(cmd, 'g', 'e', 't') {
			return GET
		}
		if str3icmp(cmd, 't', 't', 'l') {
			return TTL
		}
	case 4:
		if str4icmp(cmd, 'i', 'n', 'c', 'r') {
			return INCR
		}
		if str4icmp(cmd, 'd', 'e', 'c', 'r') {
			return DECR
		}
		if str4icmp(cmd, 'm', 'g', 'e', 't') {
			return MGET
		}
		if str4icmp(cmd, 'm', 's', 'e', 't') {
			return MSET
		}
		if str4icmp(cmd, 'p', 'i', 'n', 'g') {
			return PING
		}
		if str4icmp(cmd, 'r', 'p', 'o', 'p') {
			return RPOP
		}
		if str4icmp(cmd, 'l', 'p', 'o', 'p') {
			return LPOP
		}
		if str4icmp(cmd, 'l', 's', 'e', 't') {
			return LSET
		}
		if str4icmp(cmd, 'l', 'r', 'e', 'm') {
			return LREM
		}
		if str4icmp(cmd, 's', 'a', 'd', 'd') {
			return SADD
		}
		if str4icmp(cmd, 's', 'r', 'e', 'm') {
			return SREM
		}
		if str4icmp(cmd, 's', 'p', 'o', 'p') {
			return SPOP
		}
		if str4icmp(cmd, 'z', 'a', 'd', 'd') {
			return ZADD
		}
		if str4icmp(cmd, 'z', 'r', 'e', 'm') {
			return ZREM
		}
		if str4icmp(cmd, 'h', 's', 'e', 't') {
			return HSET
		}
		if str4icmp(cmd, 'h', 'g', 'e', 't') {
			return HGET
		}
		if str4icmp(cmd, 'h', 'd', 'e', 'l') {
			return HDEL
		}
		if str4icmp(cmd, 'h', 'l', 'e', 'n') {
			return HLEN
		}
		if str4icmp(cmd, 'k', 'e', 'y', 's') {
			return KEYS
		}
		if str4icmp(cmd, 'a', 'u', 't', 'h') {
			return AUTH
		}
		if str4icmp(cmd, 'e', 'c', 'h', 'o') {
			return ECHO
		}
		if str4icmp(cmd, 't', 'y', 'p', 'e') {
			return TYPE
		}
		if str4icmp(cmd, 's', 'o', 'r', 't') {
			return SORT
		}
		if str4icmp(cmd, 'e', 'v', 'a', 'l') {
			return EVAL
		}
		if str4icmp(cmd, 'i', 'n', 'f', 'o') {
			return INFO
		}
		if str4icmp(cmd, 'p', 't', 't', 'l') {
			return PTTL
		}
		if str4icmp(cmd, 'd', 'u', 'm', 'p') {
			return DUMP
		}
		if str4icmp(cmd, 't', 'i', 'm', 'e') {
			return TIME
		}
		if str4icmp(cmd, 'q', 'u', 'i', 't') {
			return QUIT
		}
		if str4icmp(cmd, 'e', 'x', 'e', 'c') {
			return EXEC
		}
		if str4icmp(cmd, 's', 'c', 'a', 'n') {
			return SCAN
		}
		if str4icmp(cmd, 'l', 'l', 'e', 'n') {
			return LLEN
		}
		if str4icmp(cmd, 'm', 'o', 'v', 'e') {
			return MOVE
		}
	case 5:
		if str5icmp(cmd, 's', 'e', 't', 'n', 'x') {
			return SETNX
		}
		if str5icmp(cmd, 's', 'e', 't', 'e', 'x') {
			return SETEX
		}
		if str5icmp(cmd, 'r', 'p', 'u', 's', 'h') {
			return RPUSH
		}
		if str5icmp(cmd, 'l', 'p', 'u', 's', 'h') {
			return LPUSH
		}
		if str5icmp(cmd, 'l', 't', 'r', 'i', 'm') {
			return LTRIM
		}
		if str5icmp(cmd, 's', 'm', 'o', 'v', 'e') {
			return SMOVE
		}
		if str5icmp(cmd, 's', 'c', 'a', 'r', 'd') {
			return SCARD
		}
		if str5icmp(cmd, 's', 'd', 'i', 'f', 'f') {
			return SDIFF
		}
		if str5icmp(cmd, 's', 's', 'c', 'a', 'n') {
			return SSCAN
		}
		if str5icmp(cmd, 'z', 'c', 'a', 'r', 'd') {
			return ZCARD
		}
		if str5icmp(cmd, 'z', 'r', 'a', 'n', 'k') {
			return ZRANK
		}
		if str5icmp(cmd, 'z', 's', 'c', 'a', 'n') {
			return ZSCAN
		}
		if str5icmp(cmd, 'h', 'm', 's', 'e', 't') {
			return HMSET
		}
		if str5icmp(cmd, 'h', 'm', 'g', 'e', 't') {
			return HMGET
		}
		if str5icmp(cmd, 'h', 'k', 'e', 'y', 's') {
			return HKEYS
		}
		if str5icmp(cmd, 'h', 'v', 'a', 'l', 's') {
			return HVALS
		}
		if str5icmp(cmd, 'h', 's', 'c', 'a', 'n') {
			return HSCAN
		}
		if str5icmp(cmd, 'i', 's', 'c', 'a', 'n') {
			return ISCAN
		}
		if str5icmp(cmd, 'b', 'i', 't', 'o', 'p') {
			return BITOP
		}
		if str5icmp(cmd, 'p', 'f', 'a', 'd', 'd') {
			return PFADD
		}
		if str5icmp(cmd, 'm', 'u', 'l', 't', 'i') {
			return MULTI
		}
		if str5icmp(cmd, 'w', 'a', 't', 'c', 'h') {
			return WATCH
		}
		if str5icmp(cmd, 'b', 'r', 'p', 'o', 'p') {
			return BRPOP
		}
		if str5icmp(cmd, 'b', 'l', 'p', 'o', 'p') {
			return BLPOP
		}
		if str5icmp(cmd, 'i', 'i', 'n', 'f', 'o') {
			return IINFO
		}
	case 6:
		if str6icmp(cmd, 'o', 'p', 'i', 'n', 'f', 'o') {
			return OPINFO
		}
		if str6icmp(cmd, 'p', 's', 'e', 't', 'e', 'x') {
			return PSETEX
		}
		if str6icmp(cmd, 'a', 'p', 'p', 'e', 'n', 'd') {
			return APPEND
		}
		if str6icmp(cmd, 's', 't', 'r', 'l', 'e', 'n') {
			return STRLEN
		}
		if str6icmp(cmd, 'e', 'x', 'i', 's', 't', 's') {
			return EXISTS
		}
		if str6icmp(cmd, 's', 'e', 't', 'b', 'i', 't') {
			return SETBIT
		}
		if str6icmp(cmd, 'g', 'e', 't', 'b', 'i', 't') {
			return GETBIT
		}
		if str6icmp(cmd, 'r', 'p', 'u', 's', 'h', 'x') {
			return RPUSHX
		}
		if str6icmp(cmd, 'l', 'p', 'u', 's', 'h', 'x') {
			return LPUSHX
		}
		if str6icmp(cmd, 'l', 'i', 'n', 'd', 'e', 'x') {
			return LINDEX
		}
		if str6icmp(cmd, 'l', 'r', 'a', 'n', 'g', 'e') {
			return LRANGE
		}
		if str6icmp(cmd, 's', 'i', 'n', 't', 'e', 'r') {
			return SINTER
		}
		if str6icmp(cmd, 's', 'u', 'n', 'i', 'o', 'n') {
			return SUNION
		}
		if str6icmp(cmd, 'z', 'r', 'a', 'n', 'g', 'e') {
			return ZRANGE
		}
		if str6icmp(cmd, 'z', 'c', 'o', 'u', 'n', 't') {
			return ZCOUNT
		}
		if str6icmp(cmd, 'z', 's', 'c', 'o', 'r', 'e') {
			return ZSCORE
		}
		if str6icmp(cmd, 'h', 's', 'e', 't', 'n', 'x') {
			return HSETNX
		}
		if str6icmp(cmd, 'i', 'n', 'c', 'r', 'b', 'y') {
			return INCRBY
		}
		if str6icmp(cmd, 'd', 'e', 'c', 'r', 'b', 'y') {
			return DECRBY
		}
		if str6icmp(cmd, 'g', 'e', 't', 's', 'e', 't') {
			return GETSET
		}
		if str6icmp(cmd, 'm', 's', 'e', 't', 'n', 'x') {
			return MSETNX
		}
		if str6icmp(cmd, 'r', 'e', 'n', 'a', 'm', 'e') {
			return RENAME
		}
		if str6icmp(cmd, 'e', 'x', 'p', 'i', 'r', 'e') {
			return EXPIRE
		}
		if str6icmp(cmd, 'g', 'e', 'o', 'a', 'd', 'd') {
			return GEOADD
		}
		if str6icmp(cmd, 'g', 'e', 'o', 'p', 'o', 's') {
			return GEOPOS
		}
		if str6icmp(cmd, 's', 'c', 'r', 'i', 'p', 't') {
			return SCRIPT
		}
		if str6icmp(cmd, 'd', 'b', 's', 'i', 'z', 'e') {
			return DBSIZE
		}
		if str6icmp(cmd, 'c', 'o', 'n', 'f', 'i', 'g') {
			return CONFIG
		}
		if str6icmp(cmd, 'o', 'b', 'j', 'e', 'c', 't') {
			return OBJECT
		}
		if str6icmp(cmd, 'b', 'i', 't', 'p', 'o', 's') {
			return BITPOS
		}
		if str6icmp(cmd, 's', 'u', 'b', 's', 't', 'r') {
			return SUBSTR
		}
		if str6icmp(cmd, 'p', 'u', 'b', 's', 'u', 'b') {
			return PUBSUB
		}
		if str6icmp(cmd, 's', 'e', 'l', 'e', 'c', 't') {
			return SELECT
		}
	case 7:
		if str7icmp(cmd, 'l', 'i', 'n', 's', 'e', 'r', 't') {
			return LINSERT
		}
		if str7icmp(cmd, 'z', 'i', 'n', 'c', 'r', 'b', 'y') {
			return ZINCRBY
		}
		if str7icmp(cmd, 'h', 'i', 'n', 'c', 'r', 'b', 'y') {
			return HINCRBY
		}
		if str7icmp(cmd, 'h', 's', 't', 'r', 'l', 'e', 'n') {
			return HSTRLEN
		}
		if str7icmp(cmd, 'h', 'g', 'e', 't', 'a', 'l', 'l') {
			return HGETALL
		}
		if str7icmp(cmd, 'h', 'e', 'x', 'i', 's', 't', 's') {
			return HEXISTS
		}
		if str7icmp(cmd, 'p', 'e', 'x', 'p', 'i', 'r', 'e') {
			return PEXPIRE
		}
		if str7icmp(cmd, 'g', 'e', 'o', 'h', 'a', 's', 'h') {
			return GEOHASH
		}
		if str7icmp(cmd, 'g', 'e', 'o', 'd', 'i', 's', 't') {
			return GEODIST
		}
		if str7icmp(cmd, 'p', 'e', 'r', 's', 'i', 's', 't') {
			return PERSIST
		}
		if str7icmp(cmd, 'r', 'e', 's', 't', 'o', 'r', 'e') {
			return RESTORE
		}
		if str7icmp(cmd, 'p', 'f', 'c', 'o', 'u', 'n', 't') {
			return PFCOUNT
		}
		if str7icmp(cmd, 'p', 'f', 'm', 'e', 'r', 'g', 'e') {
			return PFMERGE
		}
		if str7icmp(cmd, 'e', 'v', 'a', 'l', 's', 'h', 'a') {
			return EVALSHA
		}
		if str7icmp(cmd, 'p', 'u', 'b', 'l', 'i', 's', 'h') {
			return PUBLISH
		}
		if str7icmp(cmd, 'd', 'i', 's', 'c', 'a', 'r', 'd') {
			return DISCARD
		}
		if str7icmp(cmd, 'u', 'n', 'w', 'a', 't', 'c', 'h') {
			return UNWATCH
		}
		if str7icmp(cmd, 's', 'l', 'o', 'w', 'l', 'o', 'g') {
			return SLOWLOG
		}
		if str7icmp(cmd, 'f', 'l', 'u', 's', 'h', 'd', 'b') {
			return FLUSHDB
		}
	case 8:
		if str8icmp(cmd, 's', 'e', 't', 'r', 'a', 'n', 'g', 'e') {
			return SETRANGE
		}
		if str8icmp(cmd, 'g', 'e', 't', 'r', 'a', 'n', 'g', 'e') {
			return GETRANGE
		}
		if str8icmp(cmd, 's', 'm', 'e', 'm', 'b', 'e', 'r', 's') {
			return SMEMBERS
		}
		if str8icmp(cmd, 'z', 'r', 'e', 'v', 'r', 'a', 'n', 'k') {
			return ZREVRANK
		}
		if str8icmp(cmd, 'r', 'e', 'n', 'a', 'm', 'e', 'n', 'x') {
			return RENAMENX
		}
		if str8icmp(cmd, 'e', 'x', 'p', 'i', 'r', 'e', 'a', 't') {
			return EXPIREAT
		}
		if str8icmp(cmd, 'f', 'l', 'u', 's', 'h', 'a', 'l', 'l') {
			return FLUSHALL
		}
		if str8icmp(cmd, 'b', 'i', 't', 'c', 'o', 'u', 'n', 't') {
			return BITCOUNT
		}
		if str8icmp(cmd, 'b', 'i', 't', 'f', 'i', 'e', 'l', 'd') {
			return BITFIELD
		}
	case 9:
		if str9icmp(cmd, 'r', 'p', 'o', 'p', 'l', 'p', 'u', 's', 'h') {
			return RPOPLPUSH
		}
		if str9icmp(cmd, 's', 'i', 's', 'm', 'e', 'm', 'b', 'e', 'r') {
			return SISMEMBER
		}
		if str9icmp(cmd, 'z', 'l', 'e', 'x', 'c', 'o', 'u', 'n', 't') {
			return ZLEXCOUNT
		}
		if str9icmp(cmd, 'z', 'r', 'e', 'v', 'r', 'a', 'n', 'g', 'e') {
			return ZREVRANGE
		}
		if str9icmp(cmd, 'p', 'e', 'x', 'p', 'i', 'r', 'e', 'a', 't') {
			return PEXPIREAT
		}
		if str9icmp(cmd, 'g', 'e', 'o', 'r', 'a', 'd', 'i', 'u', 's') {
			return GEORADIUS
		}
		if str9icmp(cmd, 'r', 'a', 'n', 'd', 'o', 'm', 'k', 'e', 'y') {
			return RANDOMKEY
		}
		if str9icmp(cmd, 's', 'u', 'b', 's', 'c', 'r', 'i', 'b', 'e') {
			return SUBSCRIBE
		}
	case 10:
		if str10icmp(cmd, 's', 'd', 'i', 'f', 'f', 's', 't', 'o', 'r', 'e') {
			return SDIFFSTORE
		}
		if str10icmp(cmd, 'p', 's', 'u', 'b', 's', 'c', 'r', 'i', 'b', 'e') {
			return PSUBSCRIBE
		}
		if str10icmp(cmd, 'b', 'r', 'p', 'o', 'p', 'l', 'p', 'u', 's', 'h') {
			return BRPOPLPUSH
		}
	case 11:
		if str11icmp(cmd, 's', 'r', 'a', 'n', 'd', 'm', 'e', 'm', 'b', 'e', 'r') {
			return SRANDMEMBER
		}
		if str11icmp(cmd, 's', 'i', 'n', 't', 'e', 'r', 's', 't', 'o', 'r', 'e') {
			return SINTERSTORE
		}
		if str11icmp(cmd, 's', 'u', 'n', 'i', 'o', 'n', 's', 't', 'o', 'r', 'e') {
			return SUNIONSTORE
		}
		if str11icmp(cmd, 'z', 'u', 'n', 'i', 'o', 'n', 's', 't', 'o', 'r', 'e') {
			return ZUNIONSTORE
		}
		if str11icmp(cmd, 'z', 'i', 'n', 't', 'e', 'r', 's', 't', 'o', 'r', 'e') {
			return ZINTERSTORE
		}
		if str11icmp(cmd, 'z', 'r', 'a', 'n', 'g', 'e', 'b', 'y', 'l', 'e', 'x') {
			return ZRANGEBYLEX
		}
		if str11icmp(cmd, 'i', 'n', 'c', 'r', 'b', 'y', 'f', 'l', 'o', 'a', 't') {
			return INCRBYFLOAT
		}
		if str11icmp(cmd, 'u', 'n', 's', 'u', 'b', 's', 'c', 'r', 'i', 'b', 'e') {
			return UNSUBSCRIBE
		}
	case 12:
		if str12icmp(cmd, 'h', 'i', 'n', 'c', 'r', 'b', 'y', 'f', 'l', 'o', 'a', 't') {
			return HINCRBYFLOAT
		}
		if str12icmp(cmd, 'p', 'u', 'n', 's', 'u', 'b', 's', 'c', 'r', 'i', 'b', 'e') {
			return PUNSUBSCRIBE
		}
	case 13:
		if str13icmp(cmd, 'z', 'r', 'a', 'n', 'g', 'e', 'b', 'y', 's', 'c', 'o', 'r', 'e') {
			return ZRANGEBYSCORE
		}
	case 14:
		if str14icmp(cmd, 'z', 'r', 'e', 'm', 'r', 'a', 'n', 'g', 'e', 'b', 'y', 'l', 'e', 'x') {
			return ZREMRANGEBYLEX
		}
		if str14icmp(cmd, 'z', 'r', 'e', 'v', 'r', 'a', 'n', 'g', 'e', 'b', 'y', 'l', 'e', 'x') {
			return ZREVRANGEBYLEX
		}
	case 15:
		if str15icmp(cmd, 'z', 'r', 'e', 'm', 'r', 'a', 'n', 'g', 'e', 'b', 'y', 'r', 'a', 'n', 'k') {
			return ZREMRANGEBYRANK
		}
	case 16:
		if str16icmp(cmd, 'z', 'r', 'e', 'm', 'r', 'a', 'n', 'g', 'e', 'b', 'y', 's', 'c', 'o', 'r', 'e') {
			return ZREMRANGEBYSCORE
		}
		if str16icmp(cmd, 'z', 'r', 'e', 'v', 'r', 'a', 'n', 'g', 'e', 'b', 'y', 's', 'c', 'o', 'r', 'e') {
			return ZREVRANGEBYSCORE
		}

	case 17:
		if str17icmp(cmd, 'g', 'e', 'o', 'r', 'a', 'd', 'i', 'u', 's', 'b', 'y', 'm', 'e', 'm', 'b', 'e', 'r') {
			return GEORADIUSBYMEMBER
		}
	} // switch()
	return UNKOWN
}
