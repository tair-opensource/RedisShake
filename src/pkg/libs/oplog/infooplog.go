package oplog

import (
	"fmt"
	"strconv"
)

type RedisInfoOplog struct {
	CurrentOpid int64
	GtidSet     map[uint64]int64 // serverId --> opid
	OpdelOpid   map[string]int64
}

// ParseRedisOplogInfo convert result of info oplog to map[uint64]int64(<serverID> --> <appliedOpid>).
//
// info oplog reply example:
//   # Oplog
//   current_opid:1
//   opapply_source_count:1
//   opapply_source_0:server_id=3171317,applied_opid=1
//   opdel_source_count:2
//   opdel_source_0:source_name=bls_channel_02,to_del_opid=1,last_update_time=1500279590
//   opdel_source_1:source_name=bls_channel_01,to_del_opid=1,last_update_time=1500279587
func ParseRedisInfoOplog(oplogInfo []byte) (*RedisInfoOplog, error) {
	p := new(RedisInfoOplog)

	var err error
	var i int
	var opapplySourceCount uint64

	// "opapply_source_count:1\r\nopapply_source_0:server_id=3171317,applied_opid=1\r\n" is converted to map[string]string{"opapply_source_count": "1", "opapply_source_0": "server_id=3171317,applied_opid=1"}.
	KV := ParseInfo(oplogInfo)

	// current_opid
	if value, ok := KV["current_opid"]; ok {
		p.CurrentOpid, err = strconv.ParseInt(value, 10, 0)
		if err != nil {
			return nil, err
		}
	} else {
		p.CurrentOpid = -1
	}

	// opapply_source_count:xxx
	if value, ok := KV["opapply_source_count"]; ok {
		opapplySourceCount, err = strconv.ParseUint(value, 10, 0)
		if err != nil {
			return nil, err
		}
	} else {
		goto return_error
	}

	p.GtidSet = make(map[uint64]int64)
	for i = 0; i < int(opapplySourceCount); i++ {
		key := fmt.Sprintf("opapply_source_%d", i) // key  "opapply_source_0"
		value, ok := KV[key]                       // value "server_id=7031,applied_opid=9842282"
		if !ok {
			goto return_error
		}

		subKV := ParseValue(value)

		var serverID uint64
		var appliedOpid int64
		if serverIDStr, ok := subKV["server_id"]; ok {
			serverID, err = strconv.ParseUint(serverIDStr, 10, 0)
			if err != nil {
				return nil, err
			}
		} else {
			goto return_error
		}
		if appliedOpidStr, ok := subKV["applied_opid"]; ok {
			appliedOpid, err = strconv.ParseInt(appliedOpidStr, 10, 0)
			if err != nil {
				return nil, err
			}
		} else {
			goto return_error
		}
		p.GtidSet[serverID] = appliedOpid
	}
	return p, nil
return_error:
	return nil, fmt.Errorf("invalid opapply info:\n %s", string(oplogInfo))
}
