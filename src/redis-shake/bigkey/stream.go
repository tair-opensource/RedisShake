package bigkey

import (
	"bytes"
	"encoding/binary"
	"fmt"
	"github.com/alibaba/RedisShake/pkg/libs/assert"
	"github.com/alibaba/RedisShake/pkg/libs/log"
	"github.com/alibaba/RedisShake/pkg/rdb"
	"github.com/alibaba/RedisShake/redis-shake/datastruct/listpack"
	redigo "github.com/garyburd/redigo/redis"
)

/*
 * The master entry is composed like in the following example:
 *
 *  +-------+---------+------------+---------+--/--+---------+---------+-+
 *	| count | deleted | num-fields | field_1 | field_2 | ... | field_N |0|
 *	+-------+---------+------------+---------+--/--+---------+---------+-+

 * Populate the Listpack with the new entry. We use the following
 * encoding:
 *
 * +-----+--------+----------+-------+-------+-/-+-------+-------+--------+
 * |flags|entry-id|num-fields|field-1|value-1|...|field-N|value-N|lp-count|
 * +-----+--------+----------+-------+-------+-/-+-------+-------+--------+
 *
 * However if the SAMEFIELD flag is set, we have just to populate
 * the entry with the values, so it becomes:
 *
 * +-----+--------+-------+-/-+-------+--------+
 * |flags|entry-id|value-1|...|value-N|lp-count|
 * +-----+--------+-------+-/-+-------+--------+
 *
 * The entry-id field is actually two separated fields: the ms
 * and seq difference compared to the master entry.
 *
 * The lp-count field is a number that states the number of Listpack pieces
 * that compose the entry, so that it's possible to travel the entry
 * in reverse order: we can just start from the end of the Listpack, read
 * the entry, and jump back N times to seek the "flags" field to read
 * the stream full entry. */

func RestoreBigStreamEntry(c redigo.Conn, e *rdb.BinEntry) {

	sendCount := 0

	r := rdb.NewRdbReader(bytes.NewReader(e.Value))
	_, _ = r.ReadByte() // 15

	// 1. length(number of listpack), k1, v1, k2, v2, ..., number, ms, seq

	/* Load the number of Listpack. */
	nListpack, _ := r.ReadLength64()
	for i := uint64(0); i < nListpack; i++ {
		/* Load key, value. */
		key, _ := r.ReadString()
		value, _ := r.ReadString()

		/* key is streamId, like: 1612181627287-0 */
		masterMs := int64(binary.BigEndian.Uint64(key[:8]))
		masterSeq := int64(binary.BigEndian.Uint64(key[8:]))

		/* value is a listpack */
		lp := listpack.NewListpack(value)

		/* The front of stream listpack is master entry */
		/* Parse the master entry */
		count := lp.NextInteger()              // count
		deleted := lp.NextInteger()            // deleted
		numFields := lp.NextInteger()          // num-fields
		fields := make([]string, 0, numFields) // fields
		for j := int64(0); j < numFields; j++ {
			fields = append(fields, lp.Next())
		}
		assert.Must(lp.NextInteger() == 0) // master entry end by zero

		/* Parse entries */
		for count != 0 || deleted != 0 {
			flags := lp.NextInteger() // [is_same_fields|is_deleted]
			entryMs := lp.NextInteger()
			entrySeq := lp.NextInteger()

			args := []interface{}{e.Key, fmt.Sprintf("%v-%v", entryMs+masterMs, entrySeq+masterSeq)}

			if flags&2 == 2 { // same fields, get field from master entry.
				for j := int64(0); j < numFields; j++ {
					args = append(args, fields[i], lp.Next())
				}
			} else { // get field by lp.Next()
				num := lp.NextInteger()
				for j := int64(0); j < num; j++ {
					args = append(args, lp.Next(), lp.Next())
				}
			}

			_ = lp.Next() // lp_count

			if flags&1 == 1 { // is_deleted
				deleted -= 1
			} else {
				count -= 1
				send(c, &sendCount, "XADD", args)
			}
		}
	}

	/* Load number */
	_, _ = r.ReadLength64() // number

	/* Load lastid */
	lastMs, _ := r.ReadLength64()
	lastSeq, _ := r.ReadLength64()
	lastid := fmt.Sprintf("%v-%v", lastMs, lastSeq)

	if nListpack == 0 {
		/* Use the XADD MAXLEN 0 trick to generate an empty stream if
		 * the key we are serializing is an empty string, which is possible
		 * for the Stream type. */
		args := []interface{}{e.Key, "MAXLEN", "0", lastid, "x", "y"}
		send(c, &sendCount, "XADD", args)
	}

	/* Append XSETID after XADD, make sure lastid is correct,
	 * in case of XDEL lastid. */
	send(c, &sendCount, "XSETID", []interface{}{e.Key, lastid})

	/* 2. nConsumerGroup, groupName, ms, seq, PEL, Consumers */

	/* Load the number of groups. */
	nConsumerGroup, _ := r.ReadLength64()
	for i := uint64(0); i < nConsumerGroup; i++ {

		/* Load the group name. */
		groupName, _ := r.ReadString()

		/* Load the last ID */
		ms, _ := r.ReadLength64()  // ms
		seq, _ := r.ReadLength64() // seq
		lastid := fmt.Sprintf("%v-%v", ms, seq)

		/* Create Group */
		args := []interface{}{"CREATE", e.Key, groupName, lastid}
		send(c, &sendCount, "XGROUP", args)

		/* Load the global PEL */
		nPEL, _ := r.ReadLength64()
		mapId2Time := make(map[string]uint64)
		mapId2Count := make(map[string]uint64)

		for i := uint64(0); i < nPEL; i++ {

			/* Load streamId */
			tmpBytes, _ := r.ReadBytes(16)
			ms := binary.BigEndian.Uint64(tmpBytes[:8])
			seq := binary.BigEndian.Uint64(tmpBytes[8:])
			streamId := fmt.Sprintf("%v-%v", ms, seq)

			/* Load deliveryTime */
			tmpBytes, _ = r.ReadBytes(8)
			deliveryTime := binary.LittleEndian.Uint64(tmpBytes)

			/* Load deliveryCount */
			deliveryCount, _ := r.ReadLength64()

			/* Save deliveryTime and deliveryCount  */
			mapId2Time[streamId] = deliveryTime
			mapId2Count[streamId] = deliveryCount
		}

		/* Generate XCLAIMs for each consumer that happens to
		 * have pending entries. Empty consumers are discarded. */
		nConsumer, _ := r.ReadLength64()
		for i := uint64(0); i < nConsumer; i++ {

			/* Load consumerName */
			consumerName, _ := r.ReadString()

			/* Load lastSeenTime */
			tmpBytes, _ := r.ReadBytes(8)
			_ = binary.LittleEndian.Uint64(tmpBytes) // lastSeenTime, used in bgsave, but not used in bgrewriteaof.

			/* Consumer PEL */
			nPEL, _ := r.ReadLength64()
			for i := uint64(0); i < nPEL; i++ {

				/* Load streamId */
				tmpBytes, _ := r.ReadBytes(16)
				ms := binary.BigEndian.Uint64(tmpBytes[:8])
				seq := binary.BigEndian.Uint64(tmpBytes[8:])
				streamId := fmt.Sprintf("%v-%v", ms, seq)

				/* Send */
				args := []interface{}{e.Key, groupName, consumerName, "0", streamId, "TIME", mapId2Time[streamId], "RETRYCOUNT", mapId2Count[streamId], "JUSTID", "FORCE"}
				send(c, &sendCount, "XCLAIM", args)
			}
		}
	}

	flushAndReceive(c, &sendCount)
}

func send(c redigo.Conn, count *int, cmd string, args []interface{}) {
	err := c.Send(cmd, args...)
	if err != nil {
		log.Panicf("cmd: %v, args: %v\n", cmd, args)
	}
	*count++
	if *count%100 == 0 {
		flushAndReceive(c, count)
	}
}

func flushAndReceive(c redigo.Conn, count *int) {
	err := c.Flush()
	if err != nil {
		log.PanicError(err, "flush command to redis failed")
	}
	for i := 0; i < *count; i++ {
		ret, err := c.Receive()
		if err != nil {
			log.Panicf("ret: %v\n", ret)
		}
	}
	*count = 0
}
