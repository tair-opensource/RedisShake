package dbSync

import (
	"redis-shake/configure"

	"pkg/libs/atomic2"
	"time"
	"pkg/libs/log"
)

const (
	barrierStatusNo        = ""
	barrierStatusAdd       = "add barrier"
	barrierStatusHoldStart = "start barrier"   // "multi" hold the barrier
	barrierStatusHolding   = "holding barrier" // holding
	barrierStatusHoldEnd   = "release barrier" // "exec" release the previous barrier

	flushStatusNo      = 0
	flushStatusYes     = 1
	// flushStatusMustNot = 2
)

var (
	/*
	 * should add barrier?
	 */
	barrierMap = map[string]string {
		"select": barrierStatusAdd,
		"multi":  barrierStatusHoldStart,
		"exec":   barrierStatusHoldEnd,
	}
)

func (ds *DbSyncer) addSendId(sendId *atomic2.Int64, val int) {
	(*sendId).Add(int64(val))
	// redis client may flush the data in "send()" so we need to put the data into delay channel here
	if conf.Options.Metric {
		// delay channel
		ds.addDelayChan((*sendId).Get())
	}
}

func (ds *DbSyncer) addDelayChan(id int64) {
	// send
	/*
	 * available >=4096: 1:1 sampling
	 * available >=1024: 1:10 sampling
	 * available >=128: 1:100 sampling
	 * else: 1:1000 sampling
	 */
	available := cap(ds.delayChannel) - len(ds.delayChannel)
	if available >= 4096 ||
		available >= 1024 && id%10 == 0 ||
		available >= 128 && id%100 == 0 ||
		id%1000 == 0 {
		// non-blocking add
		select {
		case ds.delayChannel <- &delayNode{t: time.Now(), id: id}:
		default:
			// do nothing but print when channel is full
			log.Warnf("DbSyncer[%d] delayChannel is full", ds.id)
		}
	}
}

/*
 * @return barrier status
 *     string: barrier status code
 *     int: flushStatusNo: no barrier, flushStatusYes: with barrier
 */
func barrierStatus(cmd string, prevBarrierStatus string) (string, int) {
	switch prevBarrierStatus {
	case barrierStatusNo:
		fallthrough
	case barrierStatusAdd:
		fallthrough
	case barrierStatusHoldEnd:
		ret, ok := barrierMap[cmd]
		if !ok {
			return barrierStatusNo, flushStatusNo
		}

		return ret, flushStatusYes
	case barrierStatusHoldStart:
		fallthrough
	case barrierStatusHolding:
		ret := barrierMap[cmd]
		if ret == barrierStatusHoldEnd {
			return barrierStatusHoldEnd, flushStatusYes
		}
		return barrierStatusHolding, flushStatusNo
	default:
		log.Panicf("illegal barrier status[%v]", prevBarrierStatus)
	}

	return "", flushStatusNo
}