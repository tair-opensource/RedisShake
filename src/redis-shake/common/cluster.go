package utils

import (
	"pkg/libs/log"

	redigoCluster "github.com/vinllen/redis-go-cluster"
	redigo "github.com/garyburd/redigo/redis"
)

var (
	RecvChanSize = 4096
)

/* implement redigo.Conn(https://github.com/garyburd/redigo)
 * Embed redis-go-cluster(https://github.com/chasex/redis-go-cluster)
 * The reason I create this struct is that redis-go-cluster isn't fulfill redigo.Conn
 * interface. So I implement "Err", "Send", "Flush" and "Receive" interfaces.
 */
type ClusterConn struct {
	client   *redigoCluster.Cluster
	recvChan chan reply
	batcher  *redigoCluster.Batch
}

type reply struct {
	answer interface{}
	err    error
}

func NewClusterConn(clusterClient *redigoCluster.Cluster, recvChanSize int) redigo.Conn {
	if recvChanSize == 0 {
		recvChanSize = RecvChanSize
	}

	return &ClusterConn{
		client:   clusterClient,
		recvChan: make(chan reply, recvChanSize),
	}
}

func (cc *ClusterConn) Close() error {
	cc.client.Close()
	return nil
}

func (cc *ClusterConn) Err() error {
	return nil
}

func (cc *ClusterConn) Do(commandName string, args ...interface{}) (reply interface{}, err error) {
	return cc.client.Do(commandName, args...)
}

// just add into batcher
func (cc *ClusterConn) Send(commandName string, args ...interface{}) error {
	if cc.batcher == nil {
		cc.batcher = cc.client.NewBatch()
	}
	return cc.batcher.Put(commandName, args...)
}

// send batcher and put the return into recvChan
func (cc *ClusterConn) Flush() error {
	if cc.batcher == nil {
		log.Info("batcher is empty, no need to flush")
		return nil
	}

	ret, err := cc.client.RunBatch(cc.batcher)
	defer func() {
		cc.batcher = nil // reset batcher
	}()

	if err != nil {
		cc.recvChan <- reply{
			answer: nil,
			err:    err,
		}

		return err
	}

	// for redis-go-cluster driver, "Receive" function returns all the replies once flushed.
	// However, this action is different with redigo driver that "Receive" only returns 1
	// reply each time.

	retLength := len(ret)
	availableSize := cap(cc.recvChan) - len(cc.recvChan)
	if availableSize < retLength {
		log.Warnf("available channel size[%v] less than current returned batch size[%v]", availableSize, retLength)
	}
	log.Debugf("cluster flush batch with size[%v], return replies size[%v]", cc.batcher.GetBatchSize(), retLength)

	for _, ele := range ret {
		cc.recvChan <- reply{
			answer: ele,
			err:    err,
		}
	}

	return err
}

// read recvChan
func (cc *ClusterConn) Receive() (reply interface{}, err error) {
	ret := <- cc.recvChan
	return ret.answer, ret.err
}