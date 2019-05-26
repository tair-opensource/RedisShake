package utils

import (
	redigoCluster "github.com/vinllen/redis-go-cluster"
	redigo "github.com/garyburd/redigo/redis"
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
	ret, err := cc.client.RunBatch(cc.batcher)
	cc.batcher = nil // reset batcher
	cc.recvChan <- reply{
		answer: ret,
		err: err,
	}

	return err
}

// read recvChan
func (cc *ClusterConn) Receive() (reply interface{}, err error) {
	ret := <- cc.recvChan
	return ret.answer, ret.err
}