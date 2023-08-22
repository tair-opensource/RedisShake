package utils

import (
	"sync"
)

type UniqueQueue struct {
	innerChannel chan interface{}
	set          map[interface{}]bool
	lock         sync.Mutex
	closed       bool
	Ch           chan interface{}
}

func NewUniqueQueue(size int) *UniqueQueue {
	mc := new(UniqueQueue)
	mc.innerChannel = make(chan interface{}, size)
	mc.Ch = make(chan interface{})
	mc.set = make(map[interface{}]bool)
	go func() {
		for item := range mc.innerChannel {
			mc.lock.Lock()
			delete(mc.set, item)
			mc.lock.Unlock()
			mc.Ch <- item
		}
		close(mc.Ch)
	}()
	return mc
}

func (mc *UniqueQueue) Put(item interface{}) {
	mc.lock.Lock()
	if _, ok := mc.set[item]; ok {
		mc.lock.Unlock()
		return
	} else {
		mc.set[item] = true
		mc.lock.Unlock()
		mc.innerChannel <- item
	}
}

func (mc *UniqueQueue) Len() int {
	return len(mc.innerChannel)
}

func (mc *UniqueQueue) Close() {
	close(mc.innerChannel)
}
