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

	// closeLock keeps Close from closing innerChannel while a Put is sending to it.
	closeLock sync.RWMutex
	closeOnce sync.Once
	done      chan struct{}
}

func NewUniqueQueue(size int) *UniqueQueue {
	mc := new(UniqueQueue)
	mc.innerChannel = make(chan interface{}, size)
	mc.Ch = make(chan interface{})
	mc.set = make(map[interface{}]bool)
	mc.done = make(chan struct{})
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

// Put adds item unless it is already queued. It does nothing after Close.
func (mc *UniqueQueue) Put(item interface{}) {
	mc.closeLock.RLock()
	defer mc.closeLock.RUnlock()
	if mc.closed {
		return
	}
	mc.lock.Lock()
	if _, ok := mc.set[item]; ok {
		mc.lock.Unlock()
		return
	}
	mc.set[item] = true
	mc.lock.Unlock()
	select {
	case mc.innerChannel <- item:
	case <-mc.done:
	}
}

func (mc *UniqueQueue) Len() int {
	return len(mc.innerChannel)
}

// Close stops accepting items; items already queued are still delivered to Ch.
// It may be called more than once and concurrently with Put.
func (mc *UniqueQueue) Close() {
	mc.closeOnce.Do(func() {
		close(mc.done) // releases a Put blocked on a full queue
		mc.closeLock.Lock()
		mc.closed = true
		close(mc.innerChannel)
		mc.closeLock.Unlock()
	})
}
