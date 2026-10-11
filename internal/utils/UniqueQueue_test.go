package utils

import (
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

func drain(q *UniqueQueue) []interface{} {
	items := make([]interface{}, 0)
	for item := range q.Ch {
		items = append(items, item)
	}
	return items
}

func TestUniqueQueueDeliversQueuedItemsAfterClose(t *testing.T) {
	q := NewUniqueQueue(10)
	q.Put(1)
	q.Put(2)
	q.Put(1) // duplicate of a queued item
	q.Close()
	require.Equal(t, []interface{}{1, 2}, drain(q))
}

func TestUniqueQueueCloseTwice(t *testing.T) {
	q := NewUniqueQueue(10)
	q.Close()
	q.Close()
	require.Empty(t, drain(q))
}

func TestUniqueQueuePutAfterClose(t *testing.T) {
	q := NewUniqueQueue(10)
	q.Close()
	q.Put(1)
	require.Empty(t, drain(q))
}

func TestUniqueQueueCloseReleasesBlockedPut(t *testing.T) {
	q := NewUniqueQueue(1)
	q.Put(1)
	q.Put(2) // the consumer goroutine holds 1, 2 fills the buffer
	putDone := make(chan struct{})
	go func() {
		q.Put(3) // blocks: nobody reads Ch
		close(putDone)
	}()
	time.Sleep(50 * time.Millisecond)
	q.Close()
	select {
	case <-putDone:
	case <-time.After(5 * time.Second):
		t.Fatal("Put stayed blocked after Close")
	}
	drain(q)
}

func TestUniqueQueueConcurrentPutAndClose(t *testing.T) {
	for i := 0; i < 100; i++ {
		q := NewUniqueQueue(4)
		go drain(q)
		var wg sync.WaitGroup
		for p := 0; p < 4; p++ {
			wg.Add(1)
			go func(p int) {
				defer wg.Done()
				for k := 0; k < 100; k++ {
					q.Put(p*1000 + k)
				}
			}(p)
		}
		for c := 0; c < 2; c++ {
			wg.Add(1)
			go func() {
				defer wg.Done()
				q.Close()
			}()
		}
		wg.Wait()
	}
}
