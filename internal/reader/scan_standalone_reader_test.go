package reader

import (
	"context"
	"sync/atomic"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

func Test_scanStandaloneReader_waitForScanQueueCapacity(t *testing.T) {
	var queueLen atomic.Int64
	queueLen.Store(3)

	r := &scanStandaloneReader{
		ctx: context.Background(),
		opts: &ScanReaderOptions{
			ScanMaxQueueLen: 2,
		},
		queueLen: func() int {
			return int(queueLen.Load())
		},
	}

	done := make(chan bool, 1)
	go func() {
		done <- r.waitForScanQueueCapacity(10)
	}()

	select {
	case <-done:
		t.Fatal("waitForScanQueueCapacity returned before queue length dropped below threshold")
	case <-time.After(50 * time.Millisecond):
	}

	queueLen.Store(1)

	select {
	case ok := <-done:
		require.True(t, ok)
	case <-time.After(300 * time.Millisecond):
		t.Fatal("waitForScanQueueCapacity did not resume after queue length dropped below threshold")
	}
}

func Test_scanStandaloneReader_waitForScanQueueCapacity_ContextDone(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	cancel()

	r := &scanStandaloneReader{
		ctx: ctx,
		opts: &ScanReaderOptions{
			ScanMaxQueueLen: 1,
		},
		queueLen: func() int {
			return 1
		},
	}

	require.False(t, r.waitForScanQueueCapacity(10))
}
