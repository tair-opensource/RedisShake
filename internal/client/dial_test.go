package client

import (
	"context"
	"net"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

// freeAddress returns an address that nobody listens on right now.
func freeAddress(t *testing.T) string {
	l, err := net.Listen("tcp", "127.0.0.1:0")
	require.NoError(t, err)
	address := l.Addr().String()
	require.NoError(t, l.Close())
	return address
}

func setDialRetryBackoff(t *testing.T, d time.Duration) {
	old := dialRetryBackoff
	dialRetryBackoff = d
	t.Cleanup(func() { dialRetryBackoff = old })
}

// The server becomes reachable only after the first attempts fail, as with a resolver that
// is slow to answer right after the pod starts.
func TestDialRetriesUntilServerIsReachable(t *testing.T) {
	setDialRetryBackoff(t, 100*time.Millisecond)
	address := freeAddress(t)

	accepted := make(chan struct{})
	go func() {
		time.Sleep(150 * time.Millisecond)
		l, err := net.Listen("tcp", address)
		if err != nil {
			return
		}
		defer l.Close()
		conn, err := l.Accept()
		if err != nil {
			return
		}
		conn.Close()
		close(accepted)
	}()

	conn, err := dial(context.Background(), address, false, TlsConfig{})
	require.NoError(t, err)
	conn.Close()

	select {
	case <-accepted:
	case <-time.After(5 * time.Second):
		t.Fatal("server did not accept the connection")
	}
}

func TestDialGivesUpAfterAllAttempts(t *testing.T) {
	setDialRetryBackoff(t, 10*time.Millisecond)
	address := freeAddress(t)

	start := time.Now()
	_, err := dial(context.Background(), address, false, TlsConfig{})
	require.Error(t, err)
	// Backoff before attempts 2..5 is 10+20+40+80 ms.
	require.GreaterOrEqual(t, time.Since(start), 150*time.Millisecond)
}

func TestDialStopsWhenContextIsCanceled(t *testing.T) {
	setDialRetryBackoff(t, time.Hour)
	address := freeAddress(t)

	ctx, cancel := context.WithCancel(context.Background())
	time.AfterFunc(100*time.Millisecond, cancel)

	start := time.Now()
	_, err := dial(ctx, address, false, TlsConfig{})
	require.Error(t, err)
	require.Less(t, time.Since(start), 5*time.Second)
}
