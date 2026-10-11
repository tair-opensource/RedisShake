package reader

import (
	"context"
	"net"
	"os"
	"os/exec"
	"strconv"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"RedisShake/internal/client"
	"RedisShake/internal/config"
	"RedisShake/internal/entry"
	"RedisShake/internal/log"

	"github.com/mcuadros/go-defaults"
	"github.com/stretchr/testify/require"
)

// These tests start a real redis-server and are skipped when it is not in PATH.
// A connection failure in the reader exits the process, so a regression fails
// the whole test binary rather than a single test.

func TestMain(m *testing.M) {
	defaults.SetDefaults(&config.Opt)
	// Short enough to keep connections alive against the 1-second idle limits below.
	pingInterval = 250 * time.Millisecond
	logDir, err := os.MkdirTemp("", "redis-shake-reader-test")
	if err != nil {
		panic(err)
	}
	defer os.RemoveAll(logDir)
	log.Init("info", "shake.log", logDir, false, 0, 0, 0, false)
	os.Exit(m.Run())
}

func startRedis(t *testing.T, args ...string) string {
	path, err := exec.LookPath("redis-server")
	if err != nil {
		t.Skip("redis-server not found in PATH")
	}
	l, err := net.Listen("tcp", "127.0.0.1:0")
	require.NoError(t, err)
	port := l.Addr().(*net.TCPAddr).Port
	require.NoError(t, l.Close())

	args = append([]string{"--port", strconv.Itoa(port), "--save", "", "--appendonly", "no", "--dir", t.TempDir()}, args...)
	cmd := exec.Command(path, args...)
	require.NoError(t, cmd.Start())
	t.Cleanup(func() {
		_ = cmd.Process.Kill()
		_ = cmd.Wait()
	})

	address := "127.0.0.1:" + strconv.Itoa(port)
	require.Eventually(t, func() bool {
		conn, err := net.Dial("tcp", address)
		if err != nil {
			return false
		}
		conn.Close()
		return true
	}, 5*time.Second, 50*time.Millisecond)
	return address
}

func redisDo(t *testing.T, address string, args ...interface{}) interface{} {
	c := client.NewRedisClient(context.Background(), address, "", "", false, client.TlsConfig{}, false)
	defer c.Close()
	return c.Do(args...)
}

// idleKillingProxy forwards TCP connections and resets those that carry no traffic
// in either direction for idleTimeout, like a NAT or VPN gateway forgetting a silent flow.
type idleKillingProxy struct {
	listener    net.Listener
	target      string
	idleTimeout time.Duration
	killed      atomic.Int64
}

func startIdleKillingProxy(t *testing.T, target string, idleTimeout time.Duration) *idleKillingProxy {
	l, err := net.Listen("tcp", "127.0.0.1:0")
	require.NoError(t, err)
	p := &idleKillingProxy{listener: l, target: target, idleTimeout: idleTimeout}
	t.Cleanup(func() { l.Close() })
	go p.serve()
	return p
}

func (p *idleKillingProxy) address() string {
	return p.listener.Addr().String()
}

func (p *idleKillingProxy) serve() {
	for {
		down, err := p.listener.Accept()
		if err != nil {
			return
		}
		up, err := net.Dial("tcp", p.target)
		if err != nil {
			down.Close()
			continue
		}
		go p.handle(down.(*net.TCPConn), up.(*net.TCPConn))
	}
}

func (p *idleKillingProxy) handle(down, up *net.TCPConn) {
	var lastActivity atomic.Int64
	lastActivity.Store(time.Now().UnixNano())
	var closeOnce sync.Once
	closed := make(chan struct{})
	closeBoth := func() {
		closeOnce.Do(func() {
			close(closed)
			down.Close()
			up.Close()
		})
	}
	pipe := func(dst, src net.Conn) {
		buf := make([]byte, 32*1024)
		for {
			n, err := src.Read(buf)
			if n > 0 {
				lastActivity.Store(time.Now().UnixNano())
				if _, werr := dst.Write(buf[:n]); werr != nil {
					break
				}
			}
			if err != nil {
				break
			}
		}
		closeBoth()
	}
	go pipe(up, down)
	go pipe(down, up)

	ticker := time.NewTicker(p.idleTimeout / 10)
	defer ticker.Stop()
	for {
		select {
		case <-closed:
			return
		case <-ticker.C:
			if time.Since(time.Unix(0, lastActivity.Load())) > p.idleTimeout {
				p.killed.Add(1)
				// Linger 0 makes Close send RST, as a gateway does to a flow it no longer knows.
				_ = down.SetLinger(0)
				_ = up.SetLinger(0)
				closeBoth()
				return
			}
		}
	}
}

// startKsnReader starts a scan reader with keyspace notifications. On cleanup it
// cancels the context and waits for the reader to finish before the servers stop,
// so that a stopped server does not fail the reader.
func startKsnReader(t *testing.T, address string) <-chan *entry.Entry {
	ctx, cancel := context.WithCancel(context.Background())
	opts := &ScanReaderOptions{}
	defaults.SetDefaults(opts)
	opts.Address = address
	opts.KSN = true
	opts.Count = 10

	r := NewScanStandaloneReader(ctx, opts)
	chs := r.StartRead(ctx)
	entries := make(chan *entry.Entry, 1024)
	finished := make(chan struct{})
	go func() {
		for e := range chs[0] {
			select {
			case entries <- e:
			default: // nobody waits for this entry
			}
		}
		close(finished)
	}()
	t.Cleanup(func() {
		cancel()
		select {
		case <-finished:
		case <-time.After(5 * time.Second):
			t.Error("reader did not stop after cancel")
		}
		// The subscription is blocked in Receive until the next PONG wakes it up.
		time.Sleep(3 * pingInterval)
	})
	return entries
}

func waitEntry(t *testing.T, entries <-chan *entry.Entry, cmd string, key string) {
	timeout := time.After(10 * time.Second)
	for {
		select {
		case e := <-entries:
			if len(e.Argv) > 1 && e.Argv[0] == cmd && e.Argv[1] == key {
				return
			}
		case <-timeout:
			t.Fatalf("%s %q was not received", cmd, key)
		}
	}
}

// The source sits behind a gateway that resets silent connections. After the scan,
// the subscription and the DUMP connection idle while no keys change; heartbeats
// must keep them alive so that a later change is still delivered.
func TestScanReaderKsnSurvivesIdleConnectionDrop(t *testing.T) {
	const idleTimeout = time.Second
	src := startRedis(t, "--notify-keyspace-events", "EA")
	proxy := startIdleKillingProxy(t, src, idleTimeout)

	redisDo(t, src, "SET", "before", "1")
	entries := startKsnReader(t, proxy.address())
	waitEntry(t, entries, "RESTORE", "before")

	time.Sleep(3 * idleTimeout)
	redisDo(t, src, "SET", "after", "1")
	waitEntry(t, entries, "RESTORE", "after")

	require.Zero(t, proxy.killed.Load(), "proxy reset idle connections")
}

// Redis closes normal clients idle for longer than `timeout`. Subscribers are exempt,
// but the DUMP connection is not: it idles after the scan while no keys change.
func TestScanReaderKsnSurvivesServerIdleTimeout(t *testing.T) {
	const serverTimeout = time.Second
	src := startRedis(t, "--notify-keyspace-events", "EA", "--timeout", "1")

	redisDo(t, src, "SET", "before", "1")
	entries := startKsnReader(t, src)
	waitEntry(t, entries, "RESTORE", "before")

	time.Sleep(3 * serverTimeout)
	redisDo(t, src, "SET", "after", "1")
	waitEntry(t, entries, "RESTORE", "after")
}

// Keys deleted on the source are deleted on the target through keyspace notifications,
// with PING replies interleaved on the subscription.
func TestScanReaderKsnForwardsDelWithHeartbeat(t *testing.T) {
	src := startRedis(t, "--notify-keyspace-events", "EA")

	redisDo(t, src, "SET", "k", "1")
	entries := startKsnReader(t, src)
	waitEntry(t, entries, "RESTORE", "k")

	time.Sleep(300 * time.Millisecond)
	redisDo(t, src, "DEL", "k")
	waitEntry(t, entries, "DEL", "k")
}

// Shutdown requested while the scan is still running: both the scan and the
// subscription stop and close the dump queue.
func TestScanReaderKsnStopsDuringScan(t *testing.T) {
	src := startRedis(t, "--notify-keyspace-events", "EA")
	args := []interface{}{"MSET"}
	for i := 0; i < 20000; i++ {
		args = append(args, "key:"+strconv.Itoa(i), "1")
	}
	redisDo(t, src, args...)

	entries := startKsnReader(t, src)
	timeout := time.After(10 * time.Second)
	for received := 0; received < 100; {
		select {
		case <-entries:
			received++
		case <-timeout:
			t.Fatal("scan did not start")
		}
	}
	// Cleanup cancels the reader in the middle of the scan and waits for it to stop.
}
