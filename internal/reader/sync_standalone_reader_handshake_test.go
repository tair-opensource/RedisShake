package reader

import (
	"bufio"
	"context"
	"fmt"
	"net"
	"reflect"
	"testing"
	"time"

	"RedisShake/internal/client"
	"RedisShake/internal/client/proto"

	"github.com/stretchr/testify/require"
)

type replicationHandshakeStep struct {
	args  []string
	reply string
}

func newReplicationTestClient(t *testing.T, steps []replicationHandshakeStep, onComplete func()) *client.Redis {
	t.Helper()
	listener, err := net.Listen("tcp", "127.0.0.1:0")
	require.NoError(t, err)
	steps = append([]replicationHandshakeStep{{args: []string{"ping"}, reply: "+PONG\r\n"}}, steps...)
	done := make(chan error, 1)
	go func() {
		done <- func() error {
			conn, err := listener.Accept()
			if err != nil {
				return err
			}
			defer conn.Close()
			if err := conn.SetDeadline(time.Now().Add(5 * time.Second)); err != nil {
				return err
			}
			reader := proto.NewReader(bufio.NewReader(conn))
			for _, step := range steps {
				reply, err := reader.ReadReply()
				if err != nil {
					return fmt.Errorf("waiting for %v: %w", step.args, err)
				}
				args := client.ArrayString(reply, nil)
				if !reflect.DeepEqual(args, step.args) {
					return fmt.Errorf("expected %v, got %v", step.args, args)
				}
				if _, err := conn.Write([]byte(step.reply)); err != nil {
					return err
				}
			}
			if onComplete != nil {
				onComplete()
			}
			return nil
		}()
	}()
	r := client.NewRedisClient(context.Background(), listener.Addr().String(), "", "", false, client.TlsConfig{}, false)
	t.Cleanup(func() {
		r.Close()
		listener.Close()
		select {
		case err := <-done:
			require.NoError(t, err)
		case <-time.After(5 * time.Second):
			t.Error("replication test server did not finish")
		}
	})
	return r
}

func TestSyncReaderPSyncCapabilities(t *testing.T) {
	eof := []string{"REPLCONF", "CAPA", "EOF"}
	filter := []string{"REPLCONF", "rdb-filter-only", ""}
	tests := []struct {
		name        string
		syncRdb     bool
		tryDiskless bool
		steps       []replicationHandshakeStep
		wantEOF     bool
	}{
		{name: "full sync", syncRdb: true},
		{
			name: "full sync with diskless", syncRdb: true, tryDiskless: true,
			steps: []replicationHandshakeStep{{eof, "+OK\r\n"}}, wantEOF: true,
		},
		{
			name:  "empty RDB without try_diskless",
			steps: []replicationHandshakeStep{{eof, "+OK\r\n"}, {filter, "+OK\r\n"}}, wantEOF: true,
		},
		{
			name: "empty RDB with try_diskless", tryDiskless: true,
			steps: []replicationHandshakeStep{{eof, "+OK\r\n"}, {filter, "+OK\r\n"}}, wantEOF: true,
		},
		{
			name:  "unsupported filter",
			steps: []replicationHandshakeStep{{eof, "+OK\r\n"}, {filter, "-ERR Unrecognized REPLCONF option: rdb-filter-only\r\n"}}, wantEOF: true,
		},
		{
			name:  "filter denied by ACL",
			steps: []replicationHandshakeStep{{eof, "+OK\r\n"}, {filter, "-NOPERM no permissions for this REPLCONF option\r\n"}}, wantEOF: true,
		},
		{
			name:  "unsupported EOF skips filter",
			steps: []replicationHandshakeStep{{eof, "-ERR Unrecognized REPLCONF option: CAPA\r\n"}},
		},
		{
			name: "rejected diskless capability", syncRdb: true, tryDiskless: true,
			steps: []replicationHandshakeStep{{eof, "-ERR Unrecognized REPLCONF option: CAPA\r\n"}},
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			steps := append([]replicationHandshakeStep(nil), tt.steps...)
			steps = append(steps,
				replicationHandshakeStep{[]string{"INFO", "persistence"}, "+rdb_bgsave_in_progress:0\r\n"},
				replicationHandshakeStep{[]string{"PSYNC", "?", "-1"}, "+FULLRESYNC 0123456789012345678901234567890123456789 0\r\n"},
			)
			r := &syncStandaloneReader{
				ctx: context.Background(),
				opts: &SyncReaderOptions{
					SyncRdb: tt.syncRdb, SyncAof: true, TryDiskless: tt.tryDiskless,
				},
				client: newReplicationTestClient(t, steps, nil),
			}
			r.sendPSync()
			require.Equal(t, tt.wantEOF, r.isDiskless)
			require.Zero(t, r.stat.AofReceivedOffset)
		})
	}
}

func TestSyncReaderOptionalReplconf(t *testing.T) {
	tests := []struct {
		name      string
		reply     string
		supported bool
		wantError bool
	}{
		{name: "supported", reply: "+OK\r\n", supported: true},
		{name: "unsupported", reply: "-ERR Unrecognized REPLCONF option\r\n"},
		{name: "denied", reply: "-NOPERM no permissions\r\n"},
		{name: "unexpected status", reply: "+UNKNOWN\r\n", wantError: true},
		{name: "unexpected integer", reply: ":1\r\n", wantError: true},
		{name: "unexpected null", reply: "$-1\r\n", wantError: true},
		{name: "malformed response", reply: "?invalid\r\n", wantError: true},
		{name: "connection closed", wantError: true},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			r := &syncStandaloneReader{
				client: newReplicationTestClient(t, []replicationHandshakeStep{
					{[]string{"REPLCONF", "rdb-filter-only", ""}, tt.reply},
				}, nil),
			}
			supported, err := r.sendOptionalReplconf("rdb-filter-only", "")
			require.Equal(t, tt.supported, supported)
			if tt.wantError {
				require.Error(t, err)
			} else {
				require.NoError(t, err)
			}
		})
	}
}

func TestSyncReaderAcknowledgesZeroOffset(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	done := make(chan struct{})
	r := &syncStandaloneReader{
		ctx: ctx,
		client: newReplicationTestClient(t, []replicationHandshakeStep{
			{[]string{"replconf", "ack", "0"}, ""},
		}, func() {
			cancel()
			<-done // Keep the connection open until the ACK loop has stopped.
		}),
	}
	// The server cancels the context when it receives ACK 0. Its deadline also
	// ensures a reader that skips offset zero fails the test.
	go func() {
		r.sendReplconfAck()
		close(done)
	}()
	select {
	case <-done:
	case <-time.After(5 * time.Second):
		t.Fatal("ACK loop did not stop")
	}
}
