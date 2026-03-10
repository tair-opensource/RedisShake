package reader

import (
	"context"
	"encoding/json"
	"testing"

	"RedisShake/internal/log"

	"github.com/stretchr/testify/require"
)

func Test_syncStandaloneReader_Status(t *testing.T) {
	type fields struct {
		ctx  context.Context
		opts *SyncReaderOptions
	}
	tests := []struct {
		name   string
		fields fields
		want   interface{}
	}{
		{
			name: "syncStandaloneReader_Status_Marshal",
			fields: fields{
				ctx: context.Background(),
				opts: &SyncReaderOptions{
					Cluster:       false,
					Address:       "127.0.0.1:6379",
					Username:      "username",
					Password:      "password",
					Tls:           false,
					SyncRdb:       false,
					SyncAof:       false,
					PreferReplica: false,
					TryDiskless:   false,
				},
			},
			want: map[string]interface{}{
				"name":                "",
				"address":             "",
				"dir":                 "",
				"status":              "",
				"rdb_file_size_bytes": 0,
				"rdb_file_size_human": "",
				"rdb_received_bytes":  0,
				"rdb_received_human":  "",
				"rdb_sent_bytes":      0,
				"rdb_sent_human":      "",
				"aof_received_offset": 0,
				"aof_sent_offset":     0,
				"aof_received_bytes":  0,
				"aof_received_human":  "",
			},
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			r := &SyncStandaloneReader{
				ctx:  tt.fields.ctx,
				opts: tt.fields.opts,
			}

			want, err := json.Marshal(tt.want)
			if err != nil {
				log.Warnf("marshal want failed, err=[%v]", err)
				return
			}

			got, err := json.Marshal(r.Status())
			if err != nil {
				log.Warnf("marshal status failed, err=[%v]", err)
				return
			}

			require.JSONEq(t, string(want), string(got))
		})
	}
}
