package reader

import (
	"testing"

	"github.com/stretchr/testify/require"
)

func TestParseKeyEvent(t *testing.T) {
	tests := []struct {
		name    string
		resp    interface{}
		want    keyEvent
		wantOk  bool
		wantErr bool
	}{
		{
			name:   "set event",
			resp:   []interface{}{"pmessage", "__keyevent@*__:*", "__keyevent@0__:set", "user:1"},
			want:   keyEvent{dbId: 0, name: "set", key: "user:1"},
			wantOk: true,
		},
		{
			name:   "del event in non-zero db",
			resp:   []interface{}{"pmessage", "__keyevent@12__:*", "__keyevent@12__:del", "k"},
			want:   keyEvent{dbId: 12, name: "del", key: "k"},
			wantOk: true,
		},
		{
			name:   "key with colons and digits",
			resp:   []interface{}{"pmessage", "__keyevent@*__:*", "__keyevent@3__:expired", "a:7:b"},
			want:   keyEvent{dbId: 3, name: "expired", key: "a:7:b"},
			wantOk: true,
		},
		{
			name:   "binary key",
			resp:   []interface{}{"pmessage", "__keyevent@*__:*", "__keyevent@0__:setex", "\x00\xff\r\n"},
			want:   keyEvent{dbId: 0, name: "setex", key: "\x00\xff\r\n"},
			wantOk: true,
		},
		{
			name: "reply to PING",
			resp: []interface{}{"pong", ""},
		},
		{
			name:    "plain PONG status is not a subscription reply",
			resp:    "PONG",
			wantErr: true,
		},
		{
			name:    "message from a plain channel",
			resp:    []interface{}{"message", "chan", "payload"},
			wantErr: true,
		},
		{
			name:    "pmessage from a foreign channel",
			resp:    []interface{}{"pmessage", "*", "news", "payload"},
			wantErr: true,
		},
		{
			name:    "short pmessage",
			resp:    []interface{}{"pmessage", "__keyevent@*__:*", "__keyevent@0__:set"},
			wantErr: true,
		},
		{
			name:    "empty array",
			resp:    []interface{}{},
			wantErr: true,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got, ok, err := parseKeyEvent(tt.resp)
			if tt.wantErr {
				require.Error(t, err)
				return
			}
			require.NoError(t, err)
			require.Equal(t, tt.wantOk, ok)
			require.Equal(t, tt.want, got)
		})
	}
}
