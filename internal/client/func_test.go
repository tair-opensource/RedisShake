package client

import (
	"testing"
)

func TestParseServerVersion(t *testing.T) {
	tests := []struct {
		name       string
		serverInfo string
		want       bool
		wantErr    bool
	}{
		{
			name: "Valkey 9.x",
			serverInfo: `# Server
valkey_version:9.0.0
valkey_mode:standalone
os:Linux 5.4.0 x86_64
arch_bits:64
`,
			want:    true,
			wantErr: false,
		},
		{
			name: "Valkey 8.x",
			serverInfo: `# Server
valkey_version:8.5.1
valkey_mode:standalone
os:Linux 6.2.0 x86_64
arch_bits:64
`,
			want:    true,
			wantErr: false,
		},
		{
			name: "Redis 7.x",
			serverInfo: `# Server
redis_version:7.2.3
redis_mode:standalone
os:Linux 5.4.0 x86_64
arch_bits:64
`,
			want:    false,
			wantErr: false,
		},
		{
			name: "Redis 6.x",
			serverInfo: `# Server
redis_version:6.2.14
redis_mode:standalone
os:Linux 5.4.0 x86_64
arch_bits:64
`,
			want:    false,
			wantErr: false,
		},
		{
			name: "Redis 8.0 (future)",
			serverInfo: `# Server
redis_version:8.0.0
redis_mode:standalone
os:Linux 5.4.0 x86_64
arch_bits:64
`,
			want:    false,
			wantErr: false,
		},
		{
			name: "Redis with extra spaces in version",
			serverInfo: `# Server
redis_version: 7.2.3
redis_mode:standalone
`,
			want:    false,
			wantErr: false,
		},
		{
			name:       "Empty info string",
			serverInfo: "",
			want:       false,
			wantErr:    true,
		},
		{
			name: "No version info",
			serverInfo: `# Server
os:Linux 5.4.0 x86_64
arch_bits:64
`,
			want:    false,
			wantErr: true,
		},
		{
			name: "Valkey with multiple lines and extra info",
			serverInfo: `# Server
valkey_version:8.5.1
# Memory
used_memory:1234567
# Stats
total_connections_received:100
`,
			want:    true,
			wantErr: false,
		},
		{
			name: "Redis 5.0 (old version)",
			serverInfo: `# Server
redis_version:5.0.14
redis_mode:standalone
`,
			want:    false,
			wantErr: false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got, err := ParseServerVersion(tt.serverInfo)
			if (err != nil) != tt.wantErr {
				t.Errorf("ParseServerVersion() error = %v, wantErr %v", err, tt.wantErr)
				return
			}
			if got != tt.want {
				t.Errorf("ParseServerVersion() = %v, want %v", got, tt.want)
			}
		})
	}
}
