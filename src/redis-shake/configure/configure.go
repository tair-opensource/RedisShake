package conf

import "time"

type Configuration struct {
	// config file variables
	Id                        string   `config:"id"`
	LogFile                   string   `config:"log_file"`
	SystemProfile             int      `config:"system_profile"`
	HttpProfile               int      `config:"http_profile"`
	NCpu                      int      `config:"ncpu"`
	Parallel                  int      `config:"parallel"`
	InputRdb                  string   `config:"input_rdb"`
	OutputRdb                 string   `config:"output_rdb"`
	SourceAddress             string   `config:"source.address"`
	SourcePasswordRaw         string   `config:"source.password_raw"`
	SourcePasswordEncoding    string   `config:"source.password_encoding"`
	SourceVersion             uint     `config:"source.version"`
	SourceAuthType            string   `config:"source.auth_type"`
	TargetAddress             string   `config:"target.address"`
	TargetPasswordRaw         string   `config:"target.password_raw"`
	TargetPasswordEncoding    string   `config:"target.password_encoding"`
	TargetVersion             uint     `config:"target.version"`
	TargetDB                  int      `config:"target.db"`
	TargetAuthType            string   `config:"target.auth_type"`
	FakeTime                  string   `config:"fake_time"`
	Rewrite                   bool     `config:"rewrite"`
	FilterDB                  string   `config:"filter.db"`
	FilterKey                 []string `config:"filter.key"`
	FilterSlot                []string `config:"filter.slot"`
	BigKeyThreshold           uint64   `config:"big_key_threshold"`
	Psync                     bool     `config:"psync"`
	Metric                    bool     `config:"metric"`
	MetricPrintLog            bool     `config:"metric.print_log"`
	HeartbeatUrl              string   `config:"heartbeat.url"`
	HeartbeatInterval         uint     `config:"heartbeat.interval"`
	HeartbeatExternal         string   `config:"heartbeat.external"`
	HeartbeatNetworkInterface string   `config:"heartbeat.network_interface"`
	SenderSize                uint64   `config:"sender.size"`
	SenderCount               uint     `config:"sender.count"`
	SenderDelayChannelSize    uint     `config:"sender.delay_channel_size"`
	KeepAlive                 uint     `config:"keep_alive"`
	PidPath                   string   `config:"pid_path"`

	// inner variables
	ReplaceHashTag bool   `config:"replace_hash_tag"`
	ExtraInfo      bool   `config:"extra"`
	SockFileName   string `config:"sock.file_name"`
	SockFileSize   uint   `config:"sock.file_size"`

	// generated variables
	HeartbeatIp        string
	ShiftTime          time.Duration // shift
	TargetRedisVersion string        // to_redis_version
	TargetReplace      bool          // to_replace
}

var Options Configuration
