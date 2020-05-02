package conf

import "time"

type Configuration struct {
	// version
	ConfVersion uint `config:"conf.version"` // do not modify the tag name

	// config file variables
	Id                     string   `config:"id"`
	LogFile                string   `config:"log.file"`
	LogLevel               string   `config:"log.level"`
	SystemProfile          int      `config:"system_profile"`
	HttpProfile            int      `config:"http_profile"`
	Parallel               int      `config:"parallel"`
	SourceType             string   `config:"source.type"`
	SourceAddress          string   `config:"source.address"`
	SourcePasswordRaw      string   `config:"source.password_raw"`
	SourcePasswordEncoding string   `config:"source.password_encoding"`
	SourceAuthType         string   `config:"source.auth_type"`
	SourceTLSEnable        bool     `config:"source.tls_enable"`
	SourceRdbInput         []string `config:"source.rdb.input"`
	SourceRdbParallel      int      `config:"source.rdb.parallel"`
	SourceRdbSpecialCloud  string   `config:"source.rdb.special_cloud"`
	TargetAddress          string   `config:"target.address"`
	TargetPasswordRaw      string   `config:"target.password_raw"`
	TargetPasswordEncoding string   `config:"target.password_encoding"`
	TargetDBString         string   `config:"target.db"`
	TargetAuthType         string   `config:"target.auth_type"`
	TargetType             string   `config:"target.type"`
	TargetTLSEnable        bool     `config:"target.tls_enable"`
	TargetRdbOutput        string   `config:"target.rdb.output"`
	TargetVersion          string   `config:"target.version"`
	FakeTime               string   `config:"fake_time"`
	KeyExists              string   `config:"key_exists"`
	FilterDBWhitelist      []string `config:"filter.db.whitelist"`
	FilterDBBlacklist      []string `config:"filter.db.blacklist"`
	FilterKeyWhitelist     []string `config:"filter.key.whitelist"`
	FilterKeyBlacklist     []string `config:"filter.key.blacklist"`
	FilterSlot             []string `config:"filter.slot"`
	FilterLua              bool     `config:"filter.lua"`
	BigKeyThreshold        uint64   `config:"big_key_threshold"`
	Metric                 bool     `config:"metric"`
	MetricPrintLog         bool     `config:"metric.print_log"`
	SenderSize             uint64   `config:"sender.size"`
	SenderCount            uint     `config:"sender.count"`
	SenderDelayChannelSize uint     `config:"sender.delay_channel_size"`
	KeepAlive              uint     `config:"keep_alive"`
	PidPath                string   `config:"pid_path"`
	ScanKeyNumber          uint32   `config:"scan.key_number"`
	ScanSpecialCloud       string   `config:"scan.special_cloud"`
	ScanKeyFile            string   `config:"scan.key_file"`
	Qps                    int      `config:"qps"`
	ResumeFromBreakPoint   bool     `config:"resume_from_break_point"`

	/*---------------------------------------------------------*/
	// inner variables
	Psync                     bool     `config:"psync"`
	NCpu                      int      `config:"ncpu"`
	HeartbeatUrl              string   `config:"heartbeat.url"`
	HeartbeatInterval         uint     `config:"heartbeat.interval"`
	HeartbeatExternal         string   `config:"heartbeat.external"`
	HeartbeatNetworkInterface string   `config:"heartbeat.network_interface"`
	ReplaceHashTag            bool     `config:"replace_hash_tag"`
	ExtraInfo                 bool     `config:"extra"`
	SockFileName              string   `config:"sock.file_name"`
	SockFileSize              uint     `config:"sock.file_size"`
	FilterKey                 []string `config:"filter.key"` // compatible with older versions
	FilterDB                  string   `config:"filter.db"`  // compatible with older versions
	Rewrite                   bool     `config:"rewrite"`    // compatible with older versions < 1.6.27

	/*---------------------------------------------------------*/
	// generated variables
	SourceAddressList []string      // source address list
	TargetAddressList []string      // target address list
	SourceVersion     string        // source version
	HeartbeatIp       string        // heartbeat ip
	ShiftTime         time.Duration // shift
	TargetReplace     bool          // to_replace
	TargetDB          int           // int type
	Version           string        // version
	Type              string        // input mode -type=xxx
}

var Options Configuration

const (
	RedisTypeStandalone = "standalone"
	RedisTypeSentinel   = "sentinel"
	RedisTypeCluster    = "cluster"
	RedisTypeProxy      = "proxy"

	StandAloneRoleMaster = "master"
	StandAloneRoleSlave  = "slave"
	StandAloneRoleAll    = "all"

	TypeDecode  = "decode"
	TypeRestore = "restore"
	TypeDump    = "dump"
	TypeSync    = "sync"
	TypeRump    = "rump"
)

func GetSafeOptions() Configuration {
	polish := Options
	polish.SourcePasswordRaw = "***"
	polish.SourcePasswordEncoding = "***"
	polish.TargetPasswordRaw = "***"
	polish.TargetPasswordEncoding = "***"
	return polish
}