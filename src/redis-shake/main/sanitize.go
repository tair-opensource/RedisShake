package main

import (
	"fmt"
	"runtime"
	"math"
	"os"
	"time"
	"strings"
	"strconv"

	"pkg/libs/log"
	"redis-shake/common"
	"redis-shake/configure"

	logRotate "gopkg.in/natefinch/lumberjack.v2"
)

// sanitize options. TODO, need split
func SanitizeOptions(tp string) error {
	var err error
	if tp != conf.TypeDecode && tp != conf.TypeRestore && tp != conf.TypeDump && tp != conf.TypeSync && tp != conf.TypeRump {
		return fmt.Errorf("unknown type[%v]", tp)
	}

	if conf.Options.Id == "" {
		conf.Options.Id = "redis-shake-default"
	}

	if conf.Options.NCpu < 0 || conf.Options.NCpu > 1024 {
		return fmt.Errorf("invalid ncpu[%v]", conf.Options.NCpu)
	} else if conf.Options.NCpu == 0 {
		runtime.GOMAXPROCS(runtime.NumCPU())
	} else {
		runtime.GOMAXPROCS(conf.Options.NCpu)
	}

	if conf.Options.Parallel == 0 { // not set
		conf.Options.Parallel = 64 // default is 64
	} else if conf.Options.Parallel > 1024 {
		return fmt.Errorf("parallel[%v] should in (0, 1024]", conf.Options.Parallel)
	} else {
		conf.Options.Parallel = int(math.Max(float64(conf.Options.Parallel), float64(conf.Options.NCpu)))
	}

	// 500 M
	if conf.Options.BigKeyThreshold > 500 * utils.MB {
		return fmt.Errorf("BigKeyThreshold[%v] should <= 500 MB", conf.Options.BigKeyThreshold)
	} else if conf.Options.BigKeyThreshold == 0 {
		conf.Options.BigKeyThreshold = 50 * utils.MB
	}

	// source password
	if conf.Options.SourcePasswordRaw != "" && conf.Options.SourcePasswordEncoding != "" {
		return fmt.Errorf("only one of source password_raw or password_encoding should be given")
	} else if conf.Options.SourcePasswordEncoding != "" {
		sourcePassword := "" // todo, inner version
		conf.Options.SourcePasswordRaw = string(sourcePassword)
	}
	// target password
	if conf.Options.TargetPasswordRaw != "" && conf.Options.TargetPasswordEncoding != "" {
		return fmt.Errorf("only one of target password_raw or password_encoding should be given")
	} else if conf.Options.TargetPasswordEncoding != "" {
		targetPassword := "" // todo, inner version
		conf.Options.TargetPasswordRaw = string(targetPassword)
	}

	// parse source and target address and type
	if err := utils.ParseAddress(tp); err != nil {
		return fmt.Errorf("mode[%v] parse address failed[%v]", tp, err)
	}

	if tp == conf.TypeRestore || tp == conf.TypeDecode {
		if len(conf.Options.SourceRdbInput) == 0 {
			return fmt.Errorf("input rdb shouldn't be empty when type in {restore, decode}")
		}
		// check file exist
		for _, rdb := range conf.Options.SourceRdbInput {
			if _, err := os.Stat(rdb); os.IsNotExist(err) {
				return fmt.Errorf("input rdb file[%v] not exists", rdb)
			}
		}
	}
	if tp == conf.TypeDump && conf.Options.TargetRdbOutput == "" {
		conf.Options.TargetRdbOutput = "output-rdb-dump"
	}

	if tp == conf.TypeDump || tp == conf.TypeSync {
		if conf.Options.SourceRdbParallel <= 0 || conf.Options.SourceRdbParallel > len(conf.Options.SourceAddressList) {
			conf.Options.SourceRdbParallel = len(conf.Options.SourceAddressList)
		}
	} else if tp == conf.TypeRestore || tp == conf.TypeDecode {
		if conf.Options.SourceRdbParallel <= 0 || conf.Options.SourceRdbParallel > len(conf.Options.SourceRdbInput) {
			conf.Options.SourceRdbParallel = len(conf.Options.SourceRdbInput)
		}
	}

	if conf.Options.SourceRdbSpecialCloud != "" && conf.Options.SourceRdbSpecialCloud != utils.UCloudCluster {
		return fmt.Errorf("rdb special cloud type[%s] is not supported", conf.Options.SourceRdbSpecialCloud)
	}

	if conf.Options.LogFile != "" {
		//conf.Options.LogFile = fmt.Sprintf("%s.log", conf.Options.Id)

		utils.LogRotater = &logRotate.Logger{
			Filename:   conf.Options.LogFile,
			MaxSize:    100, //MB
			MaxBackups: 10,
			MaxAge:     0,
		}
		log.StdLog = log.New(utils.LogRotater, "")
	}
	// set log level
	var logDeepLevel log.LogLevel
	switch conf.Options.LogLevel {
	case utils.LogLevelNone:
		logDeepLevel = log.LEVEL_NONE
	case utils.LogLevelError:
		logDeepLevel = log.LEVEL_ERROR
	case utils.LogLevelWarn:
		logDeepLevel = log.LEVEL_WARN
	case "":
		fallthrough
	case utils.LogLevelInfo:
		logDeepLevel = log.LEVEL_INFO
	case utils.LogLevelDebug:
		logDeepLevel = log.LEVEL_DEBUG
	default:
		return fmt.Errorf("invalid log level[%v]", conf.Options.LogLevel)
	}
	log.SetLevel(logDeepLevel)

	if conf.Options.SourceAuthType == "" {
		conf.Options.SourceAuthType = "auth"
	} else {
		log.Warnf("source.auth_type[%s] != %s", conf.Options.SourceAuthType, "auth")
	}
	if conf.Options.TargetAuthType == "" {
		conf.Options.TargetAuthType = "auth"
	} else {
		log.Warnf("target.auth_type[%s] != %s", conf.Options.TargetAuthType, "auth")
	}

	// heartbeat, 86400 = 1 day
	if conf.Options.HeartbeatInterval > 86400 {
		return fmt.Errorf("HeartbeatInterval[%v] should in [0, 86400]", conf.Options.HeartbeatInterval)
	} else if conf.Options.HeartbeatInterval == 0 {
		conf.Options.HeartbeatInterval = 10
	}

	if conf.Options.HeartbeatNetworkInterface == "" {
		conf.Options.HeartbeatIp = "127.0.0.1"
	} else {
		conf.Options.HeartbeatIp, _, err = utils.GetLocalIp([]string{conf.Options.HeartbeatNetworkInterface})
		if err != nil {
			return fmt.Errorf("get ip failed[%v]", err)
		}
	}

	if conf.Options.FakeTime != "" {
		switch conf.Options.FakeTime[0] {
		case '-', '+':
			if d, err := time.ParseDuration(strings.ToLower(conf.Options.FakeTime)); err != nil {
				return fmt.Errorf("parse fake_time failed[%v]", err)
			} else {
				conf.Options.ShiftTime = d
			}
		case '@':
			if n, err := strconv.ParseInt(conf.Options.FakeTime[1:], 10, 64); err != nil {
				return fmt.Errorf("parse fake_time failed[%v]", err)
			} else {
				conf.Options.ShiftTime = time.Duration(n*int64(time.Millisecond) - time.Now().UnixNano())
			}
		default:
			if t, err := time.Parse("2006-01-02 15:04:05", conf.Options.FakeTime); err != nil {
				return fmt.Errorf("parse fake_time failed[%v]", err)
			} else {
				conf.Options.ShiftTime = time.Duration(t.UnixNano() - time.Now().UnixNano())
			}
		}
	}

	if conf.Options.Rewrite {
		conf.Options.KeyExists = "rewrite"
	}
	if conf.Options.KeyExists == "" {
		conf.Options.KeyExists = "none"
	} else if conf.Options.KeyExists == "ignore" && tp == "rump" {
		conf.Options.KeyExists = "none"
	}
	if conf.Options.KeyExists != "none" && conf.Options.KeyExists != "rewrite" && conf.Options.KeyExists != "ignore" {
		return fmt.Errorf("key_exists should in {none, rewrite, ignore}")
	}

	if conf.Options.FilterDB != "" {
		conf.Options.FilterDBWhitelist = []string{conf.Options.FilterDB}
	}
	if len(conf.Options.FilterDBWhitelist) != 0 && len(conf.Options.FilterDBBlacklist) != 0 {
		return fmt.Errorf("only one of 'filter.db.whitelist' and 'filter.db.blacklist' can be given")
	}

	if len(conf.Options.FilterKey) != 0 {
		conf.Options.FilterKeyWhitelist = conf.Options.FilterKey
	}
	if len(conf.Options.FilterKeyWhitelist) != 0 && len(conf.Options.FilterKeyBlacklist) != 0 {
		return fmt.Errorf("only one of 'filter.key.whitelist' and 'filter.key.blacklist' can be given")
	}

	if len(conf.Options.FilterSlot) > 0 {
		for i, val := range conf.Options.FilterSlot {
			if _, err := strconv.Atoi(val); err != nil {
				return fmt.Errorf("parse FilterSlot with index[%v] failed[%v]", i, err)
			}
		}
	}

	if conf.Options.TargetDBString == "" {
		conf.Options.TargetDB = -1
	} else if v, err := strconv.Atoi(conf.Options.TargetDBString); err != nil {
		return fmt.Errorf("parse target.db[%v] failed[%v]", conf.Options.TargetDBString, err)
	} else if v < 0 {
		conf.Options.TargetDB = -1
	} else {
		conf.Options.TargetDB = v
	}

	// if the target is "cluster", only allow pass db 0
	if conf.Options.TargetType == conf.RedisTypeCluster {
		if conf.Options.TargetDB == -1 {
			conf.Options.FilterDBWhitelist = []string{"0"} // set whitelist = 0
			conf.Options.FilterDBBlacklist = []string{}    // reset blacklist
			log.Info("the target redis type is cluster, only pass db0")
		} else if conf.Options.TargetDB == 0 {
			log.Info("the target redis type is cluster, all db syncing to db0")
		} else {
			// > 0
			return fmt.Errorf("target.db[%v] should in {-1, 0} when target type is cluster", conf.Options.TargetDB)
		}
	}

	if conf.Options.HttpProfile < -1 || conf.Options.HttpProfile > 65535 {
		return fmt.Errorf("HttpProfile[%v] should in [0, 65535]", conf.Options.HttpProfile)
	} else if conf.Options.HttpProfile == 0 {
		// set to default when not set
		conf.Options.HttpProfile = defaultHttpPort
	} else if conf.Options.HttpProfile == -1 {
		log.Info("http_profile is disable")
	}

	if conf.Options.SystemProfile < 0 || conf.Options.SystemProfile > 65535 {
		return fmt.Errorf("SystemProfile[%v] should in [0, 65535]", conf.Options.SystemProfile)
	} else if conf.Options.SystemProfile == 0 {
		// set to default when not set
		conf.Options.SystemProfile = defaultSystemPort
	}

	if conf.Options.SenderSize < 0 || conf.Options.SenderSize >= 1073741824 {
		return fmt.Errorf("SenderSize[%v] should in [0, 1073741824]", conf.Options.SenderSize)
	} else if conf.Options.SenderSize == 0 {
		// set to default when not set
		conf.Options.SenderSize = defaultSenderSize
	}

	if conf.Options.SenderCount < 0 || conf.Options.SenderCount >= 100000 {
		return fmt.Errorf("SenderCount[%v] should in [0, 100000]", conf.Options.SenderCount)
	} else if conf.Options.SenderCount == 0 {
		// set to default when not set
		conf.Options.SenderCount = defaultSenderCount
	}
	if conf.Options.TargetType == conf.RedisTypeCluster && int(conf.Options.SenderCount) > utils.RecvChanSize {
		log.Infof("RecvChanSize is modified from [%v] to [%v]", utils.RecvChanSize, int(conf.Options.SenderCount))
		utils.RecvChanSize = int(conf.Options.SenderCount)
	}

	if conf.Options.SenderDelayChannelSize == 0 {
		conf.Options.SenderDelayChannelSize = 32
	}

	// [0, 100 million]
	if conf.Options.Qps < 0 || conf.Options.Qps >= 100000000 {
		return fmt.Errorf("qps[%v] should in (0, 100000000]", conf.Options.Qps)
	} else if conf.Options.Qps == 0 {
		conf.Options.Qps = 500000
	}

	if tp == conf.TypeRestore || tp == conf.TypeSync || tp == conf.TypeRump {
		// version check is useless, we only want to verify the correctness of configuration.
		if conf.Options.TargetVersion == "" {
			// get target redis version and set TargetReplace.
			for _, address := range conf.Options.TargetAddressList {
				// single connection even if the target is cluster
				if v, err := utils.GetRedisVersion(address, conf.Options.TargetAuthType,
					conf.Options.TargetPasswordRaw, conf.Options.TargetTLSEnable); err != nil {
					return fmt.Errorf("get target redis version failed[%v]", err)
				} else if conf.Options.TargetVersion != "" && conf.Options.TargetVersion != v {
					return fmt.Errorf("target redis version is different: [%v %v]", conf.Options.TargetVersion, v)
				} else {
					conf.Options.TargetVersion = v
				}
			}
		} else {
			/*
			 * see github issue #173.
			 * set 1 if target is target version can't be fetched just like twemproxy.
			 */
			conf.Options.BigKeyThreshold = 1
			log.Warnf("target version[%v] given, set big_key_threshold = 1. see #173",
				conf.Options.TargetVersion, conf.Options.SourceVersion)
		}

		if strings.HasPrefix(conf.Options.TargetVersion, "4.") ||
			strings.HasPrefix(conf.Options.TargetVersion, "3.") ||
			strings.HasPrefix(conf.Options.TargetVersion, "5.") {
			conf.Options.TargetReplace = true
		} else {
			conf.Options.TargetReplace = false
		}
	}

	// check version and set big_key_threshold. see #173
	if tp == conf.TypeSync || tp == conf.TypeRump { // "tp == restore" hasn't been handled
		// fetch source version
		for _, address := range conf.Options.SourceAddressList {
			// single connection even if the target is cluster
			if v, err := utils.GetRedisVersion(address, conf.Options.SourceAuthType,
				conf.Options.SourcePasswordRaw, conf.Options.SourceTLSEnable); err != nil {
				return fmt.Errorf("get source redis version failed[%v]", err)
			} else if conf.Options.SourceVersion != "" && conf.Options.SourceVersion != v {
				return fmt.Errorf("source redis version is different: [%v %v]", conf.Options.SourceVersion, v)
			} else {
				conf.Options.SourceVersion = v
			}
		}

		// compare version. see github issue #173.
		// v1.6.24. update in #211 again. if the source version is bigger than the target version, do restore directly, if failed,
		// then try to split it
		/*
		if ret := utils.CompareVersion(conf.Options.SourceVersion, conf.Options.TargetVersion, 2); ret != 0 && ret != 1 {
			// target version is smaller than source version, or unknown
			log.Warnf("target version[%v] < source version[%v], set big_key_threshold = 1. see #173",
				conf.Options.TargetVersion, conf.Options.SourceVersion)
			conf.Options.BigKeyThreshold = 1
		}*/

		// set "psync = true" if the source version is >= 2.8
		if tp == conf.TypeSync {
			if ret := utils.CompareVersion(conf.Options.SourceVersion, "2.8", 2); ret != 1 {
				conf.Options.Psync = true
			} else {
				conf.Options.ResumeFromBreakPoint = false
			}
		}
	}

	if tp == conf.TypeRump {
		if conf.Options.ScanKeyNumber == 0 {
			conf.Options.ScanKeyNumber = 100
		}

		if conf.Options.ScanSpecialCloud != "" && conf.Options.ScanSpecialCloud != utils.TencentCluster &&
			conf.Options.ScanSpecialCloud != utils.AliyunCluster {
			return fmt.Errorf("special cloud type[%s] is not supported", conf.Options.ScanSpecialCloud)
		}

		if conf.Options.ScanSpecialCloud != "" && conf.Options.ScanKeyFile != "" {
			return fmt.Errorf("scan.special_cloud[%v] and scan.key_file[%v] can't all be given at the same time",
				conf.Options.ScanSpecialCloud, conf.Options.ScanKeyFile)
		}

		if int(conf.Options.ScanKeyNumber) > utils.RecvChanSize && conf.Options.TargetType == conf.RedisTypeCluster {
			log.Infof("RecvChanSize is modified from [%v] to [%v]", utils.RecvChanSize, int(conf.Options.ScanKeyNumber))
			utils.RecvChanSize = int(conf.Options.ScanKeyNumber)
		}

		//if len(conf.Options.SourceAddressList) == 1 {
		//	return fmt.Errorf("source address length should == 1 when type is 'rump'")
		//}
	}

	// check rdbchecksum
	if tp == conf.TypeDump || (tp == conf.TypeSync || tp == conf.TypeRump) && conf.Options.BigKeyThreshold > 1 {
		for _, address := range conf.Options.SourceAddressList {
			check, err := utils.GetRDBChecksum(address, conf.Options.SourceAuthType,
				conf.Options.SourcePasswordRaw, conf.Options.SourceTLSEnable)
			if err != nil {
				// ignore
				log.Warnf("fetch source rdb[%v] checksum failed[%v], ignore", address, err)
				continue
			}

			log.Infof("source rdb[%v] checksum[%v]", address, check)
			if check == "no" {
				return fmt.Errorf("source rdb[%v] checksum should be open[config set rdbchecksum yes]", address)
			}
		}

		//if len(conf.Options.SourceAddressList) == 1 {
		//	return fmt.Errorf("source address length should == 1 when type is 'rump'")
		//}
	}

	// enable resume from break point
	if conf.Options.ResumeFromBreakPoint {
		if tp != conf.TypeSync {
			// set false if tp is not 'sync'
			conf.Options.ResumeFromBreakPoint = false
		}

		if conf.Options.Psync == false {
			return fmt.Errorf("'psync' should == true if enable resume_from_break_point")
		}

		if conf.Options.TargetDB != -1 {
			return fmt.Errorf("target.db should only == -1 if enable resume_from_break_point")
		}

		// check db type
		if conf.Options.SourceType != conf.Options.TargetType {
			return fmt.Errorf("source type must equal to the target type when 'resume_from_break_point == true'" +
				": source.type[%v] != target.type[%v]", conf.Options.SourceType, conf.Options.TargetType)
		}

		// check cluster nodes number
		if conf.Options.SourceType == conf.RedisTypeCluster {
			if len(conf.Options.SourceAddressList) != len(conf.Options.TargetAddressList) {
				return fmt.Errorf("source db node number must equal to the target db node when " +
					"'resume_from_break_point == true': source[%v] != target[%v]",
					len(conf.Options.SourceAddressList), len(conf.Options.TargetAddressList))
			}

			// check slot distribution
			// 1. get source slot distribution
			srcSlot, err := utils.GetSlotDistribution(conf.Options.SourceAddressList[0], conf.Options.SourceAuthType,
				conf.Options.SourcePasswordRaw, false)
			if err != nil {
				return fmt.Errorf("resume_from_break_point get source slot distribution failed: %v", err)
			}
			// 2. get source slot distribution
			dstSlot, err := utils.GetSlotDistribution(conf.Options.TargetAddressList[0], conf.Options.TargetAuthType,
				conf.Options.TargetPasswordRaw, false)
			if err != nil {
				return fmt.Errorf("resume_from_break_point get target slot distribution failed: %v", err)
			}
			// 3. do comparison
			if utils.CheckSlotDistributionEqual(srcSlot, dstSlot) == false {
				return fmt.Errorf("resume_from_break_point source slot distribution should be equal to the" +
					" target: src[%v] dst[%v]", srcSlot, dstSlot)
			}
		}
	}

	return nil
}