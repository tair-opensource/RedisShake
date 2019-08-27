// Copyright 2019 Aliyun Cloud.
// Copyright 2016 CodisLabs. All Rights Reserved.
// Licensed under the MIT (MIT-LICENSE.txt) license.

package main

import (
	"encoding/json"
	"flag"
	"fmt"
	"math"
	_ "net/http/pprof"
	"os"
	"os/signal"
	"reflect"
	"runtime"
	"runtime/debug"
	"strconv"
	"strings"
	"syscall"
	"time"

	"pkg/libs/log"
	"redis-shake"
	"redis-shake/base"
	"redis-shake/common"
	"redis-shake/configure"
	"redis-shake/metric"
	"redis-shake/restful"
	"github.com/gugemichael/nimo4go"
	logRotate "gopkg.in/natefinch/lumberjack.v2"
)

type Exit struct{ Code int }

const (
	defaultHttpPort    = 9320
	defaultSystemPort  = 9310
	defaultSenderSize  = 65535
	defaultSenderCount = 1024
)

func main() {
	var err error
	defer handleExit()
	defer utils.Goodbye()

	// argument options
	configuration := flag.String("conf", "", "configuration path")
	tp := flag.String("type", "", "run type: decode, restore, dump, sync, rump")
	version := flag.Bool("version", false, "show version")
	flag.Parse()

	if *version {
		fmt.Println(utils.Version)
		return
	}

	if *configuration == "" || *tp == "" {
		if !*version {
			fmt.Println("Please show me the '-conf' and '-type'")
		}
		fmt.Println(utils.Version)
		flag.PrintDefaults()
		return
	}

	conf.Options.Version = utils.Version
	conf.Options.Type = *tp

	var file *os.File
	if file, err = os.Open(*configuration); err != nil {
		crash(fmt.Sprintf("Configure file open failed. %v", err), -1)
	}

	configure := nimo.NewConfigLoader(file)
	configure.SetDateFormat(utils.GolangSecurityTime)
	if err := configure.Load(&conf.Options); err != nil {
		crash(fmt.Sprintf("Configure file %s parse failed. %v", *configuration, err), -2)
	}

	// verify parameters
	if err = sanitizeOptions(*tp); err != nil {
		crash(fmt.Sprintf("Conf.Options check failed: %s", err.Error()), -4)
	}

	initSignal()
	initFreeOS()
	nimo.Profiling(int(conf.Options.SystemProfile))
	utils.Welcome()
	utils.StartTime = fmt.Sprintf("%v", time.Now().Format(utils.GolangSecurityTime))

	if err = utils.WritePidById(conf.Options.Id, conf.Options.PidPath); err != nil {
		crash(fmt.Sprintf("write pid failed. %v", err), -5)
	}

	// create runner
	var runner base.Runner
	switch *tp {
	case conf.TypeDecode:
		runner = new(run.CmdDecode)
	case conf.TypeRestore:
		runner = new(run.CmdRestore)
	case conf.TypeDump:
		runner = new(run.CmdDump)
	case conf.TypeSync:
		runner = new(run.CmdSync)
	case conf.TypeRump:
		runner = new(run.CmdRump)
	}

	// create metric
	metric.CreateMetric(runner)
	go startHttpServer()

	// print configuration
	if opts, err := json.Marshal(conf.Options); err != nil {
		crash(fmt.Sprintf("marshal configuration failed[%v]", err), -6)
	} else {
		log.Infof("redis-shake configuration: %s", string(opts))
	}

	// run
	runner.Main()

	log.Infof("execute runner[%v] finished!", reflect.TypeOf(runner))
}

func initSignal() {
	sigs := make(chan os.Signal, 1)
	signal.Notify(sigs, syscall.SIGINT, syscall.SIGTERM)
	go func() {
		sig := <-sigs
		log.Info("receive signal: ", sig)

		if utils.LogRotater != nil {
			utils.LogRotater.Rotate()
		}

		os.Exit(0)
	}()
}

func initFreeOS() {
	go func() {
		for {
			debug.FreeOSMemory()
			time.Sleep(5 * time.Second)
		}
	}()
}

func startHttpServer() {
	if conf.Options.HttpProfile == -1 {
		return
	}

	utils.InitHttpApi(conf.Options.HttpProfile)
	utils.HttpApi.RegisterAPI("/conf", nimo.HttpGet, func([]byte) interface{} {
		return &conf.Options
	})
	restful.RestAPI()

	if err := utils.HttpApi.Listen(); err != nil {
		crash(fmt.Sprintf("start http listen error[%v]", err), -4)
	}
}

// sanitize options
func sanitizeOptions(tp string) error {
	var err error
	if tp != conf.TypeDecode && tp != conf.TypeRestore && tp != conf.TypeDump && tp != conf.TypeSync && tp != conf.TypeRump {
		return fmt.Errorf("unknown type[%v]", tp)
	}

	if conf.Options.Id == "" {
		return fmt.Errorf("id shoudn't be empty")
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
		fallthrough
	case utils.LogLevelAll:
		logDeepLevel = log.LEVEL_DEBUG
	default:
		return fmt.Errorf("invalid log level[%v]", conf.Options.LogLevel)
	}
	log.SetLevel(logDeepLevel)

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
		}

		if strings.HasPrefix(conf.Options.TargetVersion, "4.") ||
			strings.HasPrefix(conf.Options.TargetVersion, "3.") ||
			strings.HasPrefix(conf.Options.TargetVersion, "5.") {
			conf.Options.TargetReplace = true
		} else {
			conf.Options.TargetReplace = false
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

	return nil
}

func crash(msg string, errCode int) {
	fmt.Println(msg)
	panic(Exit{errCode})
}

func handleExit() {
	if e := recover(); e != nil {
		if exit, ok := e.(Exit); ok == true {
			os.Exit(exit.Code)
		}
		panic(e)
	}
}
