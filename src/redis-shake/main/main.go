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
	TypeDecode  = "decode"
	TypeRestore = "restore"
	TypeDump    = "dump"
	TypeSync    = "sync"

	defaultHttpPort    = 20881
	defaultSystemPort  = 20882
	defaultSenderSize  = 65535
	defaultSenderCount = 1024
)

func main() {
	var err error
	defer handleExit()
	defer utils.Goodbye()

	// argument options
	configuration := flag.String("conf", "", "configuration path")
	tp := flag.String("type", "", "run type: decode, restore, dump, sync")
	version := flag.Bool("version", false, "show version")
	flag.Parse()

	if *configuration == "" || *tp == "" || *version {
		if !*version {
			fmt.Println("Please show me the '-conf' and '-type'")
		}
		fmt.Println(utils.Version)
		flag.PrintDefaults()
		return
	}

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
	case TypeDecode:
		runner = new(run.CmdDecode)
	case TypeRestore:
		runner = new(run.CmdRestore)
	case TypeDump:
		runner = new(run.CmdDump)
	case TypeSync:
		runner = new(run.CmdSync)
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
	if tp != TypeDecode && tp != TypeRestore && tp != TypeDump && tp != TypeSync {
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
		conf.Options.Parallel = 1
	} else if conf.Options.Parallel > 1024 {
		return fmt.Errorf("parallel[%v] should in (0, 1024]", conf.Options.Parallel)
	} else {
		conf.Options.Parallel = int(math.Max(float64(conf.Options.Parallel), float64(conf.Options.NCpu)))
	}

	if conf.Options.BigKeyThreshold > 524288000 {
		return fmt.Errorf("BigKeyThreshold[%v] should <= 524288000", conf.Options.BigKeyThreshold)
	}

	if (tp == TypeRestore || tp == TypeSync) && conf.Options.TargetAddress == "" {
		return fmt.Errorf("target address shouldn't be empty when type in {restore, sync}")
	}
	if (tp == TypeDump || tp == TypeSync) && conf.Options.SourceAddress == "" {
		return fmt.Errorf("source address shouldn't be empty when type in {dump, sync}")
	}

	if conf.Options.SourcePasswordRaw != "" && conf.Options.SourcePasswordEncoding != "" {
		return fmt.Errorf("only one of source password_raw or password_encoding should be given")
	} else if conf.Options.SourcePasswordEncoding != "" {
		sourcePassword := "" // todo, inner version
		conf.Options.SourcePasswordRaw = string(sourcePassword)
	}

	if conf.Options.TargetPasswordRaw != "" && conf.Options.TargetPasswordEncoding != "" {
		return fmt.Errorf("only one of target password_raw or password_encoding should be given")
	} else if conf.Options.TargetPasswordEncoding != "" {
		targetPassword := "" // todo, inner version
		conf.Options.TargetPasswordRaw = string(targetPassword)
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

	// heartbeat, 86400 = 1 day
	if conf.Options.HeartbeatInterval > 86400 {
		return fmt.Errorf("HeartbeatInterval[%v] should in [0, 86400]", conf.Options.HeartbeatInterval)
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
		if n, err := strconv.ParseInt(conf.Options.FilterDB, 10, 32); err != nil {
			return fmt.Errorf("parse FilterDB failed[%v]", err)
		} else {
			base.AcceptDB = func(db uint32) bool {
				return db == uint32(n)
			}
		}
	}

	if len(conf.Options.FilterSlot) > 0 {
		for i, val := range conf.Options.FilterSlot {
			if _, err := strconv.Atoi(val); err != nil {
				return fmt.Errorf("parse FilterSlot with index[%v] failed[%v]", i, err)
			}
		}
	}

	if conf.Options.TargetDB >= 0 {
		// pass, >= 0 means enable
	}

	if conf.Options.HttpProfile < 0 || conf.Options.HttpProfile > 65535 {
		return fmt.Errorf("HttpProfile[%v] should in [0, 65535]", conf.Options.HttpProfile)
	} else if conf.Options.HttpProfile  == 0 {
		// set to default when not set
		conf.Options.HttpProfile = defaultHttpPort
	}

	if conf.Options.SystemProfile < 0 || conf.Options.SystemProfile > 65535 {
		return fmt.Errorf("SystemProfile[%v] should in [0, 65535]", conf.Options.SystemProfile)
	} else if conf.Options.SystemProfile  == 0 {
		// set to default when not set
		conf.Options.SystemProfile = defaultSystemPort
	}

	if conf.Options.SenderSize < 0 || conf.Options.SenderSize >= 1073741824 {
		return fmt.Errorf("SenderSize[%v] should in [0, 1073741824]", conf.Options.SenderSize)
	} else if conf.Options.SenderSize  == 0 {
		// set to default when not set
		conf.Options.SenderSize = defaultSenderSize
	}

	if conf.Options.SenderCount < 0 || conf.Options.SenderCount >= 100000 {
		return fmt.Errorf("SenderCount[%v] should in [0, 100000]", conf.Options.SenderCount)
	} else if conf.Options.SenderCount  == 0 {
		// set to default when not set
		conf.Options.SenderCount = defaultSenderCount
	}

	if tp == TypeRestore || tp == TypeSync {
		// get target redis version and set TargetReplace.
		if conf.Options.TargetRedisVersion, err = utils.GetRedisVersion(conf.Options.TargetAddress,
			conf.Options.TargetAuthType, conf.Options.TargetPasswordRaw); err != nil {
			return fmt.Errorf("get target redis version failed[%v]", err)
		} else {
			if strings.HasPrefix(conf.Options.TargetRedisVersion, "4.") ||
				strings.HasPrefix(conf.Options.TargetRedisVersion, "3.") {
				conf.Options.TargetReplace = true
			} else {
				conf.Options.TargetReplace = false
			}
		}
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
