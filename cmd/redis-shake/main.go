package main

import (
	"RedisShake/internal/client"
	"context"
	_ "net/http/pprof"
	"os"
	"os/signal"
	"strings"
	"sync/atomic"
	"syscall"
	"time"
	"strconv"
	"sync"

	"RedisShake/internal/config"
	"RedisShake/internal/entry"
	"RedisShake/internal/filter"
	"RedisShake/internal/log"
	"RedisShake/internal/reader"
	"RedisShake/internal/status"
	"RedisShake/internal/utils"
	"RedisShake/internal/writer"

	"fmt"
	"runtime"

	"github.com/mcuadros/go-defaults"
)

var (
	// These variables will be set during build time
	Version   = "unknown"
	GitCommit = "unknown"
)

func getVersionString() string {
	return fmt.Sprintf("%s %s/%s (Git SHA: %s)", Version, runtime.GOOS, runtime.GOARCH, GitCommit)
}

// 新增 parseLazyfreePending 从 INFO 输出中解析 lazyfree_pending_objects 的值
func parseLazyfreePending(info string) int {
    lines := strings.Split(info, "\n")
    for _, line := range lines {
        if strings.HasPrefix(line, "lazyfree_pending_objects:") {
            parts := strings.SplitN(line, ":", 2)
            if len(parts) == 2 {
                val, err := strconv.Atoi(strings.TrimSpace(parts[1]))
                if err == nil {
                    return val
                }
            }
        }
    }
    return -1
}
// 新增 waitAsyncFlushForNode 等待单个 Redis 节点异步清空完成
// 每 2s 轮询一次 lazyfree_pending_objects，默认 10 分钟内未变为 0 则 panic
// address: 节点地址，用于日志输出
// timeout: FLUSHALL 异步执行超时时间
func waitAsyncFlushForNode(client *client.Redis, address string, timeout time.Duration) {
    ticker := time.NewTicker(2 * time.Second)
    defer ticker.Stop()

    deadline := time.After(timeout)
    for {
        select {
        case <-deadline:
            log.Panicf("Timeout: async flush did not complete within %v for node %s", timeout, address)

        case <-ticker.C:
            client.Send("INFO", "memory")
            client.Flush()
            reply, err := client.Receive()
            if err != nil {
                log.Warnf("[%s] Failed to get INFO memory: %v", address, err)
                continue
            }
            info, ok := reply.(string)
            if !ok {
                log.Warnf("[%s] Unexpected reply type for INFO memory: %T", address, reply)
                continue
            }
            pending := parseLazyfreePending(info)
            if pending == -1 {
                log.Warnf("[%s] Could not find lazyfree_pending_objects in INFO memory output", address)
                // 可能版本不支持，视为完成
                return
            }
            // 输出当前 pending 值（INFO 级别，保持与 RedisShake 日志风格一致）
            log.Infof("[%s] lazyfree_pending_objects = %d", address, pending)

            if pending == 0 {
                log.Infof("[%s] Node completed async flush (lazyfree_pending_objects=0)", address)
                return
            }
        }
    }
}
// 新增 waitAsyncFlushForCluster 等待集群所有节点异步清空完成
// addresses: 集群所有主节点地址
// opts: 目标 Redis 连接配置（用于创建临时客户端）
// timeout: FLUSHALL 异步执行超时时间
func waitAsyncFlushForCluster(addresses []string, opts *writer.RedisWriterOptions, timeout time.Duration) {
    var wg sync.WaitGroup
    for _, addr := range addresses {
        wg.Add(1)
        go func(address string) {
            defer wg.Done()
            // 为每个节点创建临时客户端
            ctx := context.Background()
            tempClient := client.NewRedisClient(ctx, address, opts.Username, opts.Password, opts.Tls, opts.TlsConfig, false)
            defer tempClient.Close()
            waitAsyncFlushForNode(tempClient, address, timeout) // 传入地址
        }(addr)
    }
    wg.Wait()
    log.Infof("All cluster nodes have completed async flush")
}

func main() {
	// Add version flag check before config loading
	if len(os.Args) == 2 && (os.Args[1] == "-v" || os.Args[1] == "--version" || os.Args[1] == "version") {
		fmt.Printf("redis-shake version %s\n", getVersionString())
		os.Exit(0)
	}

	// Add version info at startup
	log.Infof("redis-shake version %s", getVersionString())

	v := config.LoadConfig()

	log.Init(config.Opt.Advanced.LogLevel,
		config.Opt.Advanced.LogFile,
		config.Opt.Advanced.Dir,
		config.Opt.Advanced.LogRotation,
		config.Opt.Advanced.LogMaxSize,
		config.Opt.Advanced.LogMaxAge,
		config.Opt.Advanced.LogMaxBackups,
		config.Opt.Advanced.LogCompress)
	utils.ChdirAndAcquireFileLock()
	utils.SetNcpu()
	utils.SetPprofPort()
	luaRuntime := filter.NewFunctionFilter(config.Opt.Filter.Function)

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	// create reader
	var theReader reader.Reader
	switch {
	case v.IsSet("sync_reader"):
		opts := new(reader.SyncReaderOptions)
		defaults.SetDefaults(opts)
		err := v.UnmarshalKey("sync_reader", opts)
		if err != nil {
			log.Panicf("failed to read the SyncReader config entry. err: %v", err)
		}
		if opts.Cluster {
			log.Infof("create SyncClusterReader")
			log.Infof("* address (should be the address of one node in the Redis cluster): %s", opts.Address)
			log.Infof("* username: %s", opts.Username)
			log.Infof("* password: %s", strings.Repeat("*", len(opts.Password)))
			log.Infof("* tls: %v", opts.Tls)
			theReader = reader.NewSyncClusterReader(ctx, opts)
		} else {
			if opts.Sentinel.Address != "" {
				address := client.FetchAddressFromSentinel(&opts.Sentinel)
				opts.Address = address
			}
			log.Infof("create SyncStandaloneReader")
			log.Infof("* address: %s", opts.Address)
			log.Infof("* username: %s", opts.Username)
			log.Infof("* password: %s", strings.Repeat("*", len(opts.Password)))
			log.Infof("* tls: %v", opts.Tls)
			theReader = reader.NewSyncStandaloneReader(ctx, opts)
		}
	case v.IsSet("scan_reader"):
		opts := new(reader.ScanReaderOptions)
		defaults.SetDefaults(opts)
		err := v.UnmarshalKey("scan_reader", opts)
		if err != nil {
			log.Panicf("failed to read the ScanReader config entry. err: %v", err)
		}
		if opts.Cluster {
			log.Infof("create ScanClusterReader")
			log.Infof("* address (should be the address of one node in the Redis cluster): %s", opts.Address)
			log.Infof("* username: %s", opts.Username)
			log.Infof("* password: %s", strings.Repeat("*", len(opts.Password)))
			log.Infof("* tls: %v", opts.Tls)
			theReader = reader.NewScanClusterReader(ctx, opts)
		} else {
			log.Infof("create ScanStandaloneReader")
			log.Infof("* address: %s", opts.Address)
			log.Infof("* username: %s", opts.Username)
			log.Infof("* password: %s", strings.Repeat("*", len(opts.Password)))
			log.Infof("* tls: %v", opts.Tls)
			theReader = reader.NewScanStandaloneReader(ctx, opts)
		}
	case v.IsSet("rdb_reader"):
		opts := new(reader.RdbReaderOptions)
		defaults.SetDefaults(opts)
		err := v.UnmarshalKey("rdb_reader", opts)
		if err != nil {
			log.Panicf("failed to read the RdbReader config entry. err: %v", err)
		}
		theReader = reader.NewRDBReader(opts)
		log.Infof("create RdbReader: %v", opts.Filepath)
	case v.IsSet("aof_reader"):
		opts := new(reader.AOFReaderOptions)
		defaults.SetDefaults(opts)
		err := v.UnmarshalKey("aof_reader", opts)
		if err != nil {
			log.Panicf("failed to read the AOFReader config entry. err: %v", err)
		}
		theReader = reader.NewAOFReader(opts)
		log.Infof("create AOFReader: %v", opts.Filepath)
	default:
		log.Panicf("no reader config entry found")
	}
	// create writer
	var theWriter writer.Writer
	// 新增，保存目标 Redis 配置，用于等待函数
	var redisWriterOpts *writer.RedisWriterOptions
	switch {
	case v.IsSet("file_writer"):
		opts := new(writer.FileWriterOptions)
		defaults.SetDefaults(opts)
		err := v.UnmarshalKey("file_writer", opts)
		if err != nil {
			log.Panicf("failed to read the FileWriter config entry. err: %v", err)
		}
		theWriter = writer.NewFileWriter(ctx, opts)
	case v.IsSet("redis_writer"):
		opts := new(writer.RedisWriterOptions)
		defaults.SetDefaults(opts)
		err := v.UnmarshalKey("redis_writer", opts)
		if err != nil {
			log.Panicf("failed to read the RedisStandaloneWriter config entry. err: %v", err)
		}
		if opts.OffReply && config.Opt.Advanced.RDBRestoreCommandBehavior == "panic" {
			log.Panicf("the RDBRestoreCommandBehavior can't be 'panic' when the server not reply to commands")
		}
		redisWriterOpts = opts // 新增，保存配置
		if opts.Cluster {
			log.Infof("create RedisClusterWriter")
			log.Infof("* address (should be the address of one node in the Redis cluster): %s", opts.Address)
			log.Infof("* username: %s", opts.Username)
			log.Infof("* password: %s", strings.Repeat("*", len(opts.Password)))
			log.Infof("* tls: %v", opts.Tls)
			theWriter = writer.NewRedisClusterWriter(ctx, opts)
		} else {
			if opts.Sentinel.Address != "" {
				address := client.FetchAddressFromSentinel(&opts.Sentinel)
				opts.Address = address
			}
			log.Infof("create RedisStandaloneWriter")
			log.Infof("* address: %s", opts.Address)
			log.Infof("* username: %s", opts.Username)
			log.Infof("* password: %s", strings.Repeat("*", len(opts.Password)))
			log.Infof("* tls: %v", opts.Tls)
			theWriter = writer.NewRedisStandaloneWriter(ctx, opts)
		}
		// if config.Opt.Advanced.EmptyDBBeforeSync {
			// exec FLUSHALL command to flush db
			// entry := entry.NewEntry()
			// entry.Argv = []string{"FLUSHALL"}
			// theWriter.Write(entry)
		// }
	default:
		log.Panicf("no writer config entry found")
	}

	// 新增，启动 writer 的后台写协程（确保命令能够发送）
	theWriter.StartWrite(ctx)
	    // 新增，清空目标库（如果需要）
	if config.Opt.Advanced.EmptyDBBeforeSync {
		argv := strings.Fields(config.Opt.Advanced.FlushAllCommand)
		if len(argv) == 0 {
			log.Panicf("flushall_command is empty")
		}
		if config.Opt.Advanced.FlushAllMode == "async" {
			hasAsync := false
			for _, arg := range argv {
				if strings.EqualFold(arg, "ASYNC") {
					hasAsync = true
					break
				}
			}
			if !hasAsync {
				argv = append(argv, "ASYNC")
			}
		}
		log.Infof("Sending flush command: %s", strings.Join(argv, " "))

		entry := entry.NewEntry()
		entry.Argv = argv
		theWriter.Write(entry)

		if config.Opt.Advanced.FlushAllMode == "async" {
			log.Infof("Async flush mode enabled, waiting for all cluster nodes to complete...")
			if clusterWriter, ok := theWriter.(*writer.RedisClusterWriter); ok {
				addresses := clusterWriter.GetAddresses()
				// 将分钟转换为 time.Duration
				timeout := time.Duration(config.Opt.Advanced.FlushAllAsyncTimeout) * time.Minute
				waitAsyncFlushForCluster(addresses, redisWriterOpts, timeout)
			} else {
				log.Panicf("Async flush waiting is only supported for cluster writer, but got %T", theWriter)
			}
		}
	}

	// create status
	if config.Opt.Advanced.StatusPort != 0 {
		status.Init(theReader, theWriter)
	}
	// create log entry count
	logEntryCount := status.EntryCount{
		ReadCount:  0,
		WriteCount: 0,
	}

	log.Infof("start syncing...")

	go waitShutdown(cancel)

	chrs := theReader.StartRead(ctx)

	// 新增，注释掉下面原流程
	// theWriter.StartWrite(ctx)

	readerDone := make(chan bool)

	for _, chr := range chrs {
		go func(ch chan *entry.Entry) {
			for e := range ch {
				// calc arguments
				e.Parse()

				// update reader status
				if config.Opt.Advanced.StatusPort != 0 {
					status.AddReadCount(e.CmdName)
				}
				// update log entry count
				atomic.AddUint64(&logEntryCount.ReadCount, 1)

				// filter
				if !filter.Filter(e) {
					log.Debugf("skip command: %v", e)
					continue
				}

				// run lua function
				log.Debugf("function before: %v", e)
				entries := luaRuntime.RunFunction(e)
				log.Debugf("function after: %v", entries)

				// write
				for _, theEntry := range entries {
					theEntry.Parse()
					theWriter.Write(theEntry)

					// update writer status
					if config.Opt.Advanced.StatusPort != 0 {
						status.AddWriteCount(theEntry.CmdName)
					}
					// update log entry count
					atomic.AddUint64(&logEntryCount.WriteCount, 1)
				}
			}
			readerDone <- true
		}(chr)
	}

	// caluate ops and log to screen
	go func() {
		if config.Opt.Advanced.LogInterval <= 0 {
			log.Infof("log interval is 0, will not log to screen")
			return
		}
		ticker := time.NewTicker(time.Duration(config.Opt.Advanced.LogInterval) * time.Second)
		defer ticker.Stop()
		for range ticker.C {
			logEntryCount.UpdateOPS()
			log.Infof("%s, %s", logEntryCount.String(), theReader.StatusString())
		}
	}()

	ticker := time.NewTicker(1 * time.Second)
	defer ticker.Stop()
	readerCnt := len(chrs)
Loop:
	for {
		select {
		case done := <-readerDone:
			if done {
				readerCnt--
			}
			if readerCnt == 0 {
				break Loop
			}
		case <-ticker.C:
			pingEntry := entry.NewEntry()
			pingEntry.DbId = 0
			pingEntry.CmdName = "PING"
			pingEntry.Argv = []string{"PING"}
			pingEntry.Group = "connection"
			theWriter.Write(pingEntry)
		}
	}

	theWriter.Close()       // Wait for all writing operations to complete
	utils.ReleaseFileLock() // Release file lock
	log.Infof("all done")
}

func waitShutdown(cancel context.CancelFunc) {
	quitCh := make(chan os.Signal, 1)
	signal.Notify(quitCh, syscall.SIGINT, syscall.SIGTERM, syscall.SIGQUIT)
	sigTimes := 0
	for {
		sig := <-quitCh
		if shouldForceExit := handleShutdownSignal(sig, &sigTimes, cancel); shouldForceExit {
			os.Exit(0)
		}
	}
}

func handleShutdownSignal(sig os.Signal, sigTimes *int, cancel context.CancelFunc) bool {
	if sig == syscall.SIGINT {
		*sigTimes = *sigTimes + 1
		log.Infof("Got signal: %s to exit. Press Ctrl+C again to force exit.", sig)
		if *sigTimes >= 2 {
			return true
		}
		cancel()
		return false
	}

	log.Infof("Got signal: %s to exit.", sig)
	cancel()
	return false
}
