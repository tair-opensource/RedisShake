package aof_test

import (
	"fmt"
	"net/http"
	"os"
	"runtime"
	"testing"

	"github.com/alibaba/RedisShake/internal/commands"
	"github.com/alibaba/RedisShake/internal/config"
	"github.com/alibaba/RedisShake/internal/filter"
	"github.com/alibaba/RedisShake/internal/log"
	"github.com/alibaba/RedisShake/internal/reader"
	"github.com/alibaba/RedisShake/internal/statistics"
	"github.com/alibaba/RedisShake/internal/writer"
	"github.com/go-redis/redis"
)

const (
	AOFRestorePath    = "/test_aof_restore.toml"
	AppendOnlyAoFPath = "/appendonlydir/appendonly.aof.manifest"
)

func AOFMain(configFile string, aofFilePath string) {

	// load aof  config file
	config.LoadFromFile(configFile)

	log.Init()
	log.Infof("GOOS: %s, GOARCH: %s", runtime.GOOS, runtime.GOARCH)
	log.Infof("Ncpu: %d, GOMAXPROCS: %d", config.Config.Advanced.Ncpu, runtime.GOMAXPROCS(0))
	log.Infof("pid: %d", os.Getpid())
	log.Infof("pprof_port: %d", config.Config.Advanced.PprofPort)

	// start pprof
	if config.Config.Advanced.PprofPort != 0 {
		go func() {
			err := http.ListenAndServe(fmt.Sprintf("localhost:%d", config.Config.Advanced.PprofPort), nil)
			if err != nil {
				log.PanicError(err)
			}
		}()
	}

	// start statistics
	if config.Config.Advanced.MetricsPort != 0 {
		statistics.Metrics.Address = config.Config.Source.Address
		go func() {
			log.Infof("metrics url: http://localhost:%d", config.Config.Advanced.MetricsPort)
			mux := http.NewServeMux()
			mux.HandleFunc("/", statistics.Handler)
			err := http.ListenAndServe(fmt.Sprintf("localhost:%d", config.Config.Advanced.MetricsPort), mux)
			if err != nil {
				log.PanicError(err)
			}
		}()
	}

	// create writer
	var theWriter writer.Writer
	target := &config.Config.Target
	switch config.Config.Target.Type {
	case "standalone":
		theWriter = writer.NewRedisWriter(target.Address, target.Username, target.Password, target.IsTLS)
	case "cluster":
		theWriter = writer.NewRedisClusterWriter(target.Address, target.Username, target.Password, target.IsTLS)
	default:
		log.Panicf("unknown target type: %s", target.Type)
	}

	var theReader reader.Reader

	theReader = reader.NewAOFReader(aofFilePath)

	fmt.Printf("the aof path:%v\n", aofFilePath)
	ch := theReader.StartRead()

	// start sync
	statistics.Init()
	id := uint64(0)
	for e := range ch {
		statistics.UpdateInQueueEntriesCount(uint64(len(ch)))
		// calc arguments
		e.Id = id
		id++
		e.CmdName, e.Group, e.Keys = commands.CalcKeys(e.Argv)
		e.Slots = commands.CalcSlots(e.Keys)

		// filter
		code := filter.Filter(e)
		statistics.UpdateEntryId(e.Id)
		if code == filter.Allow {
			theWriter.Write(e)
			statistics.AddAllowEntriesCount()
		} else if code == filter.Disallow {
			// do something
			statistics.AddDisallowEntriesCount()
		} else {
			log.Panicf("error when run lua filter. entry: %s", e.ToString())
		}
	}
	theWriter.Close()
	log.Infof("finished.")
}

// if you use this test you need start redis in port 6379s
func TestMainFunction(t *testing.T) {

	//	os.Args = []string{"redis-shake", "/home/hwy/kaiyuan/restore.toml"}
	wdPath, err := os.Getwd()
	if err != nil {
		panic(err)
	}

	configPath := wdPath + AOFRestorePath
	aofFilePath := wdPath + AppendOnlyAoFPath
	fmt.Printf("configPath:%v, aofFilepath:%v\n", configPath, aofFilePath)

	AOFMain(configPath, aofFilePath) //restore aof

	client := redis.NewClient(&redis.Options{
		Addr:     "localhost:6379",
		Password: "",
		DB:       0,
	})

	pong, err := client.Ping().Result()
	if err != nil {
		t.Fatalf("Failed to connect to Redis: %v", err)
	}
	fmt.Println("Connected to Redis:", pong)
	/*
		for i := 11; i <= 10000; i++ {
			value := strconv.Itoa(i)
			score := float64(i)
			z := redis.Z{Score: score, Member: value}
			err := client.ZAdd("myzset", z).Err()
			fmt.Println(value)
			if err != nil {
				fmt.Println("Failed to write data to Redis:", err)
				return
			}
		}
	*/
	// 读取整个有序集合
	zsetValues, err := client.ZRangeWithScores("myzset", 0, -1).Result()
	if err != nil {
		fmt.Println("Failed to read data from Redis:", err)
		return
	}

	// 遍历有序集合中的元素和分数
	for _, z := range zsetValues {
		member := z.Member.(string)
		score := z.Score
		fmt.Printf("Member: %s, Score: %f\n", member, score)
	}
	/*
		expected := map[string]string{
			"kl":    "kl",
			"key0":  "2022-03-29 17:25:54.592593",
			"key1":  "2022-03-29 17:25:54.876326",
			"key2":  "2022-03-29 17:25:52.871918",
			"key3":  "2022-03-29 17:25:53.034060",
			"key4":  "2022-03-29 17:25:53.196913",
			"key5":  "2022-03-29 17:25:53.356234",
			"key6":  "2022-03-29 17:25:53.513544",
			"key7":  "2022-03-29 17:25:53.671556",
			"key8":  "2022-03-29 17:25:53.861237",
			"key9":  "2022-03-29 17:25:54.020518",
			"key10": "2022-03-29 17:25:54.177881",
			"key11": "2022-03-29 17:25:54.337640",
		}
	*/
	/*for key, value := range expected {
		result, err := client.Get(key).Result()
		if err != nil {
			t.Fatalf("Failed to read key %s from Redis: %v", key, err)
		}

		if result != value {
			t.Errorf("Value for key %s is incorrect. Expected: %s, Got: %s", key, value, result)
		}
	}

	for key := 11; key <= 10000; key++ {
		result, err := client.Get(strconv.Itoa(key)).Result()
		if err != nil {
			t.Fatalf("Failed to read key %v from Redis: %v", key, err)
		}

		if result != strconv.Itoa(key) {
			t.Errorf("Value for key %v is incorrect. Expected: %v, Got: %v", key, key, result)
		}
	}*/

	/*result, err := client.SMembers("superpowers").Result()
	if err != nil {
		t.Fatalf("Failed to read set from Redis: %v", err)
	}
	strings := result[0]
	if strings != "reflexes" {
		t.Errorf("read set wrong")
	}*/

}
