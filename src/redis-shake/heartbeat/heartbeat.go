package heartbeat

import (
	"bytes"
	"encoding/json"
	"io/ioutil"
	"net/http"
	"time"

	"github.com/alibaba/RedisShake/pkg/libs/log"
	"github.com/alibaba/RedisShake/redis-shake/common"
	"github.com/alibaba/RedisShake/redis-shake/configure"
)

type HeartbeatController struct {
	ServerUrl string
	Interval  int32
}

type HeartbeatData struct {
	Id       string `json:"id"`
	Ip       string `json:"ip"`
	Port     int32  `json:"port"`
	Ts       int64  `json:"ts"`
	Version  string `json:"version"`
	External string `json:"external"`
}

type HeartbeatResponse struct {
	Error   int32  `json:"error"`
	Message string `json:"msg"`
	Data    string `json:"data"`
}

func (c *HeartbeatController) Start() {
	data := &HeartbeatData{
		Id:       conf.Options.Id,
		Ip:       conf.Options.HeartbeatIp,
		Port:     int32(conf.Options.HttpProfile),
		Version:  utils.Version,
		External: conf.Options.HeartbeatExternal,
	}

	ticker := time.NewTicker(time.Second * time.Duration(conf.Options.HeartbeatInterval))
	defer ticker.Stop()

	for range ticker.C {
		c.run(data)
	}
}

func (c *HeartbeatController) run(data *HeartbeatData) {
	data.Ts = time.Now().UnixNano() / int64(time.Millisecond)
	dataStr, _ := json.MarshalIndent(data, "", "  ")

	client := http.Client{
		Timeout: 10 * time.Second,
	}
	resp, err := client.Post(conf.Options.HeartbeatUrl, "application/json", bytes.NewBuffer(dataStr))
	if err != nil {
		// log.PurePrintf("%s\n", NewLogItem("SendHearbeatFail", "WARN", NewErrorLogDetail(conf.Options.HeartbeatUrl, err.Error())))
		log.Warnf("Event:SendHearbeatFail\tId:%s\tURL:%s\tError:%s", conf.Options.Id, conf.Options.HeartbeatUrl, err.Error())
		return
	}

	defer resp.Body.Close()
	body, err := ioutil.ReadAll(resp.Body)
	if err != nil {
		// log.PurePrintf("%s\n", NewLogItem("SendHearbeatFail", "WARN", NewErrorLogDetail(conf.Options.HeartbeatUrl, "ReadBodyFail")))
		log.Warnf("Event:SendHearbeatFail\tId:%s\tURL:%s\tReason:ReadBodyFail\tError:%s\t", conf.Options.Id, conf.Options.HeartbeatUrl, err.Error())
		return
	}

	hbResp := HeartbeatResponse{}
	if err := json.Unmarshal(body, &hbResp); err != nil {
		log.Warnf("Event:SendHearbeatFail\tId:%s\tURL:%s\tReason:InvalidResponseBody\tResponse:%s\tError:%s\t", conf.Options.Id, conf.Options.HeartbeatUrl, body, err.Error())
		return
	}

	if hbResp.Error != 0 {
		log.Warnf("Event:SendHearbeatFail\tId:%s\tURL:%s\tReason:ErrorResponse\tResponse:%s\t", conf.Options.Id, conf.Options.HeartbeatUrl, body)
		return
	}
	log.Infof("Event: SendHearbeatDone\tId:%s\t", conf.Options.Id)
}
