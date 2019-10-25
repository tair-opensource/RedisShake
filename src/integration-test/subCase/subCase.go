package subCase

import (
	redigo "github.com/garyburd/redigo/redis"
	"integration-test/inject"
	"pkg/libs/log"
	"integration-test/deploy"
	"strings"
	"net/http"
	"fmt"
	"io/ioutil"
	"encoding/json"
)

/*
 * normal: normal case
 * filterKey: filter key
 * filterDB: filter db
 * targetDB: target db != -1
 */
type SubCase struct {
	sourceConn redigo.Conn
	targetConn redigo.Conn
	shakeConf  map[string]interface{}
	shakeDir   string // redis-shake binary directory location
	runDir     string // redis-shake run directory
}

func GenerateShakeConf(sourcePort, targetPort int, filterKeyBlack, filterKeyWhite []string,
	filterDBBlack, filterDBWhite []string, targetDB string) map[string]interface{} {
	mp := make(map[string]interface{})
	mp["source.address"] = fmt.Sprintf("127.0.0.1:%d", sourcePort)
	mp["target.address"] = fmt.Sprintf("127.0.0.1:%d", targetPort)

	if len(filterKeyBlack) != 0 {
		mp["filter.key.blacklist"] = strings.Join(filterKeyBlack, ";")
	}
	if len(filterKeyWhite) != 0 {
		mp["filter.key.whitelist"] = strings.Join(filterKeyWhite, ";")
	}

	if len(filterDBBlack) != 0 {
		mp["filter.db.blacklist"] = strings.Join(filterDBBlack, ";")
	}
	if len(filterDBWhite) != 0 {
		mp["filter.db.whitelist"] = strings.Join(filterDBWhite, ";")
	}

	mp["target.db"] = targetDB

	return mp
}

func NewSubCase(sourceConn, targetConn redigo.Conn, sourcePort, targetPort int,
	filterKeyBlack, filterKeyWhite []string,
	filterDBBlack, filterDBWhite []string,
	targetDB string) *SubCase {
	sc := &SubCase{
		sourceConn: sourceConn,
		targetConn: targetConn,
		shakeConf: GenerateShakeConf(sourcePort, targetPort,
			filterKeyBlack, filterKeyWhite,
			filterDBBlack, filterDBWhite,
			targetDB),
		shakeDir: "../",
		runDir:   "redis-shake/",
	}

	return sc
}

func (sc *SubCase) Run() {
	// 1. inject key before full sync
	_, err := inject.InjectData(sc.sourceConn)
	if err != nil {
		log.Panicf("inject data before sync failed[%v]", err)
	}

	// 2. start redis-shake
	if err := deploy.StartShake(sc.shakeDir, sc.runDir, sc.shakeConf, "sync"); err != nil {
		log.Panicf("start redis-shake failed[%v]", err)
	}

	// 3. check full-sync finish
	type Val struct {
		Status string `json:"status"`
	}
	var val Val
	for {
		request, err := http.Get(fmt.Sprintf("http://localhost:%d/foo", 9320))
		if err != nil {
			log.Panicf("curl redis-shake failed[%v]", err)
		}
		body, err := ioutil.ReadAll(request.Body)
		if err != nil {
			log.Panicf("parse redis-shake curl request failed[%v]", err)
		}
		if err = json.Unmarshal(body, val); err != nil {
			log.Panicf("unmarshal redis-shake curl request failed[%v]", err)
		}
		request.Body.Close()

		// break until finish full sync
		if val.Status == "incr" {
			break
		}
	}

	// 4. start redis-full-check to check the correctness
	fullCheckConf := map[string]interface{}{
		"s":            sc.shakeConf["source.address"],
		"t":            sc.shakeConf["target.address"],
		"comparetimes": 1,
	}
	equal, err := deploy.RunFullCheck(sc.shakeDir, sc.runDir, fullCheckConf)
	if err != nil {
		log.Panicf("start redis-full-check failed[%v]", err)
	}
	if !equal {
		log.Panicf("redis-full-check not equal")
	}
}
