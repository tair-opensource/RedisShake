package subCase

import (
	redigo "github.com/garyburd/redigo/redis"
	"integration-test/inject"
	"pkg/libs/log"
	"integration-test/deploy"
	"strings"
    "fmt"
	"net/http"
    "time"
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
	sourceConn       redigo.Conn
	targetConn       redigo.Conn
	shakeConf        map[string]interface{}
	shakeDir         string // redis-shake binary directory location
	runDir           string // redis-shake run directory
	resumeBreakpoint bool   // enable resume from break point?
}

func GenerateShakeConf(sourcePort, targetPort int, filterKeyBlack, filterKeyWhite []string,
	filterDBBlack, filterDBWhite []string, targetDB string, resumeBreakpoint bool) map[string]interface{} {
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
    mp["id"] = "redis-shake-integration"
    mp["log.file"] = "redis-shake-integration.log"
    mp["http_profile"] = "9320"
    mp["rewrite"] = true

    if resumeBreakpoint {
    	mp["resume_from_break_point"] = true
    }

	return mp
}

func NewSubCase(sourceConn, targetConn redigo.Conn, sourcePort, targetPort int,
	filterKeyBlack, filterKeyWhite []string,
	filterDBBlack, filterDBWhite []string,
	targetDB string, resumeBreakpoint bool) *SubCase {
	sc := &SubCase{
		sourceConn: sourceConn,
		targetConn: targetConn,
		shakeConf: GenerateShakeConf(sourcePort, targetPort,
			filterKeyBlack, filterKeyWhite,
			filterDBBlack, filterDBWhite,
			targetDB, resumeBreakpoint),
		shakeDir:         "..",
		runDir:           "redis-shake",
		resumeBreakpoint: resumeBreakpoint,
	}

	return sc
}

func (sc *SubCase) Run() {
	// 1. inject key before full sync
    log.Info("1. inject key before full sync")
	_, err := inject.InjectData(sc.sourceConn)
	if err != nil {
		log.Panicf("inject data before sync failed[%v]", err)
	}

	// 2. start redis-shake
    log.Info("2. start redis-shake")
    if err := sc.startShake(); err != nil {
        log.Panicf("start redis-shake failed[%v]", err)
    }

	// 3. check full-sync finish
    log.Info("3. check full-sync finish")
	type Val struct {
		Status string `json:"Status"`
	}
	val := []Val{
        {},
    }
	for {
        time.Sleep(2 * time.Second)

		request, err := http.Get(fmt.Sprintf("http://localhost:%d/metric", 9320))
		if err != nil {
			log.Panicf("curl redis-shake failed[%v]", err)
		}
		body, err := ioutil.ReadAll(request.Body)
		if err != nil {
			log.Panicf("parse redis-shake curl request failed[%v]", err)
		}
		if err = json.Unmarshal(body, &val); err != nil {
			log.Panicf("unmarshal redis-shake curl request failed[%v]", err)
		}
		request.Body.Close()

		// break until finish full sync
        if len(val) == 0 {
            log.Panic("invalid restful return length")
        }
		if val[0].Status == "incr" {
            log.Info("check full-sync finish")
			break
		}

        log.Info("check full-sync not ready")
	}

	// 4. start redis-full-check to check the correctness
    log.Info("4. run full-check")
	fullCheckConf := map[string]interface{}{
		"s":            sc.shakeConf["source.address"],
		"t":            sc.shakeConf["target.address"],
		"comparetimes": 1,
	}
	equal, err := deploy.RunFullCheck(sc.runDir, fullCheckConf)
	if err != nil {
		log.Panicf("start redis-full-check failed[%v]", err)
	}
	if !equal {
		log.Panicf("redis-full-check not equal")
	}

	// 5. stop shake
    log.Info("5. stop shake")
	err = deploy.StopShake(sc.shakeConf)
	if err != nil {
		log.Errorf("stop shake failed: %v", err)
	}

    log.Info("all finish")
}

func (sc *SubCase) startShake() error {
	// 1.1: start shake
	if err := deploy.StartShake(sc.shakeDir, sc.runDir, sc.shakeConf, "sync"); err != nil {
		return fmt.Errorf("start redis-shake 1th failed[%v]", err)
	}

	if !sc.resumeBreakpoint {
		return nil
	}

	// 1.2: stop shake
	err := deploy.StopShake(sc.shakeConf)
	if err != nil {
		return fmt.Errorf("stop shake 1th failed: %v", err)
	}

	// 1.3: inject data
	_, err = inject.InjectData(sc.sourceConn)
	if err != nil {
		return fmt.Errorf("inject data 1th failed[%v]", err)
	}

	time.Sleep(10 * time.Second)

	// 2.1: start shake
	if err := deploy.StartShake(sc.shakeDir, sc.runDir, sc.shakeConf, "sync"); err != nil {
		return fmt.Errorf("start redis-shake 2th failed[%v]", err)
	}

	return nil
}