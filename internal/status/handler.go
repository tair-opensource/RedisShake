package status

import (
	"encoding/json"
	"fmt"
	"net/http"
	"time"

	"RedisShake/internal/config"
	"RedisShake/internal/log"
)

func Handler(w http.ResponseWriter, _ *http.Request) {
	w.Header().Add("Content-Type", "application/json")

	bytesChannel := make(chan []byte, 1)

	ch <- func() {
		stat.Consistent = theReader.StatusConsistent() && theWriter.StatusConsistent()
		jsonBytes, err := json.Marshal(stat)
		if err != nil {
			log.Warnf("marshal status info failed, err=[%v]", err)
			bytesChannel <- []byte(fmt.Sprintf(`{"error": "%v"}`, err))
			return
		}
		bytesChannel <- jsonBytes
	}

	select {
	case bytes := <-bytesChannel:
		_, err := w.Write(bytes)
		if err != nil {
			log.Warnf("write status info failed, err=[%v]", err)
		}
	case <-time.After(time.Second * 3):
		log.Warnf("write status info timeout")
		w.WriteHeader(http.StatusRequestTimeout)
	}
}

func setStatusPort() {
	if config.Opt.Advanced.StatusPort != 0 {
		go func() {
			addr := fmt.Sprintf(":%d", config.Opt.Advanced.StatusPort)
			if err := http.ListenAndServe(addr, http.HandlerFunc(Handler)); err != nil {
				log.Panicf(err.Error())
			}
		}()
		log.Infof("status information: http://localhost:%v", config.Opt.Advanced.StatusPort)
		log.Infof("status information: watch -n 0.3 'curl -s http://localhost:%v | python -m json.tool'", config.Opt.Advanced.StatusPort)
	} else {
		log.Infof("not set status port")
	}
}
