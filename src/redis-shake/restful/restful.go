package restful

import (
	"net/http"
	"redis-shake/common"
	"redis-shake/metric"

	"github.com/gugemichael/nimo4go"
	"github.com/prometheus/client_golang/prometheus/promhttp"
)

// register all rest api
func RestAPI() {
	registerMetric()           // register metric
	registerPrometheusMetric() // register prometheus metrics
	// add below if has more
}

func registerMetric() {
	utils.HttpApi.RegisterAPI("/metric", nimo.HttpGet, func([]byte) interface{} {
		return metric.NewMetricRest()
	})
}

func registerPrometheusMetric() {
	http.HandleFunc("/metrics", func(w http.ResponseWriter, req *http.Request) {
		metric.CalcPrometheusMetrics()
		promhttp.Handler().ServeHTTP(w, req)
	})
}
