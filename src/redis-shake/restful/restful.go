package restful

import (
	"github.com/gugemichael/nimo4go"
	"redis-shake/metric"
	"redis-shake/common"
)

// register all rest api
func RestAPI() {
	registerMetric() // register metric
	// add below if has more
}

func registerMetric() {
	utils.HttpApi.RegisterAPI("/metric", nimo.HttpGet, func([]byte) interface{} {
		return metric.NewMetricRest()
	})
}
