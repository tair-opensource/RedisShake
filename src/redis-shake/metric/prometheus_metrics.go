package metric

import (
	"strconv"

	"redis-shake/common"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promauto"
)

const (
	metricNamespace   = "redisshake"
	dbSyncerLabelName = "db_syncer"
)

var (
	pullCmdCountTotal = promauto.NewCounterVec(
		prometheus.CounterOpts{
			Namespace: metricNamespace,
			Name:      "pull_cmd_count_total",
			Help:      "RedisShake pull redis cmd count in total",
		},
		[]string{dbSyncerLabelName},
	)
	bypassCmdCountTotal = promauto.NewCounterVec(
		prometheus.CounterOpts{
			Namespace: metricNamespace,
			Name:      "bypass_cmd_count_total",
			Help:      "RedisShake bypass redis cmd count in total",
		},
		[]string{dbSyncerLabelName},
	)
	pushCmdCountTotal = promauto.NewCounterVec(
		prometheus.CounterOpts{
			Namespace: metricNamespace,
			Name:      "push_cmd_count_total",
			Help:      "RedisShake push redis cmd count in total",
		},
		[]string{dbSyncerLabelName},
	)
	successCmdCountTotal = promauto.NewCounterVec(
		prometheus.CounterOpts{
			Namespace: metricNamespace,
			Name:      "success_cmd_count_total",
			Help:      "RedisShake push redis cmd count in total",
		},
		[]string{dbSyncerLabelName},
	)
	failCmdCountTotal = promauto.NewCounterVec(
		prometheus.CounterOpts{
			Namespace: metricNamespace,
			Name:      "fail_cmd_count_total",
			Help:      "RedisShake push redis cmd count in total",
		},
		[]string{dbSyncerLabelName},
	)
	networkFlowTotalInBytes = promauto.NewCounterVec(
		prometheus.CounterOpts{
			Namespace: metricNamespace,
			Name:      "network_flow_total_in_bytes",
			Help:      "RedisShake total network flow in total (byte)",
		},
		[]string{dbSyncerLabelName},
	)
	fullSyncProcessPercent = promauto.NewGaugeVec(
		prometheus.GaugeOpts{
			Namespace: metricNamespace,
			Name:      "full_sync_process_percent",
			Help:      "RedisShake full sync process (%)",
		},
		[]string{dbSyncerLabelName},
	)
	averageDelayInMs = promauto.NewGaugeVec(
		prometheus.GaugeOpts{
			Namespace: metricNamespace,
			Name:      "average_delay_in_ms",
			Help:      "RedisShake average delay (ms)",
		},
		[]string{dbSyncerLabelName},
	)
)

// CalcPrometheusMetrics calculates some prometheus metrics e.g. average delay.
func CalcPrometheusMetrics() {
	total := utils.GetTotalLink()
	for i := 0; i < total; i++ {
		val, ok := MetricMap.Load(i)
		if !ok {
			continue
		}
		singleMetric := val.(*Metric)
		averageDelayInMs.WithLabelValues(strconv.Itoa(i)).Set(singleMetric.GetAvgDelayFloat64())
	}
}
