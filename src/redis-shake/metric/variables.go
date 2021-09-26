package metric

import (
	"fmt"

	"github.com/alibaba/RedisShake/redis-shake/base"
	utils "github.com/alibaba/RedisShake/redis-shake/common"
)

type MetricRest struct {
	StartTime            interface{}
	PullCmdCount         interface{}
	PullCmdCountTotal    interface{}
	BypassCmdCount       interface{}
	BypassCmdCountTotal  interface{}
	PushCmdCount         interface{}
	PushCmdCountTotal    interface{}
	SuccessCmdCount      interface{}
	SuccessCmdCountTotal interface{}
	FailCmdCount         interface{}
	FailCmdCountTotal    interface{}
	Delay                interface{}
	AvgDelay             interface{}
	NetworkSpeed         interface{} // network speed
	NetworkFlowTotal     interface{} // total network speed
	FullSyncProgress     interface{}
	Status               interface{}
	SenderBufCount       interface{} // length of sender buffer
	ProcessingCmdCount   interface{} // length of delay channel
	TargetDBOffset       interface{} // target redis offset
	SourceDBOffset       interface{} // source redis offset
	SourceMasterDBOffset interface{} // source redis master reply offset
	SourceAddress        interface{}
	TargetAddress        interface{}
	Details              interface{} // other details info
}

func NewMetricRest() []MetricRest {
	var detailMapList []map[string]interface{}
	if rawInfo := runner.GetDetailedInfo(); rawInfo != nil {
		detailMapList = runner.GetDetailedInfo().([]map[string]interface{})
	}
	if detailMapList == nil || len(detailMapList) == 0 {
		return []MetricRest{
			{
				StartTime: utils.StartTime,
				Status:    base.Status,
			},
		}
	}

	total := utils.GetTotalLink()
	ret := make([]MetricRest, total)
	for i := 0; i < total; i++ {
		val, ok := MetricMap.Load(i)
		if !ok {
			continue
		}
		singleMetric := val.(*Metric)
		detailMap := detailMapList[i]
		ret[i] = MetricRest{
			StartTime:            utils.StartTime,
			PullCmdCount:         singleMetric.GetPullCmdCount(),
			PullCmdCountTotal:    singleMetric.GetPullCmdCountTotal(),
			BypassCmdCount:       singleMetric.GetBypassCmdCount(),
			BypassCmdCountTotal:  singleMetric.GetBypassCmdCountTotal(),
			PushCmdCount:         singleMetric.GetPushCmdCount(),
			PushCmdCountTotal:    singleMetric.GetPushCmdCountTotal(),
			SuccessCmdCount:      singleMetric.GetSuccessCmdCount(),
			SuccessCmdCountTotal: singleMetric.GetSuccessCmdCountTotal(),
			FailCmdCount:         singleMetric.GetFailCmdCount(),
			FailCmdCountTotal:    singleMetric.GetFailCmdCountTotal(),
			Delay:                fmt.Sprintf("%s ms", singleMetric.GetDelay()),
			AvgDelay:             fmt.Sprintf("%s ms", singleMetric.GetAvgDelay()),
			NetworkSpeed:         singleMetric.GetNetworkFlow(),
			NetworkFlowTotal:     singleMetric.GetNetworkFlowTotal(),
			FullSyncProgress:     singleMetric.GetFullSyncProgress(),
			Status:               base.Status,
			SenderBufCount:       detailMap["SenderBufCount"],
			ProcessingCmdCount:   detailMap["ProcessingCmdCount"],
			TargetDBOffset:       detailMap["TargetDBOffset"],
			SourceDBOffset:       detailMap["SourceDBOffset"],
			SourceMasterDBOffset: detailMap["SourceMasterDBOffset"],
			SourceAddress:        detailMap["SourceAddress"],
			TargetAddress:        detailMap["TargetAddress"],
			Details:              detailMap["Details"],
		}
	}

	return ret
}
