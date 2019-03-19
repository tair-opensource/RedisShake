package metric

import(
	"fmt"
	"redis-shake/base"
	"redis-shake/common"
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
}

func NewMetricRest() *MetricRest {
	detailedInfo := runner.GetDetailedInfo()
	if len(detailedInfo) < 4 {
		return &MetricRest{}
	}
	senderBufCount := detailedInfo[0]
	processingCmdCount := detailedInfo[1]
	targetDbOffset := detailedInfo[2]
	sourceDbOffset := detailedInfo[3]

	return &MetricRest{
		StartTime:            utils.StartTime,
		PullCmdCount:         MetricVar.GetPullCmdCount(),
		PullCmdCountTotal:    MetricVar.GetPullCmdCountTotal(),
		BypassCmdCount:       MetricVar.GetBypassCmdCount(),
		BypassCmdCountTotal:  MetricVar.GetBypassCmdCountTotal(),
		PushCmdCount:         MetricVar.GetPushCmdCount(),
		PushCmdCountTotal:    MetricVar.GetPushCmdCountTotal(),
		SuccessCmdCount:      MetricVar.GetSuccessCmdCount(),
		SuccessCmdCountTotal: MetricVar.GetSuccessCmdCountTotal(),
		FailCmdCount:         MetricVar.GetFailCmdCount(),
		FailCmdCountTotal:    MetricVar.GetFailCmdCountTotal(),
		Delay:                fmt.Sprintf("%s ms", MetricVar.GetDelay()),
		AvgDelay:             fmt.Sprintf("%s ms", MetricVar.GetAvgDelay()),
		NetworkSpeed:         MetricVar.GetNetworkFlow(),
		NetworkFlowTotal:     MetricVar.GetNetworkFlowTotal(),
		FullSyncProgress:     MetricVar.GetFullSyncProgress(),
		Status:               base.Status,
		SenderBufCount:       senderBufCount,
		ProcessingCmdCount:   processingCmdCount,
		TargetDBOffset:       targetDbOffset,
		SourceDBOffset:       sourceDbOffset,
	}
}