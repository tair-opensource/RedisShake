package tcase

var (
	CaseList = []Base{new(Standalone2StandaloneCase),}
)

type Base interface {
	SetInfo(sourcePort, targetPort int) // set
	Info() string  // name
	Before() error // prepare
	Run() error    // run
	After() error  // finish
}
