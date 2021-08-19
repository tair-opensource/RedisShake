package base

var (
	Status      = "null"
	RDBPipeSize = 1024
)

type Runner interface {
	Main()

	GetDetailedInfo() interface{}
}
