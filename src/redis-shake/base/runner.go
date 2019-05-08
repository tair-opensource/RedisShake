package base

var(
	Status = "null"
	AcceptDB = func(db uint32) bool {
		return db >= 0 && db < 1024
	}

	RDBPipeSize = 1024
)

type Runner interface{
	Main()

	GetDetailedInfo() interface{}
}