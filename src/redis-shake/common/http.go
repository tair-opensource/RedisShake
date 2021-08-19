package utils

import (
	"github.com/gugemichael/nimo4go"
)

var (
	HttpApi *nimo.HttpRestProvider
)

func InitHttpApi(port int) {
	HttpApi = nimo.NewHttpRestProvdier(port)
}
