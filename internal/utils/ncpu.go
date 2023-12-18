package utils

import (
	"runtime"

	"RedisShake/internal/config"
	"RedisShake/internal/log"
)

func SetNcpu() {
	if config.Opt.Advanced.Ncpu != 0 {
		log.Infof("set ncpu to %d", config.Opt.Advanced.Ncpu)
		runtime.GOMAXPROCS(config.Opt.Advanced.Ncpu)
		log.Infof("set GOMAXPROCS to %v", config.Opt.Advanced.Ncpu)
	} else {
		log.Infof("GOMAXPROCS defaults to the value of runtime.NumCPU [%v]", runtime.NumCPU())
	}
}
