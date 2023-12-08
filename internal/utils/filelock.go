package utils

import (
	"os"
	"path/filepath"

	"RedisShake/internal/config"
	"RedisShake/internal/log"

	"github.com/gofrs/flock"
)

var filelock *flock.Flock

func ChdirAndAcquireFileLock() {
	// dir
	dir, _ := filepath.Abs(config.Opt.Advanced.Dir)
	file := filepath.Join(dir, "pid.lockfile")
	filelock = flock.New(file)
	locked, err := filelock.TryLock()
	if err != nil {
		log.Panicf("failed to lock file, maybe another instance is running. err=[%v], file=[%v]", err, file)
	}
	if !locked {
		log.Panicf("failed to lock pid file, another RedisShake instance is running?")
	}
	err = os.Chdir(dir) // change dir
	if err != nil {
		log.Panicf("failed to change dir. dir=[%v], err=[%v]", dir, err)
	}
	log.Infof("changed work dir. dir=[%s]", dir)
}

func ReleaseFileLock() {
	if filelock != nil {
		err := filelock.Unlock()
		if err != nil {
			log.Warnf("failed to unlock pid file. err=[%v]", err)
		}
	}
}
