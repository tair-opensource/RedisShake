package utils

import (
	"os"
	"path/filepath"

	"RedisShake/internal/config"
	"RedisShake/internal/log"

	"github.com/theckman/go-flock"
)

var filelock *flock.Flock

func ChdirAndAcquireFileLock() {
	// dir
	dir, err := filepath.Abs(config.Opt.Advanced.Dir)
	if err != nil {
		log.Panicf("failed to determine current directory: %v", err)
	}
	// create dir
	err = os.MkdirAll(dir, os.ModePerm)
	if err != nil {
		log.Panicf("failed to create dir. dir: %v, err: %v", dir, err)
	}
	filelock = flock.New(filepath.Join(dir, "pid.lockfile"))
	locked, err := filelock.TryLock()
	if err != nil {
		log.Panicf("failed to lock pid file: %v", err)
	}
	if !locked {
		log.Warnf("failed to lock pid file")
	}
	err = os.Chdir(dir) // change dir
	if err != nil {
		log.Panicf("failed to change dir. dir: %v, err: %v", dir, err)
	}
	log.Infof("changed work dir to [%s]", dir)
}

func ReleaseFileLock() {
	if filelock != nil {
		err := filelock.Unlock()
		if err != nil {
			log.Warnf("failed to unlock pid file: %v", err)
		}
	}
}
