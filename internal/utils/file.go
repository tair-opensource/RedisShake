package utils

import (
	"os"
	"path/filepath"

	"RedisShake/internal/log"
)

func CreateEmptyDir(dir string) {
	if IsExist(dir) {
		err := os.RemoveAll(dir)
		if err != nil {
			log.Panicf("remove dir failed. dir=[%s], error=[%v]", dir, err)
		}
	}
	err := os.MkdirAll(dir, 0777)
	if err != nil {
		log.Panicf("mkdir failed. dir=[%s], error=[%v]", dir, err)
	}
	log.Debugf("CreateEmptyDir: dir=[%s]", dir)
}

func IsExist(path string) bool {
	_, err := os.Stat(path)
	if err != nil {
		if os.IsNotExist(err) {
			return false
		} else {
			log.Panicf(err.Error())
		}
	}
	return true
}

func GetFileSize(path string) uint64 {
	fi, err := os.Stat(path)
	if err != nil {
		log.Panicf(err.Error())
	}
	return uint64(fi.Size())
}

func GetAbsPath(path string) string {
	absolutePath, err := filepath.Abs(path)
	if err != nil {
		log.Panicf(err.Error())
	}
	return absolutePath
}
