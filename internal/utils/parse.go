package utils

import (
	"regexp"
	"strconv"
)

func ParseDBs(s string) []int {
	dbsString := regexp.MustCompile(`db(\d+):`).FindAllStringSubmatch(s, -1)
	if dbsString == nil {
		return []int{}
	}
	dbs := make([]int, len(dbsString))
	for i, dbString := range dbsString {
		db, _ := strconv.Atoi(dbString[1])
		dbs[i] = db
	}
	return dbs
}
