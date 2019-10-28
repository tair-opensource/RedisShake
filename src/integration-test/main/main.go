package main

import (
	"pkg/libs/log"
	"flag"

	"integration-test/tcase"
)

func main() {
	log.SetLevel(log.LEVEL_INFO)
	sourcePort := flag.Int("s", 20001, "source redis port")
	targetPort := flag.Int("t", 30001, "target redis port")
    flag.Parse()

	if sourcePort == nil || targetPort == nil {
		log.Panicf("sourcePort and targetPort should be given")
	}

	log.Infof("run test starts")

	source, target := *sourcePort, *targetPort
	for _, tc := range tcase.CaseList {
		tc.SetInfo(source, target)

		if err := tc.Before(); err != nil {
			log.Panicf("run case %v before stage failed: %v", tc.Info(), err)
		}

		if err := tc.Run(); err != nil {
			log.Panicf("run case %v run stage failed: %v", tc.Info(), err)
		}

		if err := tc.Before(); err != nil {
			log.Panicf("run case %v after stage failed: %v", tc.Info(), err)
		}

		// +50 in different case
		source += 50
		target += 50
	}

	log.Infof("finish all test case")
}
