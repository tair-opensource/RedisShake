package tcase

import (
	"integration-test/deploy"
	"fmt"
    "time"
	shakeUtils "redis-shake/common"
	"pkg/libs/log"
	"integration-test/subCase"
)

type Standalone2StandaloneCase struct {
	SourcePort int
	TargetPort int
}

func (s2s *Standalone2StandaloneCase) SetInfo(sourcePort, targetPort int) {
	s2s.SourcePort = sourcePort
	s2s.TargetPort = targetPort
}

func (s2s *Standalone2StandaloneCase) Info() string {
	return fmt.Sprintf("standalone->standalone")
}

func (s2s *Standalone2StandaloneCase) Before() error {
	// deploy source redis with given port
	if err := deploy.Deploy(deploy.StandaloneScript, s2s.SourcePort, deploy.CmdStart, -1); err != nil {
		return fmt.Errorf("deploy source redis failed: %v", err)
	}

	// deploy target redis with given port
	if err := deploy.Deploy(deploy.StandaloneScript, s2s.TargetPort, deploy.CmdStart, -1); err != nil {
		return fmt.Errorf("deploy source redis failed: %v", err)
	}

    // wait ready
    time.Sleep(3 * time.Second)

	return nil
}

func (s2s *Standalone2StandaloneCase) Run() error {
	{
		// case 1
		log.Info("case 1: all")
		// build client
		sourceConn := shakeUtils.OpenRedisConn([]string{fmt.Sprintf(":%d", s2s.SourcePort)}, "auth", "", false, false)
		targetConn := shakeUtils.OpenRedisConn([]string{fmt.Sprintf(":%d", s2s.TargetPort)}, "auth", "", false, false)

		sc := subCase.NewSubCase(sourceConn, targetConn, s2s.SourcePort, s2s.TargetPort, nil, nil,
			nil, nil, "", false)
		log.Info("run")
		sc.Run()
	}

	return nil
}

func (s2s *Standalone2StandaloneCase) After() error {
	// close source redis
	if err := deploy.Deploy(deploy.StandaloneScript, s2s.SourcePort, deploy.CmdStop, -1); err != nil {
		return fmt.Errorf("close source redis failed: %v", err)
	}

	// close target redis
	if err := deploy.Deploy(deploy.StandaloneScript, s2s.TargetPort, deploy.CmdStop, -1); err != nil {
		return fmt.Errorf("close source redis failed: %v", err)
	}

	return nil
}
