package deploy

import (
	"path/filepath"
	"os"
	"fmt"
	"os/exec"
	"syscall"
	"strconv"
)

const (
	StandaloneScript = "deploy_standalone.sh"
	ClusterScript    = "deploy_cluster.sh"

	RedisShake     = "redis-shake"
	RedisShakeConf = "redis-shake.conf"

	CmdStart = "start"
	CmdStop  = "stop"
)

func run(cmd *exec.Cmd) error {
	if err := cmd.Start(); err != nil {
		return fmt.Errorf("start failed[%v]", err)
	}

	if err := cmd.Wait(); err != nil {
		if exiterr, ok := err.(*exec.ExitError); ok {
			// The program has exited with an exit code != 0

			// This works on both Unix and Windows. Although package
			// syscall is generally platform dependent, WaitStatus is
			// defined for both Unix and Windows and in both cases has
			// an ExitStatus() method with the same signature.
			if status, ok := exiterr.Sys().(syscall.WaitStatus); ok {
				if retCode := status.ExitStatus(); retCode != 0 {
					return fmt.Errorf("run with exit code[%v]", retCode)
				}
			}
		} else {
			return fmt.Errorf("wait failed[%s]", err)
		}
	}
	return nil
}

/*
 * tp: standalone, cluster
 * port: redis-server port
 * cmd: start/stop
 * node: node number, only used in cluster
 */
func Deploy(tp string, port int, cmd string, node int) error {
	script := StandaloneScript
	if tp == "cluster" {
		script = ClusterScript
	}

	dir, err := filepath.Abs(filepath.Dir(os.Args[0]))
	if err != nil {
		return fmt.Errorf("get path failed[%v]", err)
	}

	portS := fmt.Sprintf("%d", port)
	path := fmt.Sprintf("%s/%s", dir, script)

	var execCmd *exec.Cmd
	if tp != "cluster" {
		execCmd = exec.Command(path, portS, cmd)
	} else {
		execCmd = exec.Command(path, portS, cmd, strconv.Itoa(node))
	}

	return run(execCmd)
}

// start redis-shake with given configuration, mode means sync/rump/dump/restore/decode
func StartShake(shakeDir, runDir string, conf map[string]interface{}, mode string) error {
	if _, err := os.Stat(runDir); os.IsNotExist(err) {
		if err := os.Mkdir(runDir, os.ModePerm); err != nil {
			return fmt.Errorf("mkdir %v failed[%v]", runDir, err)
		}
	}

	from := fmt.Sprintf("%s/%s", shakeDir, RedisShake)
	to := fmt.Sprintf("%s/%s", runDir, RedisShake)
	cpCmd := exec.Command("cp", from, to)
	if err := run(cpCmd); err != nil {
		return fmt.Errorf("copy file from [%v] to [%v] failed[%v]", from, to, err)
	}

	f, err := os.Create(RedisShakeConf)
	if err != nil {
		return err
	}

	// write conf
	for key, val := range conf {
		_, err := f.WriteString(fmt.Sprintf("%v = %v\n", key, val))
		if err != nil {
			return err
		}
	}

	// start redis-shake
	execCmd := exec.Command(to, fmt.Sprintf("-type=%s", mode), fmt.Sprintf("-conf=%s", RedisShakeConf), "&")
	return run(execCmd)
}
