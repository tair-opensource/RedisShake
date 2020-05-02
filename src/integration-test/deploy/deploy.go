package deploy

import (
    "path/filepath"
    "pkg/libs/log"
    "os"
    "fmt"
    "strings"
    "os/exec"
    "syscall"
    "strconv"
	"io/ioutil"
)

const (
    StandaloneScript = "deploy_standalone.sh"
    ClusterScript    = "deploy_cluster.sh"

    RedisShake             = "redis-shake.linux"
    RedisShakeConf         = "redis-shake.conf"
    RedisFullCheck         = "redis-full-check"
    RedisFullCheckDiffFile = "redis-full-check.diff"
    RedisFullCheckLog      = "redis-full-check.log"

    CmdStart = "start"
    CmdStop  = "stop"
)

func runWait(cmd *exec.Cmd) error {
    if err := cmd.Wait(); err != nil {
        if exiterr, ok := err.(*exec.ExitError); ok {
            // The program has exited with an exit code != 0

            // This works on both Unix and Windows. Although package
            // syscall is generally platform dependent, WaitStatus is
            // defined for both Unix and Windows and in both cases has
            // an ExitStatus() method with the same signature.
            if status, ok := exiterr.Sys().(syscall.WaitStatus); ok {
                if retCode := status.ExitStatus(); retCode != 0 {
                    log.Panicf("run with exit code[%v]", retCode)
                    return fmt.Errorf("run with exit code[%v]", retCode)
                }
            }
        } else {
            log.Errorf("wait failed[%s]", err)
            return fmt.Errorf("wait failed[%s]", err)
        }
    }
    return nil
}

func run(cmd *exec.Cmd, wait bool) error {
    log.Info("run start")
    if err := cmd.Start(); err != nil {
        return fmt.Errorf("start failed[%v]", err)
    }

    log.Infof("run wait[%v]", wait)
    if wait == false {
        // go runWait(cmd)
        return nil
    }
    return runWait(cmd)
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
    path := fmt.Sprintf("%s/test/%s", dir, script)

    var execCmd *exec.Cmd
    if tp != "cluster" {
        execCmd = exec.Command(path, portS, cmd)
    } else {
        execCmd = exec.Command(path, portS, cmd, strconv.Itoa(node))
    }

    return run(execCmd, true)
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
    log.Infof("copy shake from [%v] to [%v]", from, to)
    cpCmd := exec.Command("cp", from, to)
    if err := run(cpCmd, true); err != nil {
        return fmt.Errorf("copy file from [%v] to [%v] failed[%v]", from, to, err)
    }

    shakeConf := fmt.Sprintf("%s/%s", runDir, RedisShakeConf)
    f, err := os.Create(shakeConf)
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

    log.Info("start shake")
    // start redis-shake
    execCmd := exec.Command("nohup", to, fmt.Sprintf("-type=%s", mode), fmt.Sprintf("-conf=%s", shakeConf))
    return run(execCmd, false)
}

func StopShake(conf map[string]interface{}) error {
	filename := fmt.Sprintf("%s.pid", conf["id"])
	data, err := ioutil.ReadFile(filename)
	if err != nil {
		return fmt.Errorf("open file[%v] failed[%v]", filename, err)
	}

    dataS := strings.TrimRight(string(data), "\n\r")

	pid, err := strconv.Atoi(dataS)
	if err != nil {
		return fmt.Errorf("parse pid[%v] failed[%v]", dataS, pid)
	}

	syscall.Kill(pid, 9)
	return nil
}

func RunFullCheck(runDir string, conf map[string]interface{}) (bool, error) {
    if _, err := os.Stat(runDir); os.IsNotExist(err) {
        if err := os.Mkdir(runDir, os.ModePerm); err != nil {
            return false, fmt.Errorf("mkdir %v failed[%v]", runDir, err)
        }
    }

    from := fmt.Sprintf("tools/%s", RedisFullCheck)
    to := fmt.Sprintf("%s/%s", runDir, RedisFullCheck)
    cpCmd := exec.Command("cp", from, to)
    if err := run(cpCmd, true); err != nil {
        return false, fmt.Errorf("copy file from [%v] to [%v] failed[%v]", from, to, err)
    }

    execCmd := exec.Command(to, fmt.Sprintf("-s=%s", conf["s"]), fmt.Sprintf("-t=%s", conf["t"]),
        fmt.Sprintf("--result=%s/%s", runDir, RedisFullCheckDiffFile),
        fmt.Sprintf("--comparetimes=%v", conf["comparetimes"]),
        fmt.Sprintf("--log=%s", RedisFullCheckLog))
    if err := run(execCmd, true); err != nil {
        return false, fmt.Errorf("run redis-full-check failed[%v]", err)
    }

    diffFile := fmt.Sprintf("%s/%s",  runDir, RedisFullCheckDiffFile)
    f, err := os.Stat(diffFile)
    if err != nil {
        return false, fmt.Errorf("stat file[%v] failed[%v]", RedisFullCheckDiffFile, err)
    }

    return f.Size() == 0, nil
}

