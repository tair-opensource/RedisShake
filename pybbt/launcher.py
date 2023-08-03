import os.path
import platform
import signal
import subprocess
import time
from pathlib import Path

from pybbt.logger import log_gray, log_red
from pybbt.utils import Timer


class Launcher:
    def __init__(self, args, work_dir: str):
        self.started = False
        if not os.path.exists(args[0]):
            log_red(f"Executable {args[0]} not found")
            raise Exception(f"Executable program not found")

        self.args = args
        self.work_dir = work_dir
        if not os.path.exists(work_dir):
            Path(self.work_dir).mkdir(parents=True, exist_ok=True)
        self.stdout_file = open(work_dir + "/stdout", 'ab')
        self.stderr_file = open(work_dir + "/stderr", 'ab')
        log_gray(f"launcher start at {self.work_dir}")
        self.started_time = time.time()
        self.process = subprocess.Popen(self.args, stdout=self.stdout_file,
                                        stderr=self.stderr_file, cwd=self.work_dir,
                                        encoding="utf-8")
        self.started = True

    def __del__(self):
        if self.started:
            log_red(f"Should stop process {self.process.pid}({self.work_dir}) before delete launcher")

    def get_pid(self):
        return self.process.pid

    def stop(self, force=False):
        if self.started:
            self.started = False
            ti = Timer()
            log_gray(f"try to stop process {self.process.pid}")
            self.process.send_signal(signal.SIGKILL if force else signal.SIGINT)
            try:
                self.process.wait(20)  # wait 20 seconds
            except subprocess.TimeoutExpired:
                if platform.system() == "Linux":
                    log_red(f"Process {self.process.pid} kill timeout, try log stack trace")
                    os.system(f"pstack {self.process.pid} > {self.work_dir}/pstack.{self.process.pid}")
                    os.system(f"cat {self.work_dir}/pstack.{self.process.pid}")
                    log_red(f"Process {self.process.pid} is still running, force kill it!")
                    self.process.send_signal(signal.SIGKILL)
                    self.process.wait()
                    input("Press Enter to continue...")
                    raise Exception(f"Process exited timeout")
                else:
                    log_red(f"Process {self.process.pid} is still running, check it!")
                    time.sleep(3600 * 24 * 365)

            self.stdout_file.close()
            self.stderr_file.close()
            log_gray(f"launcher stop at {self.work_dir} ({ti.elapsed():.2f} seconds)")

    def is_started(self):
        return self.started

    def wait_stop(self, timeout=10):
        self.process.wait(timeout)
        self.started = True
