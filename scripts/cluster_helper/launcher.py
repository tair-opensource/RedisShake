import os
import signal
import subprocess
from pathlib import Path


class Launcher:
    def __init__(self, args, work_dir):
        self.started = True
        self.args = args
        self.work_dir = work_dir
        if not os.path.exists(work_dir):
            Path(self.work_dir).mkdir(parents=True, exist_ok=True)
        self.stdout_file = open(work_dir + "/stdout", 'a')
        self.stderr_file = open(work_dir + "/stderr", 'a')
        self.process = subprocess.Popen(self.args, stdout=self.stdout_file,
                                        stderr=self.stderr_file, cwd=self.work_dir,
                                        encoding="utf-8")

    def __del__(self):
        assert not self.started, "Every Launcher should be closed manually! work_dir:" + self.work_dir

    def get_pid(self):
        return self.process.pid

    def stop(self):
        if self.started:
            self.started = False
            print(f"Waiting for process {self.process.pid} to exit...")
            self.stdout_file.close()
            self.stderr_file.close()
            self.process.send_signal(signal.SIGINT)
            self.process.wait()
            print(f"process {self.process.pid} exited.")
