import subprocess


class Launcher:
    def __init__(self, args, work_dir):
        self.args = args
        self.work_dir = work_dir
        self.process = None

    def fire(self):
        self.process = subprocess.Popen(self.args, stdout=subprocess.PIPE,
                                        stderr=subprocess.PIPE, cwd=self.work_dir,
                                        encoding="utf-8")

    def readline(self):
        return self.process.stdout.readline().strip()

    def stop(self):
        self.process.terminate()
