import subprocess


class Launcher:
    def __init__(self, args, work_dir):
        self.args = args
        self.work_dir = work_dir
        self.process = None

    def fire(self):
        self.process = subprocess.Popen(self.args, stdout=subprocess.PIPE,
                                        stderr=subprocess.PIPE, cwd=self.work_dir, encoding="utf-8")
        # while True:
        #     ret = process.poll()
        #     if ret is None:  # No process is done, wait a bit and check again.
        #         time.sleep(.1)
        #         continue
        # out, err = process.communicate()
        # print(out)

    def p(self):
        out, err = self.process.communicate()
        print(out, err)
