import argparse
import importlib
import importlib.util
import os
import shutil
import signal
import sys
import threading
from typing import List

from pybbt.context import get_runtime_context, global_context as g_ctx
from pybbt.logger import inner_print, log_logo

EXAMPLE = """Example:
# simple usage
pybbt cases/test/test0.py   # run test0.py
pybbt cases/test/           # run all the test cases in test/
pybbt cases/                # run all the test cases in cases/ and sub directories
# filter 
pybbt cases/ --filter tag0                                      # run all the test cases with tag0
pybbt cases/ --filter "not tag0"                                # run all the test cases without tag0
pybbt cases/ --filter "tag0 and tag1"                           # run all the test cases with tag0 and tag1
pybbt cases/ --filter "tag0 or tag1"                            # run cases that contain tag0 or tag1
pybbt cases/ --filter "not (tag0 or tag1)"                      # equivalent to "not tag0 and not tag1"
pybbt cases/ --filter "not (tag0 and tag1)"                     # equivalent to "not tag0 or not tag1"
pybbt cases/ --filter "(tag0 or tag1) and not (tag2 or tag3)"   # equivalent to "(tag0 or tag1) and not tag2 and not tag3"
# other options
pybbt cases/ --flags flag0 flag1    # pass flags0 and flags1 to the test cases
pybbt cases/ --dont-stop            # run all the cases even if some of them fail
pybbt cases/ --verbose              # show all the log
"""


def parse_args():
    parser = argparse.ArgumentParser(prog="pybbt",
                                     description="Python Black Box Test",
                                     formatter_class=argparse.RawDescriptionHelpFormatter,
                                     epilog=EXAMPLE)
    parser.add_argument("file_or_dir", type=str, help="test file path or directory path of test cases")
    parser.add_argument("--filter", type=str, default="",
                        help="filter the test cases")
    parser.add_argument("--flags", type=str, nargs="+", default=[],
                        help="flags that can be obtained through pybbt.get_flags() in the code")
    parser.add_argument("--parallel", type=int, default=1, help="run cases in parallel")
    parser.add_argument("--start-from", type=str, default="", help="start from the case, example: --start-from cases/test/test0.py")
    parser.add_argument("--dont-stop", action="store_true", default=False,
                        help="wont stop when error occurs")
    parser.add_argument("--verbose", action="store_true", default=False,
                        help="show all the log")
    args = parser.parse_args()
    if args.parallel > 1 and args.verbose:
        raise RuntimeError("Cannot use --verbose option when --parallel > 1")
    inner_print(f"file_or_dir: {args.file_or_dir}")
    inner_print(f"--filter: {args.filter}")
    inner_print(f"--flags: {args.flags}")
    inner_print(f"--parallel: {args.parallel}")
    inner_print(f"--start-from: {args.start_from}")
    inner_print(f"--dont-stop: {args.dont_stop}")
    inner_print(f"--verbose: {args.verbose}")
    inner_print("")
    return args


def get_cases(file_or_dir) -> List[str]:
    if os.path.isfile(file_or_dir):
        return [file_or_dir]
    elif os.path.isdir(file_or_dir):
        cases = []
        for root, dirs, files in os.walk(file_or_dir):
            for file in files:
                if file.endswith(".py") and not file.startswith("_"):
                    cases.append(os.path.join(root, file))
        return cases
    else:
        raise RuntimeError(f"Invalid file_or_dir parameter, {file_or_dir} is not a file or directory.")


def log_result_and_exit():
    inner_print("\n         The End\n")
    inner_print(f"\\o/ Total {len(g_ctx.cases)} cases, "
                f"{g_ctx.passed_cases} [green]passed[/green], "
                f"{g_ctx.skipped_cases} [purple]skipped[/purple], "
                f"{g_ctx.failed_cases} [red]failed[/red].", )
    if g_ctx.errors:
        for ec in g_ctx.errors:
            ec.log()
        sys.exit(1)
    sys.exit(0)


def run_test(tid):
    g_ctx.lock.acquire()
    g_ctx.bind_thread_local_runtime_context()
    ctx = get_runtime_context()
    ctx.thread_id = f"thread_{tid}"
    while True:
        inx = g_ctx.case_index
        g_ctx.case_index += 1
        if inx >= len(g_ctx.cases):
            break
        case = g_ctx.cases[inx]

        # prepare runtime context
        ctx.case_name = case
        ctx.case_index = inx + 1
        ctx.case_dir = os.path.abspath(f"tmp/{case.replace('/', '.')[:-3]}")

        # run case
        abs_path = os.path.abspath(case)
        spec = importlib.util.spec_from_file_location("case", abs_path)
        module = importlib.util.module_from_spec(spec)
        spec.loader.exec_module(module)
        if "main" not in dir(module):
            inner_print(f"{case} not has main()")
            g_ctx.stop_asap = True
            return
        module.main()
    inner_print(f"[green]Thread {ctx.thread_id} finished[/green]")
    g_ctx.lock.release()


def run_all_cases():
    g_ctx.threads = []
    for i in range(0, g_ctx.parallel):
        t = threading.Thread(target=run_test, args=[i + 1])
        g_ctx.threads.append(t)
        t.start()
    for t in g_ctx.threads:
        t.join()
    log_result_and_exit()


g_ctrl_c_count = 0


def signal_handler(sig, frame):
    global g_ctrl_c_count
    g_ctx.lock.acquire()
    g_ctrl_c_count += 1
    if g_ctrl_c_count >= 2:
        inner_print(f"[bold purple]\n You pressed Ctrl+C again! Exit immediately. [/bold purple]\n")
        # noinspection PyProtectedMember
        os._exit(1)
    inner_print(f"[bold purple]\n You pressed Ctrl+C! Press again to exit immediately. [/bold purple]\n")

    # main thread
    g_ctx.stop_asap = True

    g_ctx.lock.release()
    for t in g_ctx.threads:
        t.join()
    log_result_and_exit()


def get_tags_from_filter(expression: str):
    expr = expression.replace("(", " ").replace(")", " ")
    return set(tag.strip() for tag in expr.split() if tag not in ["and", "or", "not"])


def main():
    sys.path.insert(0, os.getcwd())  # add current directory to sys.path
    signal.signal(signal.SIGINT, signal_handler)
    log_logo()

    # parse args
    parser = parse_args()
    cases = get_cases(parser.file_or_dir)
    cases.sort()

    # prepare global context
    g_ctx.direct_run = False
    g_ctx.cases = cases
    g_ctx.verbose = parser.verbose
    g_ctx.stop_asap = False
    g_ctx.parallel = parser.parallel
    g_ctx.dont_stop = parser.dont_stop
    if parser.start_from:
        g_ctx.start_from_case_index = cases.index(parser.start_from) + 1
    g_ctx.filter = parser.filter
    g_ctx.filter_tags = get_tags_from_filter(parser.filter)
    g_ctx.flags = parser.flags

    # clear tmp directory
    shutil.rmtree("tmp", ignore_errors=True)

    run_all_cases()


if __name__ == "__main__":
    main()
