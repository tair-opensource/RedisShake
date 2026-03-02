"""
Command-line interface for pybbt.
"""

import argparse
import importlib.util
import os
import shutil
import sys
import time
from typing import List

# Ensure local pybbt module is used (the directory containing this file)
_local_pybbt_dir = os.path.dirname(os.path.abspath(__file__))
if _local_pybbt_dir not in sys.path:
    sys.path.insert(0, _local_pybbt_dir)

from pybbt.case import get_all_cases, get_results, run_all, reset
from pybbt.context import global_context as g_ctx
from pybbt.logger import inner_print, print_logo
from pybbt.result import report

EXAMPLE = """Example:
pybbt cases/test_example.py    # run test_example.py
pybbt cases/                    # run all tests in cases/
pybbt cases/ --parallel 4       # run tests in parallel
pybbt cases/ --dont-stop        # don't stop on first failure
pybbt cases/ --verbose          # show all output
pybbt cases/ --skip-flags slow  # skip tests with 'slow' flag
"""


def parse_args():
    """Parse command line arguments."""
    parser = argparse.ArgumentParser(
        prog="pybbt",
        description="Python Black Box Test - A simple and powerful testing framework",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog=EXAMPLE
    )
    parser.add_argument("file_or_dir", type=str,
                        help="test file path or directory path of test cases")
    parser.add_argument("--skip-flags", type=str, nargs="+", default=[],
                        help="skip cases with these flags")
    parser.add_argument("--flags", type=str, nargs="+", default=[],
                        help="global flags for tests (e.g., --flags modules)")
    parser.add_argument("--parallel", type=int, default=1,
                        help="run cases in parallel (default: 1)")
    parser.add_argument("--start-from", type=int, default=0,
                        help="start from the case, example: --start-from 3")
    parser.add_argument("--dont-stop", action="store_true", default=False,
                        help="don't stop when error occurs")
    parser.add_argument("--verbose", action="store_true", default=False,
                        help="show all output")
    args = parser.parse_args()

    if args.parallel > 1 and args.verbose:
        raise RuntimeError("Cannot use --verbose option when --parallel > 1")

    inner_print(f"file_or_dir: {args.file_or_dir}")
    inner_print(f"--skip-flags: {args.skip_flags}")
    inner_print(f"--flags: {args.flags}")
    inner_print(f"--parallel: {args.parallel}")
    inner_print(f"--start-from: {args.start_from}")
    inner_print(f"--dont-stop: {args.dont_stop}")
    inner_print(f"--verbose: {args.verbose}")
    inner_print("")

    return args


def get_cases(file_or_dir) -> List[str]:
    """Get list of test case files."""
    if os.path.isfile(file_or_dir):
        return [file_or_dir]
    elif os.path.isdir(file_or_dir):
        cases = []
        for root, dirs, files in os.walk(file_or_dir):
            for file in files:
                if file.endswith(".py") and not file.startswith("_"):
                    cases.append(os.path.join(root, file))
        return sorted(cases)
    else:
        raise RuntimeError(f"Invalid file_or_dir parameter, {file_or_dir} is not a file or directory.")


def main():
    """Main entry point for the pybbt CLI."""
    g_ctx.direct_run = False
    g_ctx.stop_asap = False

    # Add current directory to sys.path
    sys.path.insert(0, os.getcwd())

    print_logo()

    # Parse arguments
    args = parse_args()
    cases = get_cases(args.file_or_dir)

    # Configure global context
    g_ctx.verbose = args.verbose
    g_ctx.parallel = args.parallel
    g_ctx.dont_stop = args.dont_stop
    g_ctx.start_from_case_index = args.start_from
    g_ctx.skip_flags = set(args.skip_flags)
    g_ctx.global_flags = set(args.flags)

    # Clear tmp directory
    shutil.rmtree("tmp", ignore_errors=True)

    # Import all test cases to register them
    for case in cases:
        abs_path = os.path.abspath(case)
        spec = importlib.util.spec_from_file_location("case", abs_path)
        module = importlib.util.module_from_spec(spec)
        spec.loader.exec_module(module)

    # Run tests
    exit_code = run_all()

    return exit_code


if __name__ == "__main__":
    sys.exit(main())
