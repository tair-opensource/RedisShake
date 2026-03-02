"""
Test case decorators and context management for pybbt.
"""

import atexit
import inspect
import os
import sys
import threading
import typing
from functools import wraps
from itertools import product

from pybbt.context import global_context as g_ctx
from pybbt.logger import console, console_user, inner_print
from pybbt.result import Result, report
from pybbt.utils.timer import Timer

# Thread-local storage for case context
_local_context = threading.local()

# Global test registry
_lock = threading.Lock()
_all_cases: list = []
_case_index = 0
_results: list = []


class CaseContext:
    """
    Context for a single test case.

    Provides:
    - Unique working directory for the test
    - Exit hooks for cleanup
    - Test execution and result tracking
    """

    DEFAULT_CASE_DIR = "tmp"

    def __init__(self, func, flags: list):
        self.flags = flags
        self.func = func

        # Determine working directory
        rel_path = os.path.relpath(inspect.getfile(func))
        self.dir = f"{self.DEFAULT_CASE_DIR}/{rel_path}/{func.__name__}"
        self.name = f"{rel_path}::{func.__name__}"

        self._hooks = []

    def add_exit_hook(self, hook: typing.Callable):
        """Add a hook to be called when the test exits."""
        self._hooks.append(hook)

    def run(self) -> Result:
        """Execute the test case and return the result."""
        _local_context.case_context = self
        result = Result(name=self.name)

        def _run():
            timer = Timer()
            try:
                self.func()
            except Exception as e:
                result.failed = True
                result.errors.append(e)

            # Run exit hooks
            for hook in self._hooks:
                try:
                    hook()
                except Exception as e:
                    result.failed = True
                    result.errors.append(e)

            if not result.failed:
                result.passed = True
            result.time_used = timer.elapsed()

        if g_ctx.verbose:
            _run()
        else:
            with console_user.capture() as capture:
                _run()
            result.output = capture.get()

        return result


def get_case_context() -> CaseContext:
    """Get the current test case context."""
    return getattr(_local_context, 'case_context', None)


def _generate_flag_combinations(flags: dict) -> list:
    """Generate all combinations of flags."""
    if not flags:
        return [[]]

    keys = list(flags.keys())
    values = [flags[k] for k in keys]

    result = []
    for combo in product(*values):
        flag_list = []
        for k, v in zip(keys, combo):
            if v:
                flag_list.append(f"{k},{v}")
            else:
                flag_list.append(k)
        result.append(flag_list)
    return result


def case(flags: dict = None, skip: bool = False, linux_only: bool = False):
    """
    Decorator to mark a function as a test case.

    Args:
        flags: Dictionary of flag names to list of flag values.
               Each combination will be tested separately.
        skip: If True, skip this test.
        linux_only: If True, skip on non-Linux platforms.

    Example:
        @case({"mode": ["fast", "slow"]})
        def test_something():
            ASSERT_EQ(1, 1)

        # Runs twice with flags ["mode,fast"] and ["mode,slow"]
    """
    if flags is None:
        flags = {"": [""]}

    # Validate flags
    for v in flags.values():
        if not v:
            flags = {"": [""]}
            break

    def decorator(func):
        if skip:
            return func
        if linux_only and sys.platform != "linux":
            return func
        if inspect.signature(func).parameters:
            raise ValueError(f"Test function {func.__name__} should not have parameters")

        # Register all flag combinations as separate cases
        for flag_list in _generate_flag_combinations(flags):
            _all_cases.append(CaseContext(func, flag_list))

        return func

    return decorator


def _run_single_case(ctx: CaseContext, idx: int, total: int) -> Result:
    """Run a single test case and return its result."""
    if g_ctx.skip_flags & set(ctx.flags):
        inner_print(f"[{idx}/{total}] [yellow]skip[/yellow] {ctx.name} ({','.join(ctx.flags)})")
        result = Result(name=ctx.name, skipped=True)
    else:
        inner_print(f"[{idx}/{total}] [green][bold]{ctx.name}[/bold][/green] ({','.join(ctx.flags)})")
        result = ctx.run()

    return result


def _run_tests():
    """Run all registered test cases."""
    global _case_index

    threads = []
    for _ in range(g_ctx.parallel):
        t = threading.Thread(target=_worker)
        threads.append(t)
        t.start()

    for t in threads:
        t.join()

    report(_results)


def _worker():
    """Worker thread for running test cases."""
    global _case_index

    while True:
        with _lock:
            if _case_index >= len(_all_cases):
                return
            idx = _case_index + 1
            ctx = _all_cases[_case_index]
            _case_index += 1

        # Check if we should skip this case
        if idx < g_ctx.start_from_case_index:
            continue

        if g_ctx.stop_asap:
            return

        result = _run_single_case(ctx, idx, len(_all_cases))

        with _lock:
            _results.append(result)

        if result.failed:
            if g_ctx.verbose:
                result.log()
            if not g_ctx.dont_stop:
                g_ctx.stop_asap = True
                return

        # Print result summary
        if result.passed:
            inner_print(f"[{idx}/{len(_all_cases)}] [bold][green]✓[/green] {result.name} ({result.time_used:.2f}s)[/bold]")
        elif result.failed:
            inner_print(f"[{idx}/{len(_all_cases)}] [bold][red]✗[/red] {result.name} ({result.time_used:.2f}s)[/bold]")


@atexit.register
def _atexit_handler():
    """Run tests when script exits (if running directly)."""
    if "pytest" in sys.modules:
        return
    if g_ctx.direct_run and len(_all_cases) > 0:
        _run_tests()


# Expose for __main__.py
def get_all_cases():
    return _all_cases


def get_results():
    return _results


def reset():
    """Reset test registry (useful for testing)."""
    global _case_index
    with _lock:
        _all_cases.clear()
        _results.clear()
        _case_index = 0


def run_all():
    """Run all registered tests and return exit code."""
    _run_tests()
    return 1 if any(r.failed for r in _results) else 0
