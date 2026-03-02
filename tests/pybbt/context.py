"""
Global context for pybbt test execution.
"""

import threading


class GlobalContext:
    """
    Global context shared across all test cases.

    Attributes:
        lock: Reentrant lock for thread safety.
        direct_run: Whether running directly (python case.py).
        verbose: Whether to show verbose output.
        stop_asap: Flag to stop all tests immediately.
        parallel: Number of parallel test threads.
        dont_stop: Continue running tests after failures.
        start_from_case_index: Start tests from this index.
        skip_flags: Set of flags to skip.
        skip_flags_logic: Logic for skip flags ('or' or 'and').
    """

    def __init__(self):
        self.lock = threading.RLock()

        # Running mode
        self.direct_run = True  # True when running python case.py directly

        # Control flags
        self.verbose = False
        self.stop_asap = False
        self.parallel = 1
        self.dont_stop = False
        self.start_from_case_index = 0

        # Flags
        self.skip_flags = set()
        self.skip_flags_logic = "or"  # 'or' or 'and'
        self.global_flags = set()  # Global flags from CLI


# Singleton global context
global_context = GlobalContext()


def get_global_flags() -> set:
    """Get the set of global flags passed via CLI."""
    return global_context.global_flags
