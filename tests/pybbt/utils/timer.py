"""
Timer utility for measuring elapsed time.
"""

import time


class Timer:
    """A simple timer for measuring elapsed time."""

    def __init__(self):
        """Initialize the timer."""
        self._start_time = time.perf_counter()

    def elapsed(self) -> float:
        """
        Get elapsed time in seconds.

        Returns:
            float: Elapsed time in seconds.
        """
        return time.perf_counter() - self._start_time

    def reset(self):
        """Reset the timer."""
        self._start_time = time.perf_counter()
