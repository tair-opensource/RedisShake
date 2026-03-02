"""
Safe thread implementation for pybbt.
"""

import threading
import typing


class SafeThread(threading.Thread):
    """
    A thread that captures exceptions for later retrieval.

    Example:
        thread = SafeThread(lambda: 1 / 0)
        thread.start()
        thread.join()
        if thread.has_error():
            print(f"Thread failed: {thread.get_error()}")
    """

    def __init__(self, func: typing.Callable):
        """
        Initialize the safe thread.

        Args:
            func: The function to run in the thread.
        """
        threading.Thread.__init__(self)
        self.func = func
        self.error: typing.Optional[Exception] = None

    def run(self):
        """Execute the function and capture any exceptions."""
        try:
            self.func()
        except Exception as e:
            self.error = e

    def has_error(self) -> bool:
        """Check if the thread encountered an error."""
        return self.error is not None

    def get_error(self) -> Exception:
        """Get the error that occurred, if any."""
        return self.error


if __name__ == '__main__':
    # Example usage
    thread = SafeThread(lambda: 1 / 0)
    thread.start()
    thread.join()
    print(f"Has error: {thread.has_error()}")
    print(f"Error: {thread.get_error()}")
