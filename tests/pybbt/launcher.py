"""
Process launcher for pybbt.
"""

import os
import platform
import signal
import subprocess
import time
from pathlib import Path
from typing import Optional

from pybbt.logger import log_gray, log_red
from pybbt.utils.timer import Timer


class Launcher:
    """
    A process launcher for running external programs in tests.

    Example:
        launcher = Launcher(["python", "-m", "http.server"], work_dir="/tmp/server")
        # ... do something ...
        launcher.stop()
    """

    def __init__(self, args: list, work_dir: str, preload_lib: str = None, env: dict = None):
        """
        Initialize and start a process.

        Args:
            args: Command line arguments (list of strings).
            work_dir: Working directory for the process.
            preload_lib: Optional library to preload (Linux only).
            env: Optional environment variables to add/override.

        Raises:
            FileNotFoundError: If the executable is not found.
        """
        self._started = False
        self._args = args
        self._work_dir = work_dir

        if not os.path.exists(args[0]):
            raise FileNotFoundError(f"Executable not found: {args[0]}")

        # Create work directory
        Path(work_dir).mkdir(parents=True, exist_ok=True)

        # Open output files
        self._stdout_file = open(os.path.join(work_dir, "stdout"), 'ab')
        self._stderr_file = open(os.path.join(work_dir, "stderr"), 'ab')

        # Build environment
        process_env = os.environ.copy()
        if env:
            process_env.update(env)

        # Handle preload library (Linux only)
        if preload_lib and platform.system() == "Linux":
            if not os.path.exists(preload_lib):
                raise FileNotFoundError(f"Preload library not found: {preload_lib}")
            process_env['LD_PRELOAD'] = preload_lib

        log_gray(f"Starting process: {' '.join(args)}")
        log_gray(f"Working directory: {work_dir}")

        self._process = subprocess.Popen(
            args,
            stdout=self._stdout_file,
            stderr=self._stderr_file,
            cwd=work_dir,
            env=process_env
        )
        self._started = True
        log_gray(f"Process started with PID: {self._process.pid}")

    def __del__(self):
        """Destructor - warn if process was not stopped."""
        if self._started:
            log_red(f"Warning: Process {self._process.pid} ({self._work_dir}) should be stopped before deleting launcher")

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        self.stop()
        return False

    @property
    def pid(self) -> int:
        """Get the process ID."""
        return self._process.pid

    @property
    def returncode(self) -> Optional[int]:
        """Get the return code (None if still running)."""
        return self._process.returncode

    def is_running(self) -> bool:
        """Check if the process is still running."""
        return self._process.poll() is None

    def wait_stop(self, timeout: float = None):
        """Wait for the process to finish (alias for wait)."""
        return self.wait(timeout=timeout)

    def wait(self, timeout: float = None) -> int:
        """
        Wait for the process to finish.

        Args:
            timeout: Maximum time to wait in seconds.

        Returns:
            Return code of the process.

        Raises:
            subprocess.TimeoutExpired: If timeout is reached.
        """
        return self._process.wait(timeout=timeout)

    def stop(self, timeout: float = 30, force: bool = False) -> bool:
        """
        Stop the process gracefully.

        Args:
            timeout: Maximum time to wait for graceful shutdown.
            force: If True, send SIGKILL instead of SIGTERM.

        Returns:
            True if process stopped, False if timeout.
        """
        if not self._started:
            return True

        self._started = False
        timer = Timer()

        log_gray(f"Stopping process {self._process.pid}...")

        try:
            if force:
                self._process.kill()
            else:
                self._process.terminate()

            self._process.wait(timeout=timeout)
            log_gray(f"Process {self._process.pid} stopped ({timer.elapsed():.2f}s)")
            return True

        except subprocess.TimeoutExpired:
            log_red(f"Process {self._process.pid} did not stop within {timeout}s")
            if not force:
                log_red(f"Force killing process {self._process.pid}")
                self._process.kill()
                self._process.wait()
            return False

        finally:
            self._stdout_file.close()
            self._stderr_file.close()

    def get_stdout(self) -> bytes:
        """Read and return the stdout content."""
        with open(os.path.join(self._work_dir, "stdout"), 'rb') as f:
            return f.read()

    def get_stderr(self) -> bytes:
        """Read and return the stderr content."""
        with open(os.path.join(self._work_dir, "stderr"), 'rb') as f:
            return f.read()
