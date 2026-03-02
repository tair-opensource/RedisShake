"""
Logging utilities for pybbt.
"""

import datetime
import sys

from rich.console import Console
from rich.markup import escape

LOGO = r'''
 _____     _      ____       _____         _
|_   _|_ _(_)_ __|  _ \ _   |_   _|__  ___| |_
  | |/ _` | | '__| |_) | | | || |/ _ \/ __| __|
  | | (_| | | |  |  __/| |_| || |  __/\__ \ |_
  |_|\__,_|_|_|  |_|    \__, ||_|\___||___/\__|
                        |___/
'''

# Console instances for framework and user output
if sys.stdout.isatty():
    console = Console(highlight=False, log_path=False, soft_wrap=True)
    console_user = Console(highlight=False, soft_wrap=True)
else:
    force_terminal = False
    if "pytest" in sys.modules:
        force_terminal = True
    console = Console(highlight=False, log_path=False, width=1000, force_terminal=force_terminal)
    console_user = Console(highlight=False, width=1000, force_terminal=force_terminal)


def print_logo():
    """Print the pybbt logo."""
    console.print(LOGO)


def inner_print(msg):
    """Internal print for framework messages."""
    console.print(msg)


def log(msg, color="white"):
    """
    Log a message with timestamp.

    Args:
        msg: Message to log.
        color: Color for the message (rich color name).
    """
    date = f"[{datetime.datetime.now().strftime('%F %X.%f')[:-3]}]"
    console_user.print(f"[white]{escape(date)}[/white] {escape(str(msg))}", style=color)


def log_blue(msg):
    """Log a blue message."""
    log(msg, color="blue")


def log_pink(msg):
    """Log a pink message."""
    log(msg, color="deep_pink1")


def log_yellow(msg):
    """Log a yellow message."""
    log(msg, color="yellow")


def log_red(msg):
    """Log a red message."""
    log(msg, color="red")


def log_gray(msg):
    """Log a gray message."""
    log(msg, color="grey27")
