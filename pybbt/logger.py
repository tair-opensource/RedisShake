import datetime

from rich.console import Console
from rich.markup import escape

logo_str = '''
 _____     _      ____       _____         _
|_   _|_ _(_)_ __|  _ \ _   |_   _|__  ___| |_
  | |/ _` | | '__| |_) | | | || |/ _ \/ __| __|
  | | (_| | | |  |  __/| |_| || |  __/\__ \ |_
  |_|\__,_|_|_|  |_|    \__, ||_|\___||___/\__|
                        |___/
'''

is_terminal = Console().is_terminal
if is_terminal:
    console = Console(highlight=False, log_path=False)
    console_user = Console(highlight=False)
else:
    console = Console(highlight=False, log_path=False, width=1000)
    console_user = Console(highlight=False, width=1000)


def log_logo():
    console.print(logo_str)


def inner_print(msg):
    console.print(msg)


def log(msg, color="white"):
    date = f"[{datetime.datetime.now().strftime('%F %X.%f')[:-3]}]"
    console_user.print(f"[white]{escape(date)}[/white] {escape(str(msg))}", style=color)


def log_blue(msg):
    log(msg, color="blue")


def log_pink(msg):
    log(msg, color="deep_pink1")


def log_yellow(msg):
    log(msg, color="yellow")


def log_red(msg):
    log(msg, color="red")


def log_gray(msg):
    log(msg, color="grey27")
