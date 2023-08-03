import traceback
import typing

from pybbt.context import global_context as g_ctx
from pybbt.logger import console, is_terminal


class ErrorCase:
    def __init__(self, case_name: str, errors: typing.List[Exception], output: str):
        self.case_name = case_name
        self.errors = errors
        self.output = output

    def log(self):
        if is_terminal:
            console.rule(f"[red bold]Error in case: {self.case_name}[/red bold]")
        else:
            console.print(f"\nError in case: {self.case_name}")
        if not g_ctx.verbose:
            console.print(f"Output:", style="red")
            print(self.output, end="")
        for inx, error in enumerate(self.errors):
            console.print(f"Traceback{inx}:", style="red")
            tb = traceback.format_tb(error.__traceback__)
            for line in tb:
                console.print(line, end="")
            console.print(f"Exception{inx}:", style="red")
            console.print(type(error), error)
