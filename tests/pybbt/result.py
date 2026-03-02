"""
Test result management for pybbt.
"""

import traceback

from pybbt.logger import console, inner_print


class Result:
    """Represents the result of a single test case."""

    def __init__(self, name: str = "", passed: bool = False, failed: bool = False,
                 skipped: bool = False, output: str = ""):
        self.name = name
        self.passed = passed
        self.failed = failed
        self.skipped = skipped
        self.output = output
        self.time_used = 0.0
        self.errors: list[Exception] = []

    def log(self):
        """Log the test result details."""
        console.print(f"[red bold]Error in case: {self.name}[/red bold]")

        if self.output:
            console.print("Output:", style="red")
            print(self.output, end="")

        for idx, error in enumerate(self.errors):
            console.print(f"Traceback{idx}:", style="red")
            for line in traceback.format_tb(error.__traceback__):
                console.print(line, end="")
            console.print(f"Exception{idx}:", style="red")
            console.print(f"{type(error).__name__}: {error}")


def report(results: list[Result]):
    """
    Generate and print a summary report of all test results.

    Args:
        results: List of Result objects.
    """
    inner_print("\n" + "=" * 50)
    inner_print("Test Summary")
    inner_print("=" * 50 + "\n")

    total = len(results)
    passed = sum(1 for r in results if r.passed)
    skipped = sum(1 for r in results if r.skipped)
    failed = sum(1 for r in results if r.failed)

    # Print summary
    if failed == 0 and skipped == 0:
        inner_print(f"\\o/ All {total} tests [green]passed[/green]!")
    else:
        status = []
        if passed:
            status.append(f"{passed} [green]passed[/green]")
        if skipped:
            status.append(f"{skipped} [purple]skipped[/purple]")
        if failed:
            status.append(f"{failed} [red]failed[/red]")
        inner_print(f"Total {total} tests: {', '.join(status)}")

    # Print failed cases
    if failed > 0:
        inner_print("\n[red]Failed tests:[/red]")
        for r in results:
            if r.failed:
                inner_print(f"  [red]✗[/red] {r.name}")

    inner_print("")
