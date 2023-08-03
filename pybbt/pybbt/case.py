import sys

from pybbt.context import get_runtime_context, global_context as g_ctx
from pybbt.error_case import ErrorCase
from pybbt.logger import console_user, inner_print
from pybbt.subcase import SubcaseError
from pybbt.utils import create_empty_dir, Timer


def check_tags(expression: str, expression_tags, tags: set[str]):
    if expression == "":
        return True
    for tag in expression_tags:
        if tag in tags:
            expression = expression.replace(tag, "True")
        else:
            expression = expression.replace(tag, "False")
    return eval(expression)


def run_case(func):
    error = None
    ti = Timer()
    try:
        func()
    except Exception as e:
        error = e
    return error, ti.elapsed()


def case(tags=None, skip=False):
    if tags is None:
        tags = []
    ctx = get_runtime_context()
    create_empty_dir(ctx.case_dir)
    ctx.case_tags = tags

    # hack for run single case
    if g_ctx.direct_run:
        g_ctx.lock.acquire()  # acquire before release in run_test

    def decorate(func):
        def wrapper():
            nonlocal tags, skip
            # get case name
            case_name = ctx.case_name
            if case_name == "":
                case_name = __file__

            # skip case
            if skip or not check_tags(g_ctx.filter, g_ctx.filter_tags, set(tags)) or ctx.case_index < g_ctx.start_from_case_index:
                g_ctx.skipped_cases += 1
                inner_print(f"[{ctx.case_index}/{len(g_ctx.cases)}] "
                            f"[purple]skip[/purple] [bold]{case_name}[/bold]")
                return

            if g_ctx.parallel == 1:
                inner_print(f"\nTesting: [bold]{case_name}[/bold]")
            else:
                inner_print(f"Start testing: [bold]{case_name}[/bold] in thread {ctx.thread_id}")

            output = ""
            if g_ctx.verbose:  # capture output for --verbose option
                case_error, time_used = run_case(func)
                exit_errors = ctx.on_exit()
            else:
                with console_user.capture() as capture:
                    case_error, time_used = run_case(func)
                    exit_errors = ctx.on_exit()
                output = capture.get()

            if g_ctx.stop_asap:
                inner_print(f"[green]Thread {ctx.thread_id} stopped at {case_name} after {time_used:.2f} seconds[/green]")
                g_ctx.lock.release()
                sys.exit(0)

            # success
            if case_error is None and not exit_errors:
                g_ctx.passed_cases += 1
                inner_print(f"[{ctx.case_index}/{len(g_ctx.cases)}] "
                            f"[green]ok[/green] "
                            f"[bold]{case_name} ({time_used:.2f} seconds)[/bold]")
                return

            # failed
            g_ctx.failed_cases += 1
            inner_print(f"[{ctx.case_index}/{len(g_ctx.cases)}] "
                        f"[red]error[/red] "
                        f"[bold]{case_name} ({time_used:.2f} seconds)[/bold]")

            errors = []
            if isinstance(case_error, SubcaseError):
                errors.extend(case_error.errors)
            elif isinstance(case_error, Exception):
                errors.append(case_error)
            errors.extend(exit_errors)

            ec = ErrorCase(case_name, errors, output)

            if g_ctx.direct_run:
                ec.log()
                sys.exit(1)

            g_ctx.errors.append(ec)
            if not g_ctx.dont_stop:  # in multi thread mode, we stop all threads when there is error
                g_ctx.stop_asap = True
                g_ctx.lock.release()
                inner_print(f"[red]Thread {ctx.thread_id} stopped at {case_name} because of error[/red]")
                sys.exit(0)

        return wrapper

    return decorate
