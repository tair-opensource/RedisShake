import inspect
import typing
from functools import wraps

from pybbt.context import get_runtime_context, global_context as g_ctx
from pybbt.logger import inner_print, log_pink
from pybbt.utils import Timer


def get_subcase_name(func, *args, **kwargs):
    ctx = get_runtime_context()
    func_args = inspect.signature(func).bind(*args, **kwargs).arguments
    for k, v in func_args.items():
        if isinstance(v, list):
            v = [str(x) for x in v]
        v = str(v)
        if v.startswith(ctx.work_dir):
            v = v.replace(ctx.work_dir, ".")
        func_args[k] = v
    ret = [f"{x[0]}={str(x[1])}" for x in func_args.items()]
    func_args_str = ", ".join(ret).replace("'", '"')
    return f"{func.__qualname__}({func_args_str})"


class SubcaseError(Exception):
    def __init__(self, errors: typing.List[Exception]):
        self.errors = errors


def subcase(skipped: bool = False):
    def decorate(func):
        @wraps(func)
        def wrapper(*args, **kwargs):
            ctx = get_runtime_context()
            subcase_name = get_subcase_name(func, *args, **kwargs)
            ti = Timer()
            ret = None

            if skipped:
                inner_print(f"[purple]◼[/purple] skip {subcase_name} ({ti.elapsed():.2f} seconds)")
                return None

            log_pink(f"Testing subcase: {subcase_name}")
            g_ctx.lock.release()
            ctx.in_subcase = True
            case_error = None
            try:
                ret = func(*args, **kwargs)
            except Exception as e:
                case_error = e
            exit_errors = ctx.on_exit()
            ctx.in_subcase = False
            g_ctx.lock.acquire()

            if g_ctx.stop_asap:
                raise SubcaseError([])

            # success
            if case_error is None and not exit_errors:
                inner_print(f"[green]✓[/green] {subcase_name} ({ti.elapsed():.2f} seconds)")
                return ret

            # failed
            inner_print(f"[red]✗[/red] {subcase_name} ({ti.elapsed():.2f} seconds)")
            errors = exit_errors if case_error is None else [case_error] + exit_errors
            raise SubcaseError(errors)

        return wrapper

    return decorate
