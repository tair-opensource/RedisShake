import os.path
import threading
import typing


class CaseContext:
    def __init__(self):
        self.tags = set()
        self.name = ""
        self.dir = ""

    @staticmethod
    def add_exit_hook(hook: typing.Callable):
        get_runtime_context().add_exit_hook(hook)


class RuntimeContext:
    def __init__(self):
        self.work_dir = os.path.abspath(".")

        # stack
        self.case_name = ""
        self.case_index = 1
        self.case_tags = []
        self.case_dir = os.path.abspath("tmp")
        self.in_subcase = False
        self.before_exit_case_hooks = []
        self.before_exit_subcase_hooks = []

        # multi thread
        self.thread_id: str = "main_thread"

    def add_exit_hook(self, hook: typing.Callable):
        if self.in_subcase:
            self.before_exit_subcase_hooks.append(hook)
        else:
            self.before_exit_case_hooks.append(hook)

    def on_exit(self, all_object=False) -> typing.List[Exception]:  # return True if there is error
        errors = []
        hooks = self.before_exit_subcase_hooks if self.in_subcase else self.before_exit_case_hooks
        if all_object:
            hooks = self.before_exit_subcase_hooks + self.before_exit_case_hooks
        for hook in hooks:
            try:
                hook()
            except Exception as e:
                errors.append(e)
        hooks.clear()
        return errors

    def get_case_context(self):
        ctx = CaseContext()
        ctx.tags = self.case_tags
        ctx.name = self.case_name
        ctx.dir = self.case_dir
        return ctx


class GlobalContext:
    def __init__(self):
        self.lock = threading.RLock()

        # cases
        self.direct_run = True
        self.cases = []
        self.case_index = 0
        self.failed_cases = 0
        self.passed_cases = 0
        self.skipped_cases = 0
        self.errors: typing.List = []

        # control
        self.verbose = True
        self.stop_asap = False
        self.parallel = 1
        self.dont_stop = False
        self.start_from_case_index = 0

        # filter
        self.filter = ""
        self.filter_tags = set()

        # flag
        self.flags = {}

        # multi threads
        self.threads: [threading.Thread] = []
        self.thread_local = threading.local()
        self.bind_thread_local_runtime_context()  # empty runtime context for main thread

    def bind_thread_local_runtime_context(self):
        self.thread_local.runtime_context = RuntimeContext()


global_context = GlobalContext()


def get_runtime_context() -> RuntimeContext:
    return global_context.thread_local.runtime_context


def get_global_flags():
    return global_context.flags


def get_case_context():
    return get_runtime_context().get_case_context()


__all__ = ["global_context", "get_runtime_context", "get_global_flags", "get_case_context"]
