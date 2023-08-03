import threading
import typing


class SafeThread(threading.Thread):
    def __init__(self, func: typing.Callable):
        threading.Thread.__init__(self)
        self.func = func
        self.error: typing.Optional[Exception] = None

    def run(self) -> typing.NoReturn:
        # noinspection PyBroadException
        try:
            self.func()
        except Exception as e:
            self.error = e

    def has_error(self) -> bool:
        return self.error is not None

    def get_error(self) -> Exception:
        return self.error


if __name__ == '__main__':
    thread = SafeThread(lambda: 1 / 0)
    thread.start()
    thread.join()
    print(thread.has_error())
    print(thread.get_error())
