from helpers.commands import (
    SelectChecker, StringChecker, ListChecker, HashChecker, StreamChecker,
    TairHashChecker, TairStringChecker, TairZsetChecker
)
from helpers.redis import Redis


class DataInserter:
    def __init__(self, ):
        self.checkers = [
            StringChecker(),
            ListChecker(),
            HashChecker(),
            StreamChecker(),
            SelectChecker(),
            TairStringChecker(),
            TairHashChecker(),
            TairZsetChecker(),
        ]

    def add_data(self, r: Redis, cross_slots_cmd: bool):
        for checker in self.checkers:
            checker.add_data(r, cross_slots_cmd)

    def check_data(self, r: Redis, cross_slots_cmd: bool):
        for checker in self.checkers:
            checker.check_data(r, cross_slots_cmd)
