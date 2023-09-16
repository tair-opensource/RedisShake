from helpers.commands import SelectChecker, StringChecker, TairStringChecker, TairHashChecker, TairZsetChecker 
from helpers.redis import Redis
from helpers.constant import PATH_REDIS_SERVER, REDIS_SERVER_VERSION


class DataInserter:
    def __init__(self, ):
        self.checkers = [
            StringChecker(),
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
