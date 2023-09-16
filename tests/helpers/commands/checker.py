from helpers.redis import Redis


class Checker:
    def add_data(self, r: Redis, cross_slots_cmd: bool):
        ...

    def check_data(self, r: Redis, cross_slots_cmd: bool):
        ...
