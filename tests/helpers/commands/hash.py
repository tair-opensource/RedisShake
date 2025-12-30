import pybbt

from helpers.commands.checker import Checker
from helpers.constant import REDIS_SERVER_VERSION
from helpers.redis import Redis


class HashChecker(Checker):
    PREFIX = "hash"

    def __init__(self):
        self.cnt = 0

    def add_data(self, r: Redis, cross_slots_cmd: bool):
        p = r.pipeline()
        # basic hash commands
        p.hset(f"{self.PREFIX}_{self.cnt}_basic", "field1", "value1")
        p.hset(f"{self.PREFIX}_{self.cnt}_basic", "field2", "value2")
        p.hset(f"{self.PREFIX}_{self.cnt}_basic", mapping={"field3": "value3", "field4": "value4"})
        ret = p.execute()
        pybbt.ASSERT_EQ(ret, [1, 1, 2])

        # Redis 8.0+ hash field expiration commands
        if REDIS_SERVER_VERSION >= 8.0:
            p = r.pipeline()
            # HSETEX with EX (seconds)
            p.execute_command("HSETEX", f"{self.PREFIX}_{self.cnt}_hsetex_ex", "EX", 3600, "FIELDS", 2, "f1", "v1", "f2", "v2")
            # HSETEX with PX (milliseconds)
            p.execute_command("HSETEX", f"{self.PREFIX}_{self.cnt}_hsetex_px", "PX", 3600000, "FIELDS", 1, "f1", "v1")
            # HSETEX with EXAT (unix timestamp seconds)
            import time
            future_ts = int(time.time()) + 3600
            p.execute_command("HSETEX", f"{self.PREFIX}_{self.cnt}_hsetex_exat", "EXAT", future_ts, "FIELDS", 1, "f1", "v1")
            # HSETEX without expiration
            p.execute_command("HSETEX", f"{self.PREFIX}_{self.cnt}_hsetex_no_ttl", "FIELDS", 2, "f1", "v1", "f2", "v2")
            ret = p.execute()
            pybbt.ASSERT_EQ(ret, [1, 1, 1, 1])

        self.cnt += 1

    def check_data(self, r: Redis, cross_slots_cmd: bool):
        for i in range(self.cnt):
            p = r.pipeline()
            p.hgetall(f"{self.PREFIX}_{i}_basic")
            ret = p.execute()
            # Compare dict content (order-independent)
            expected = {b"field1": b"value1", b"field2": b"value2", b"field3": b"value3", b"field4": b"value4"}
            pybbt.ASSERT_TRUE(ret[0] == expected)

            # Redis 8.0+ check
            if REDIS_SERVER_VERSION >= 8.0:
                p = r.pipeline()
                p.hgetall(f"{self.PREFIX}_{i}_hsetex_ex")
                p.hgetall(f"{self.PREFIX}_{i}_hsetex_px")
                p.hgetall(f"{self.PREFIX}_{i}_hsetex_exat")
                p.hgetall(f"{self.PREFIX}_{i}_hsetex_no_ttl")
                ret = p.execute()
                # Use == for dict comparison (order-independent)
                pybbt.ASSERT_TRUE(ret[0] == {b"f1": b"v1", b"f2": b"v2"})
                pybbt.ASSERT_TRUE(ret[1] == {b"f1": b"v1"})
                pybbt.ASSERT_TRUE(ret[2] == {b"f1": b"v1"})
                pybbt.ASSERT_TRUE(ret[3] == {b"f1": b"v1", b"f2": b"v2"})

                # Check TTL is set (HTTL returns seconds)
                ttl = r.do("HTTL", f"{self.PREFIX}_{i}_hsetex_ex", "FIELDS", 1, "f1")
                pybbt.ASSERT_TRUE(ttl[0] > 0)
