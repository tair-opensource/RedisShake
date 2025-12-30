import pybbt

from helpers.commands.checker import Checker
from helpers.constant import REDIS_SERVER_VERSION
from helpers.redis import Redis


class StreamChecker(Checker):
    PREFIX = "stream"

    def __init__(self):
        self.cnt = 0
        self.entry_ids = []  # store entry IDs for later verification

    def add_data(self, r: Redis, cross_slots_cmd: bool):
        if REDIS_SERVER_VERSION < 5.0:
            return

        p = r.pipeline()
        # basic stream commands
        p.xadd(f"{self.PREFIX}_{self.cnt}_basic", {"field1": "value1"})
        p.xadd(f"{self.PREFIX}_{self.cnt}_basic", {"field2": "value2"})
        p.xadd(f"{self.PREFIX}_{self.cnt}_basic", {"field3": "value3"})
        ret = p.execute()
        # Store the entry IDs (they are auto-generated)
        self.entry_ids.append(ret)
        pybbt.ASSERT_EQ(len(ret), 3)

        # Create consumer group for testing XACKDEL (Redis 8.2+)
        if REDIS_SERVER_VERSION >= 8.2:
            # Create a stream for XACKDEL test
            entry_id = r.do("XADD", f"{self.PREFIX}_{self.cnt}_xackdel", "*", "f1", "v1")
            r.do("XADD", f"{self.PREFIX}_{self.cnt}_xackdel", "*", "f2", "v2")
            r.do("XGROUP", "CREATE", f"{self.PREFIX}_{self.cnt}_xackdel", "mygroup", "0", "MKSTREAM")
            # Read entries to make them pending
            r.do("XREADGROUP", "GROUP", "mygroup", "consumer1", "COUNT", "1", "STREAMS", f"{self.PREFIX}_{self.cnt}_xackdel", ">")

        self.cnt += 1

    def check_data(self, r: Redis, cross_slots_cmd: bool):
        if REDIS_SERVER_VERSION < 5.0:
            return

        for i in range(self.cnt):
            # Check stream length
            length = r.do("XLEN", f"{self.PREFIX}_{i}_basic")
            pybbt.ASSERT_EQ(length, 3)

            # Check stream entries exist
            entries = r.do("XRANGE", f"{self.PREFIX}_{i}_basic", "-", "+")
            pybbt.ASSERT_EQ(len(entries), 3)

            # Redis 8.2+ XACKDEL test
            if REDIS_SERVER_VERSION >= 8.2:
                # Check the stream for xackdel exists
                length = r.do("XLEN", f"{self.PREFIX}_{i}_xackdel")
                pybbt.ASSERT_TRUE(length >= 1)  # At least 1 entry should remain
