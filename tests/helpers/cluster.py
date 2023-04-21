import pybbt as p
import redis
from redis.cluster import ClusterNode

from helpers.redis import Redis


class Cluster:
    def __init__(self):
        self.num = 2
        self.nodes = []
        for i in range(self.num):
            self.nodes.append(Redis(args=["--cluster-enabled", "yes"]))
        p.ASSERT_EQ(self.nodes[0].do("cluster", "addslots", *range(0, 8192)), b"OK")
        p.ASSERT_EQ(self.nodes[1].do("cluster", "addslots", *range(8192, 16384)), b"OK")
        p.ASSERT_EQ(self.nodes[0].do("cluster", "meet", self.nodes[1].host, self.nodes[1].port), b"OK")
        p.ASSERT_EQ(self.nodes[1].do("cluster", "meet", self.nodes[0].host, self.nodes[0].port), b"OK")
        self.client = redis.RedisCluster(startup_nodes=[
            ClusterNode(self.nodes[0].host, self.nodes[0].port),
            ClusterNode(self.nodes[1].host, self.nodes[1].port)
        ], require_full_coverage=True)
        p.ASSERT_EQ_TIMEOUT(lambda: self.client.cluster_info()["cluster_state"], "ok", 10)
        p.log(f"cluster started at {self.nodes[0].get_address()}")
        p.log(self.client.cluster_nodes())

    def do(self, *args):
        try:
            ret = self.client.execute_command(*args)
        except redis.exceptions.ResponseError as e:
            return f"-{str(e)}"
        return ret

    def pipeline(self):
        return self.client.pipeline(transaction=False)

    def get_address(self):
        return self.nodes[0].get_address()

    def dbsize(self):
        size = 0
        for node in self.nodes:
            size += node.dbsize()
        return size

    @staticmethod
    def is_cluster():
        return True
