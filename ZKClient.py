from kazoo.client import KazooClient
import logging
import json

logger = logging.getLogger(__name__)


class ZKClient:
  def __init__(self, zk_servers: list, timeout: int = 5) -> None:
    self.zk = KazooClient(hosts=zk_servers)
    self.zk.start(timeout=timeout)

  def get_replica_assignment(self, topic: str , partition: str) -> object:

    replica_assignment, _ = self.zk.get(f"/brokers/topics/{topic}")
    replica_assignment = json.loads(replica_assignment.decode("utf-8"))
    return replica_assignment["partitions"][partition]
