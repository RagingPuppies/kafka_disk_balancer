from kazoo.client import KazooClient
from subprocess import PIPE, Popen
import os
from shlex import split
import logging
import json

logger = logging.getLogger("ZKClient")


class ZKClient:
  def __init__(self, zk_servers: list, timeout = 5) -> None:
    self.zk = KazooClient(hosts=zk_servers)
    self.zk.start(timeout=timeout)

  def get_replica_assignment(self, topic, partition):
    replica_assignment, _ = self.zk.get(f"/brokers/topics/{topic}")
    replica_assignment = json.loads(replica_assignment.decode("utf-8"))
    return replica_assignment["partitions"][partition]

def get_kafka_logdirs(cmd: str, stdin: object = None) -> object:

    p = Popen(split(cmd), stdout=PIPE, stdin=PIPE, shell=False, stderr=PIPE, preexec_fn=os.setpgrp,
              encoding="utf-8")

    output, error = p.communicate(stdin)
    return_code = p.poll()

    if return_code != 0:
        raise Exception(f"{output}")
    return output
