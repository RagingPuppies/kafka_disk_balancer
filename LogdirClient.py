from subprocess import PIPE, Popen
import os
import logging
import json
from shlex import split

logger = logging.getLogger(__name__)

class LogdirClient:
  def __init__(self, bootstrap_servers= 'localhost:9092', kafka_path='/opt/kafka-2.5.0/kafka/bin') -> None:
    self.bootstrap_servers = bootstrap_servers
    self.kafka_path = kafka_path

  def get_logdir_command(self, broker: str) -> str:

    broker_cmd = "" if not broker else "--broker-list {broker}".format(broker=broker)
    return "{path}/kafka-log-dirs.sh --bootstrap-server {servers} --describe {broker_cmd}".format(
      path=self.kafka_path, 
      servers=self.bootstrap_servers,
      broker_cmd=broker_cmd
      )

  def run_command_with_output(self, cmd: str) -> str:
    
    p = Popen(split(cmd), stdout=PIPE, stdin=PIPE, shell=False, stderr=PIPE, preexec_fn=os.setpgrp)
    output, _ = p.communicate(None)
    return_code = p.poll()

    if return_code != 0:
        raise Exception("{output}".format(output=output))

    return output.decode("utf-8")

  def get_kafka_logdirs(self, broker: str = "") -> object:

    logdir_cmd = self.get_logdir_command(broker)
    result = self.run_command_with_output(logdir_cmd)
    # Remove extra lines, convert to json object and drill down to brokers field.
    return json.loads(result.split("\n",2)[2])["brokers"]
    


if __name__ == "__main__":
  instance = LogdirClient()
  print(instance.get_kafka_logdirs('40001'))