import logging
from multiprocessing import Process
from pathlib import Path
from typing import List

from configuration import Configuration
from ssh_connection import SSHConnection


def run(config_path: Path, ssh_password: str) -> None:
    config: Configuration = Configuration.load(config_path)
    workers: List[Process] = []

    for node in config.nodes:
        worker: Process = Process(target=_stop_project,
                                  args=(node, config.ssh_user, ssh_password, config.project_name),
                                  daemon=False)
        workers.append(worker)
        worker.start()

    for worker in workers:
        worker.join()


def _stop_project(ssh_host: str, ssh_user: str, ssh_password: str, project_name: str) -> None:
    logging.info(f"[{ssh_host}] Stopping project")

    ssh: SSHConnection = SSHConnection(ssh_host, ssh_user, ssh_password)

    ssh.execute(f"screen -X -S {project_name} quit")

    ssh.close()
