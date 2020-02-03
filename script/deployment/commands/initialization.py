import logging

from commands.base_worker import BaseWorker
from configuration import Configuration
from ssh_connection import SSHConnection


class InitializationWorker(BaseWorker):

    def __init__(self, config: Configuration, node_index: int):
        super().__init__(config, node_index)

    def _run(self, ssh: SSHConnection) -> None:
        logging.info(f"[{self._node}] Starting initialization")

        self._create_project_structure(ssh)
        self._checkout_project(ssh)

        ssh.close()

    def _create_project_structure(self, ssh: SSHConnection) -> None:
        logging.info(f"[{self._node}] Creating project structure")

        ssh.execute(f"mkdir -p {self._config.implementation_dir}")
        ssh.execute(f"mkdir -p {self._config.result_dir}")
        ssh.execute(f"mkdir -p {self._config.data_dir}")

    def _checkout_project(self, ssh: SSHConnection) -> None:
        logging.info(f"[{self._node}] Checking out project")

        ssh.execute(f"git clone -b {self._config.repository_branch} {self._config.repository_url} {self._config.implementation_dir}")

    def _build_project(self, ssh: SSHConnection) -> None:
        logging.info(f"[{self._node}] Building project")

        


def run(config: Configuration):
    for i, node in enumerate(config.nodes):
        worker: InitializationWorker = InitializationWorker(config, i)
        worker.run()
