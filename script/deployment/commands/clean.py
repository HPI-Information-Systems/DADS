import json
import logging
from typing import Dict, List, Tuple

import requests

from commands.base_worker import BaseWorker
from configuration import Configuration
from ssh_connection import SSHConnection


class CleaningWorker(BaseWorker):

    def __init__(self, config: Configuration, node_index: int, remove_deployment_keys: bool):
        super().__init__(config, node_index)
        self._remove_deployment_keys = remove_deployment_keys

    def _run(self, ssh: SSHConnection) -> None:
        logging.info(f"[{self._node}] Starting cleaning")

        self._delete_ssh_directory(ssh)
        self._delete_project_root(ssh)
        self._delete_deployment_keys()

    def _delete_ssh_directory(self, ssh: SSHConnection) -> None:
        logging.info(f"[{self._node}] Deleting ssh directory")

        ssh.execute(f"rm -rf ~/.ssh")

    def _delete_project_root(self, ssh: SSHConnection) -> None:
        logging.info(f"[{self._node}] Deleting project root")

        ssh.execute(f"rm -rf {self._config.project_root}")

    def _delete_deployment_keys(self):
        if not self._remove_deployment_keys:
            return

        logging.info(f"[{self._node}] Removing deployment keys")

        auth: Tuple[str, str] = (self._config.repository_user, self._config.repository_password)
        request = requests.get(
            f"https://api.github.com/repos/{self._config.repository_user}/{self._config.repository_name}/keys",
            auth=auth)
        if request.status_code != 200:
            logging.error(f"Unable to get all deployment keys: {request.text}")
            return

        response: List[Dict[str, object]] = json.loads(request.text)
        for key_response in response:
            request = requests.delete(
                f"https://api.github.com/repos/{self._config.repository_user}/{self._config.repository_name}/keys/{key_response['id']}",
                auth=auth)
            if request.status_code != 204:
                logging.error(f"Unable to delete deployment key {key_response['id']}: {request.text}")


def run(config: Configuration):
    remove_deployment_keys: bool = True
    for i, node in enumerate(config.nodes):
        worker: CleaningWorker = CleaningWorker(config, i, remove_deployment_keys)
        remove_deployment_keys = False
        worker.run()
