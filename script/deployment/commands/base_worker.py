from abc import ABC, abstractmethod

from configuration import Configuration
from ssh_connection import SSHConnection


class BaseWorker(ABC):

    def __init__(self, config: Configuration, node_index: int):
        self._config: Configuration = config
        self._node_index: int = node_index

    @property
    def _node(self) -> str:
        return self._config.nodes[self._node_index]

    def __call__(self, *args, **kwargs) -> None:
        self.run()

    def run(self) -> None:
        ssh: SSHConnection = SSHConnection(self._node,
                                           self._config.ssh_user,
                                           self._config.ssh_password)

        self._run(ssh)

        ssh.close()

    @abstractmethod
    def _run(self, ssh: SSHConnection) -> None:
        raise NotImplementedError
