import logging
from typing import List

from pexpect import pxssh


class SSHConnection:

    def __init__(self, hostname: str, username: str, password: str):
        self._hostname: str = hostname
        self._ssh_session: pxssh.pxssh = pxssh.pxssh()
        if not self._ssh_session.login(hostname, username, password):
            raise Exception(f"[{hostname}] FAILED SSH CONNECTION")

        self.execute("sh")

    def execute(self, line: str) -> List[str]:
        self._ssh_session.sendline(line)
        self._ssh_session.prompt()
        response: List[str] = self._ssh_session.before.decode("utf-8").split("\r\n")[1:]
        response_as_string: str = "\n".join(response)
        logging.debug(f"[{self._hostname}] Executing: {line}\n{response_as_string}")

        return response

    def close(self):
        self.execute("exit")
        self._ssh_session.close()
