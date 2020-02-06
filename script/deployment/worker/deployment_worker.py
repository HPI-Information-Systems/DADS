import logging
import subprocess
from multiprocessing import Process
from pathlib import Path
from typing import List

from configuration import Configuration, Experiment
from experiment_environment import ExperimentEnvironment
from ssh_connection import SSHConnection


class WorkerState:

    def __init__(self, config_path: Path, experiment_path: Path, skip_list: List[str], ssh_password: str,
                 node_index: int):
        self.config: Configuration = Configuration.load(config_path)
        self.experiment: Experiment = Experiment.load(experiment_path)
        self._node_index: int = node_index
        self.node: str = self.config.nodes[node_index]
        self.skip_list: List[str] = skip_list
        self.ssh_password: str = ssh_password
        self.command: str = ExperimentEnvironment(self.config, self.experiment, node_index).command

        self.ssh: SSHConnection = SSHConnection(self.node,
                                                self.config.ssh_user,
                                                ssh_password)


def _worker(config_path: str, experiment_path: str, skip_list: List[str], ssh_password: str, node_index: int) -> None:
    state: WorkerState = WorkerState(Path(config_path), Path(experiment_path), skip_list, ssh_password, node_index)

    logging.info(f"[{state.node}] Initializing SSH connection")

    _stop_project(state)

    if "deploy" not in state.skip_list:
        logging.info(f"[{state.node}] Deploying project")
        _create_remote_workspace(state)
        _clean_remote_work_space(state)
        _create_remote_locations(state)
        _deploy_artifacts(state)
    else:
        logging.info(f"[{state.node}] Skipping deploy step")

    _run_experiment(state)

    state.ssh.close()


def _stop_project(state: WorkerState) -> None:
    logging.info(f"[{state.node}] Stopping project")

    state.ssh.execute(f"screen -X -S {state.config.project_name} quit")


def _create_remote_workspace(state: WorkerState) -> None:
    logging.info(f"[{state.node}] Creating workspace")

    state.ssh.execute(f"mkdir -p {state.config.remote_workspace}")


def _clean_remote_work_space(state: WorkerState) -> None:
    logging.info(f"[{state.node}] Cleaning workspace")

    state.ssh.execute(f"rm -rf {state.config.remote_workspace}/*")


def _create_remote_locations(state: WorkerState) -> None:
    logging.info(f"[{state.node}] Creating locations")

    for location in state.config.remote_locations:
        logging.info(f"[{state.node}]    {location}")
        state.ssh.execute(f"mkdir -p {location}")


def _deploy_artifacts(state: WorkerState) -> None:
    logging.info(f"[{state.node}] Deploying artifacts...")

    for artifact in state.config.artifacts:
        if artifact.deploy_only_on_specific_nodes and state.node not in artifact.deployment_nodes:
            continue

        logging.info(f"[{state.node}]    {artifact.source} -> {artifact.target}")
        subprocess.run(
            f"sshpass -p'{state.ssh_password}' scp -q {artifact.source} {state.config.ssh_user}@{state.node}:{artifact.target}",
            shell=True)


def _run_experiment(state: WorkerState) -> None:
    if state.experiment is None:
        return

    logging.info(f"[{state.node}] Starting experiment")

    state.ssh.execute(
        f"screen -d -m -S {state.config.project_name}-{state.experiment.id} bash -c 'cd {state.config.remote_workspace} && {state.command}'")


def run(config_path: Path, experiment_path: Path, skip_list: List[str], ssh_password: str) -> None:
    config: Configuration = Configuration.load(config_path)

    _build_project(config, skip_list)
    _deploy_project(config_path, experiment_path, skip_list, ssh_password)


def _build_project(config: Configuration, skip_list: List[str]) -> None:
    if "build" in skip_list:
        logging.info("Skipping build step")
        return

    skip_tests: str = ""
    if "tests" in skip_list:
        skip_tests = "-Dmaven.test.skip=true "

    logging.info("Building project")
    subprocess.run(f"mvn clean package -q {skip_tests} -f {config.local_project}", shell=True)


def _deploy_project(config_path: Path, experiment_path: Path, skip_list: List[str], ssh_password: str) -> None:
    config: Configuration = Configuration.load(config_path)
    workers: List[Process] = []

    for node_index, _ in enumerate(config.nodes):
        worker: Process = Process(target=_worker,
                                  args=(
                                      str(config_path.absolute()), str(experiment_path.absolute()), skip_list,
                                      ssh_password, node_index),
                                  daemon=False)
        workers.append(worker)
        worker.start()

    for worker in workers:
        worker.join()
