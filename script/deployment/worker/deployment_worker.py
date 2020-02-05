import logging
import subprocess
from multiprocessing import Process
from pathlib import Path
from typing import List, Tuple, Optional

from configuration import Configuration, Experiment
from experiment_environment import ExperimentEnvironment
from ssh_connection import SSHConnection


class WorkerState:

    def __init__(self, config_path: Path, node_index: int, ssh_password: str, experiment_id: str, skip_list: List[str]):
        self.config: Configuration = Configuration.load(config_path)
        self._node_index: int = node_index
        self.node: str = self.config.nodes[node_index]
        self.ssh_password: str = ssh_password
        self.experiment, self.command = self._set_experiment_data(experiment_id)
        self.skip_list: List[str] = skip_list
        self.ssh: SSHConnection = SSHConnection(self.node,
                                                self.config.ssh_user,
                                                ssh_password)

    def _set_experiment_data(self, experiment_id: str) -> Tuple[Optional[Experiment], str]:
        filtered_experiments: List[Experiment] = list(filter(lambda x: x.id == experiment_id, self.config.experiments))
        if len(filtered_experiments) == 0:
            return None, ""

        experiment: Experiment = filtered_experiments[0]
        environment: ExperimentEnvironment = ExperimentEnvironment(self.config, experiment, self._node_index)

        return experiment, environment.command


def _worker(config_path: str, node_index: int, ssh_password: str, experiment_id: str, skip_list: List[str]) -> None:
    state: WorkerState = WorkerState(Path(config_path), node_index, ssh_password, experiment_id, skip_list)

    logging.info(f"[{state.node}] Initializing SSH connection")

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
        f"screen -d -m -S {state.experiment.id} bash -c 'cd {state.config.remote_workspace} && {state.command}'")


def run(skip_list: List[str], experiment_id: str, config_path: Path, ssh_password: str) -> None:
    config: Configuration = Configuration.load(config_path)

    _build_project(skip_list, config)
    _deploy_project(skip_list, experiment_id, config, config_path, ssh_password)


def _build_project(skip_list: List[str], config: Configuration) -> None:
    if "build" in skip_list:
        logging.info("Skipping build step")
        return

    logging.info("Building project")
    subprocess.run(f"mvn clean package -Dmaven.test.skip=true -q -f {config.local_project}", shell=True)


def _deploy_project(skip_list: List[str], experiment_id: str, config: Configuration, config_path: Path,
                    ssh_password: str) -> None:
    workers: List[Process] = []

    for node_index, _ in enumerate(config.nodes):
        worker: Process = Process(target=_worker,
                                  args=(
                                      str(config_path.absolute()), node_index, ssh_password, experiment_id, skip_list),
                                  daemon=False)
        workers.append(worker)
        worker.start()

    for worker in workers:
        worker.join()
