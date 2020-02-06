import json
from pathlib import Path
from typing import Dict, Any, List, Optional


class DeploymentArtifact:

    def __init__(self):
        self._source: str = ""
        self._target: str = ""
        self._nodes: Optional[List[str]] = None

    @property
    def source(self) -> str:
        return self._source

    @property
    def target(self) -> str:
        return self._target

    @property
    def deploy_only_on_specific_nodes(self) -> bool:
        return self._nodes is not None

    @property
    def deployment_nodes(self) -> List[str]:
        if not self.deploy_only_on_specific_nodes:
            return []

        return self._nodes

    @staticmethod
    def from_json(json_dict: Dict[str, Any]) -> "DeploymentArtifact":
        artifact: DeploymentArtifact = DeploymentArtifact()
        artifact._source = json_dict["source"]
        artifact._target = json_dict["target"]

        if "nodes" in json_dict:
            artifact._nodes = json_dict["nodes"]

        return artifact


class Experiment:

    def __init__(self):
        self._id: str = ""
        self._nodes: Optional[List[str]] = None
        self._parameters: Dict[str, Any] = {}

    @property
    def id(self) -> str:
        return self._id

    @property
    def perform_only_on_specific_nodes(self) -> bool:
        return self._nodes is not None

    @property
    def involved_nodes(self) -> List[str]:
        if not self.perform_only_on_specific_nodes:
            return []

        return self._nodes

    @property
    def parameters(self) -> Dict[str, Any]:
        return self._parameters

    @staticmethod
    def load(experiment_path: Path) -> "Experiment":
        json_dict: Dict[str, Any] = json.load(experiment_path.open("r"))

        return Experiment._from_json(json_dict)

    @staticmethod
    def _from_json(json_dict: Dict[str, Any]) -> "Experiment":
        experiment: Experiment = Experiment()
        experiment._id = json_dict["id"]
        experiment._parameters = json_dict["parameters"]

        if "nodes" in json_dict:
            experiment._nodes = json_dict["nodes"]

        return experiment


class Configuration:

    def __init__(self):
        self._ssh_user: str = ""
        self._nodes: List[str] = []
        self._remote_workspace: str = ""
        self._remote_locations: List[str] = []
        self._project_name: str = ""
        self._local_project: str = ""
        self._artifacts: List[DeploymentArtifact] = []
        self._master_command_template: str = ""
        self._slave_command_template: str = ""

    @property
    def ssh_user(self) -> str:
        return self._ssh_user

    @property
    def nodes(self) -> List[str]:
        return self._nodes

    @property
    def remote_workspace(self) -> str:
        return self._remote_workspace

    @property
    def remote_locations(self) -> List[str]:
        return [self.remote_workspace + location for location in self._remote_locations]

    @property
    def project_name(self) -> str:
        return self._project_name

    @property
    def local_project(self) -> str:
        return self._local_project

    @property
    def artifacts(self) -> List[DeploymentArtifact]:
        return self._artifacts

    @property
    def master_command_template(self) -> str:
        return self._master_command_template

    @property
    def slave_command_template(self) -> str:
        return self._slave_command_template

    @staticmethod
    def load(config_path: Path) -> "Configuration":
        json_dict: Dict[str, Any] = json.load(config_path.open("r"))

        return Configuration._from_json(json_dict)

    @staticmethod
    def _from_json(json_dict: Dict[str, Any]) -> "Configuration":
        config: Configuration = Configuration()
        config._ssh_user = json_dict["ssh-user"]
        config._nodes = json_dict["nodes"]
        config._remote_workspace = json_dict["remote-workspace"]
        config._remote_locations = json_dict["remote-locations"]
        config._project_name = json_dict["project-name"]
        config._local_project = json_dict["local-project"]
        config._master_command_template = json_dict["master-command-template"]
        config._slave_command_template = json_dict["slave-command-template"]

        for json_artifact in json_dict["deploy-artifacts"]:
            config._artifacts.append(DeploymentArtifact.from_json(json_artifact))

        if not config.remote_workspace.startswith("~/"):
            config._remote_workspace = "~/" + config.remote_workspace

        return config
