import re
from typing import Dict, Any, List

from configuration import Configuration, Experiment

VAR_PATTERN = re.compile(r"(?P<VarBlock>{{(?P<VarName>.+?)}})")


class ExperimentEnvironment:

    def __init__(self, config: Configuration, experiment: Experiment, node_index: int):
        self._config: Configuration = config
        self._experiment: Experiment = experiment
        self._node_index: int = node_index
        self._variables: Dict[str, Any] = {}
        self._initialize_variables()

    @property
    def nodes(self) -> List[str]:
        if self._experiment.perform_only_on_specific_nodes:
            return self._experiment.involved_nodes

        return self._config.nodes

    @property
    def node(self) -> str:
        return self._config.nodes[self._node_index]

    @property
    def master(self) -> str:
        return self.nodes[0]

    @property
    def slaves(self) -> List[str]:
        return self.nodes[1:]

    @property
    def command_template(self) -> str:
        if self._node_index == 0:
            return self._config.master_command_template
        else:
            return self._config.slave_command_template

    def _initialize_variables(self):
        self._variables["experiment"] = self._experiment.id
        self._variables["node"] = self.node
        self._variables["master"] = self.master
        self._variables["num-slaves"] = len(self.slaves)

        for name in self._experiment.parameters:
            self._variables[f"parameter:{name}"] = self._experiment.parameters[name]

    @property
    def command(self) -> str:
        command: str = self.command_template
        matches = VAR_PATTERN.findall(command)

        for block, name in matches:
            value: Any = self._variables[name]
            command = command.replace(block, str(value))

        return command
