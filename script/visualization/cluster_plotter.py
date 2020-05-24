from datetime import datetime
from typing import Tuple, List, Any, Dict, Set

import pandas as pd
import plotly
import plotly.graph_objects as go
from plotly.subplots import make_subplots

from event import CalculationCompletedEvent, DurationEvent, MachineUtilizationEvent, MeasurementEvent
from event.base import StatisticsEvent
from event.duration import *


class ClusterPlotter:

    @staticmethod
    def _create_figure(include_cpu: bool = True, include_network: bool = True) -> plotly.subplots:
        rows: int = 1
        columns: int = (1 if include_cpu else 0) + (1 if include_network else 0)
        assert columns > 0

        titles: Tuple[str, ...] = ()
        spacing: float = 0.05 if columns > 1 else 0.0
        specs: List[List[Dict[str, Any]]] = [[]]

        if include_cpu:
            titles += ("CPU & Memory Utilization",)
            specs[0].append({"secondary_y": False})

        if include_network:
            titles += ("Data Transferred",)
            specs[0].append({"secondary_y": True})

        return make_subplots(rows=rows, cols=columns,
                             subplot_titles=titles,
                             horizontal_spacing=spacing,
                             specs=specs)

    @staticmethod
    def _process_step_event_types() -> List[str]:
        return [ProjectionCreatedEvent.event_identifier(),
                PCACreatedEvent.event_identifier(),
                DimensionReductionCreatedEvent.event_identifier(),
                IntersectionsCreatedEvent.event_identifier(),
                NodesExtractedEvent.event_identifier(),
                EdgePartitionCreatedEvent.event_identifier(),
                PathScoresCreatedEvent.event_identifier(),
                PathScoresNormalizedEvent.event_identifier(),
                ResultsPersistedEvent.event_identifier()]

    def __init__(self,
                 events: Dict[str, Dict[str, List[StatisticsEvent]]],
                 include_cpu: bool = True,
                 include_network: bool = False):
        self._events: Dict[str, Dict[str, List[StatisticsEvent]]] = events
        self._start_time, self._end_time = self._process_start_and_end_time()
        self._process_duration: float = (self._end_time - self._start_time).total_seconds()
        self._processor_max_memory: Dict[str, int] = self._extract_processor_max_memory()
        self._x_axis_range: List[float] = [0.0, self._process_duration]
        self._figure: plotly.subplots = self._create_figure(include_cpu, include_network)
        self._process_steps: List[Dict[str, Any]] = []
        self._sample_interval: float = 0.05  # 50 ms

    def _process_start_and_end_time(self) -> Tuple[datetime, datetime]:
        return self._start_and_end_time(self._duration_events_of_type(CalculationCompletedEvent.event_identifier()))

    def _extract_processor_max_memory(self) -> Dict[str, int]:
        results: Dict[str, int] = {}

        for processor in self._events:
            results[processor] = self._events[processor][MachineUtilizationEvent.event_identifier()][0].total_memory

        return results

    def _duration_events_of_type(self, event_identifier: str) -> List[DurationEvent]:
        results: List[DurationEvent] = []
        for processor in self._events:
            if event_identifier not in self._events[processor]:
                continue

            results += self._events[processor][event_identifier]

        return results

    def _measurement_events_of_type(self, event_identifier: str) -> Dict[str, List[MeasurementEvent]]:
        results: Dict[str, List[MeasurementEvent]] = {}

        for processor in self._events:
            if event_identifier not in self._events[processor]:
                continue

            results[processor] = self._events[processor][event_identifier]

        return results

    @staticmethod
    def _start_and_end_time(events: List[DurationEvent]) -> Tuple[datetime, datetime]:
        earliest_start: datetime = min((e.start_time for e in events))
        latest_end: datetime = max((e.end_time for e in events))
        return earliest_start, latest_end

    def _relative_start_and_end_time(self, events: List[DurationEvent]) -> Tuple[float, float]:
        start, end = self._start_and_end_time(events)
        return (start - self._start_time).total_seconds(), (end - self._start_time).total_seconds()

    def show(self) -> None:
        # self._extract_process_steps()
        # self._extract_cpu_and_memory_utilization()
        self._extract_data_transfers()

        # self._figure.show()

    def _extract_process_steps(self) -> None:
        data: pd.DataFrame = pd.DataFrame({"step-name": [],
                                           "start-time": [],
                                           "end-time": [],
                                           "abs-duration": [],
                                           "rel-duration": []})

        for step_name in self._process_step_event_types():
            events: List[StatisticsEvent] = self._duration_events_of_type(step_name)
            if not events:
                continue

            start, end = self._relative_start_and_end_time(events)
            absolute_duration: float = end - start
            relative_duration: float = absolute_duration / self._process_duration
            data = data.append({"step-name": step_name,
                                "start-time": start,
                                "end-time": end,
                                "abs-duration": absolute_duration,
                                "rel-duration": relative_duration}, ignore_index=True)

            self._process_steps.append({
                "type": "rect",
                "xref": "x",
                "yref": "paper",
                "x0": start,
                "x1": end,
                "y0": 0.0,
                "y1": 1.0,
                "opacity": 0.2,
                "layer": "below",
                "line_width": 0,
                "fillcolor": events[0].fill_color,
            })

        data.to_csv("process-steps.csv", sep=";", index_label="index")

    def _extract_cpu_and_memory_utilization(self) -> None:
        data: pd.DataFrame = pd.DataFrame({"time": [],
                                           "avg-cpu": [],
                                           "min-cpu": [],
                                           "max-cpu": [],
                                           "tot-rel-mem": [],
                                           "tot-abs-mem": [],
                                           "avg-rel-mem": [],
                                           "avg-abs-mem": [],
                                           "min-rel-mem": [],
                                           "min-abs-mem": [],
                                           "max-rel-mem": [],
                                           "max-abs-mem": []})

        events: Dict[str, List[MachineUtilizationEvent]] = self._measurement_events_of_type(MachineUtilizationEvent.event_identifier())
        event_indices: Dict[str, int] = dict([(p, 0) for p in events])
        involved_processors: int = len(events)
        total_cluster_memory: int = sum((self._processor_max_memory[p] for p in events))
        average_processor_memory: float = total_cluster_memory / involved_processors

        sample_time: float = 0.0
        while sample_time < self._process_duration:
            avg_cpu: float = 0.0
            min_cpu: float = 1.0
            max_cpu: float = 0.0
            tot_mem: int = 0
            avg_abs_mem: int = 0
            avg_rel_mem: float = 0.0
            min_abs_mem: int = total_cluster_memory
            min_rel_mem: float = 1.0
            max_abs_mem: int = 0
            max_rel_mem: float = 0.0

            for processor in events:
                index: int = event_indices[processor]
                while index < len(events[processor]) and (events[processor][index].date_time - self._start_time).total_seconds() <= sample_time:
                    index += 1

                event_a: MachineUtilizationEvent = events[processor][max(0, index - 1)]
                event_b: MachineUtilizationEvent = events[processor][min(len(events[processor]) - 1, index)]

                event_indices[processor] = index

                factor_a, factor_b = self._interpolation_ratios(event_a, event_b, sample_time)

                cpu: float = event_a.cpu_ratio * factor_a + event_b.cpu_ratio * factor_b
                mem: int = int(event_a.used_memory * factor_a + event_b.used_memory * factor_b)
                mem_ratio: float = event_a.used_memory_ratio * factor_a + event_b.used_memory_ratio * factor_b

                avg_cpu += cpu / involved_processors
                min_cpu = min(min_cpu, cpu)
                max_cpu = max(max_cpu, cpu)
                tot_mem += mem
                avg_abs_mem += mem / involved_processors
                avg_rel_mem += mem_ratio / involved_processors
                min_abs_mem = min(min_abs_mem, mem)
                min_rel_mem = min(min_rel_mem, mem_ratio)
                max_abs_mem = max(max_abs_mem, mem)
                max_rel_mem = max(max_rel_mem, mem_ratio)

            data = data.append({"time": sample_time,
                                "avg-cpu": avg_cpu,
                                "min-cpu": min_cpu,
                                "max-cpu": max_cpu,
                                "tot-rel-mem": tot_mem / total_cluster_memory,
                                "tot-abs-mem": tot_mem,
                                "avg-rel-mem": avg_abs_mem / average_processor_memory,
                                "avg-abs-mem": avg_abs_mem,
                                "min-rel-mem": min_rel_mem,
                                "min-abs-mem": min_abs_mem,
                                "max-rel-mem": max_rel_mem,
                                "max-abs-mem": max_abs_mem}, ignore_index=True)

            sample_time += self._sample_interval

        data.to_csv("cluster-utilization.csv", sep=";", index_label="index")

        # Memory
        self._figure.add_trace(go.Scatter(
            x=data["time"],
            y=data["avg-rel-mem"],
            mode="lines",
            showlegend=True,
            name="Memory",
            line_width=2,
        ), row=1, col=1)

        # CPU (Avg.)
        self._figure.add_trace(go.Scatter(
            x=data["time"],
            y=data["avg-cpu"],
            mode="lines",
            showlegend=True,
            name="CPU (avg.)",
            line_width=2,
        ), row=1, col=1)

        # CPU (Max.)
        self._figure.add_trace(go.Scatter(
            x=data["time"],
            y=data["max-cpu"],
            mode="lines",
            showlegend=True,
            name="CPU (max.)",
            line_width=2,
        ), row=1, col=1)

        self._add_process_shapes(1, 1)

    def _interpolation_ratios(self, event_a: MeasurementEvent, event_b: MeasurementEvent, sample_time: float) -> Tuple[float, float]:
        if event_a.date_time == event_b.date_time:
            return 1.0, 0

        relative_time_a: float = (event_a.date_time - self._start_time).total_seconds()
        relative_time_b: float = (event_b.date_time - self._start_time).total_seconds()

        interval: float = relative_time_b - relative_time_a
        factor_a: float = (sample_time - relative_time_a) / interval

        return factor_a, 1 - factor_a

    def _extract_data_transfers(self) -> None:
        data: pd.DataFrame = pd.DataFrame({"transfer-name": [],
                                           "start-time": [],
                                           "end-time": [],
                                           "abs-duration": [],
                                           "rel-duration": [],
                                           "abs-data": [],
                                           "rel-data": [],
                                           "avg-transfer-rate": [],
                                           "max-abs-data": [],
                                           "max-rel-data": [],
                                           "min-abs-data": [],
                                           "min-rel-data": [],
                                           "num-processors": []})

        events: List[DataTransferredEvent] = self._duration_events_of_type(DataTransferredEvent.event_identifier())
        data_transfer_names: Set[str] = set(e.simplified_name for e in events)

        total_bytes: int = 0
        for transfer_name in data_transfer_names:
            transfer_events: List[DataTransferredEvent] = [e for e in events
                                                           if e.simplified_name == transfer_name]

            start, end = self._relative_start_and_end_time(transfer_events)
            duration: float = end - start
            processors: Set[str] = set()
            for event in transfer_events:
                if event.sender == event.receiver:
                    continue

                processors.add(event.receiver)
                processors.add(event.sender)
            number_involved_processors: int = len(processors)

            received_bytes: int = 0
            max_bytes: int = 0
            min_bytes: int = float("inf")
            for processor in processors:
                processor_bytes: int = 0
                for event in transfer_events:
                    if event.processor != processor:
                        continue

                    if event.receiver != processor:
                        continue

                    if event.sender == event.receiver:
                        continue

                    processor_bytes += event.transferred_bytes

                received_bytes += processor_bytes
                total_bytes += processor_bytes
                max_bytes = max(max_bytes, processor_bytes)
                min_bytes = min(min_bytes, processor_bytes)

            if received_bytes == 0:
                continue

            data = data.append({"transfer-name": transfer_name,
                                "start-time": start,
                                "end-time": end,
                                "abs-duration": duration,
                                "rel-duration": duration / self._process_duration,
                                "abs-data": received_bytes,
                                "avg-transfer-rate": received_bytes / duration,
                                "max-abs-data": max_bytes,
                                "max-rel-data": max_bytes / received_bytes,
                                "min-abs-data": min_bytes,
                                "min-rel-data": min_bytes / received_bytes,
                                "num-processors": number_involved_processors}, ignore_index=True)

        for i in range(data.shape[0]):
            data.at[i, "rel-data"] = data.at[i, "abs-data"] / total_bytes if total_bytes != 0 else 0.0

        data = data.sort_values(by="start-time")
        data.to_csv("network-utilization.csv", sep=";", index_label="index")

    def _add_process_shapes(self, row: int, column: int) -> None:
        for shape in self._process_steps:
            self._figure.add_shape(shape, row=row, col=column)
            self._figure.layout.shapes[-1].update({"yref": "paper"})
