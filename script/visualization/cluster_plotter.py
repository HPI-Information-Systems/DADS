from datetime import datetime
from typing import Tuple, List, Any, Dict, Set

import pandas as pd
import plotly
import plotly.graph_objects as go
from plotly.subplots import make_subplots

from event import CalculationCompletedEvent, DurationEvent, MachineUtilizationEvent, MeasurementEvent
from event.base import StatisticsEvent
from event.duration import *

PROCESS_STEP_NAME: str = "step_name"
PROCESS_STEP_START: str = "start_time"
PROCESS_STEP_END: str = "end_time"
PROCESS_STEP_ABS_DUR: str = "abs_duration"
PROCESS_STEP_REL_DUR: str = "rel_duration"

MACHINE_UTILIZATION_TIME: str = "time"
MACHINE_UTILIZATION_AVG_CPU: str = "avg_cpu"
MACHINE_UTILIZATION_MIN_CPU: str = "min_cpu"
MACHINE_UTILIZATION_MAX_CPU: str = "max_cpu"
MACHINE_UTILIZATION_TOT_REL_MEM: str = "tot_rel_mem"
MACHINE_UTILIZATION_TOT_ABS_MEM: str = "tot_abs_mem"
MACHINE_UTILIZATION_AVG_REL_MEM: str = "avg_rel_mem"
MACHINE_UTILIZATION_AVG_ABS_MEM: str = "avg_abs_mem"
MACHINE_UTILIZATION_MIN_REL_MEM: str = "min_rel_mem"
MACHINE_UTILIZATION_MIN_ABS_MEM: str = "min_abs_mem"
MACHINE_UTILIZATION_MAX_REL_MEM: str = "max_rel_mem"
MACHINE_UTILIZATION_MAX_ABS_MEM: str = "max_abs_mem"

DATA_TRANSFER_NAME: str = "transfer_name"
DATA_TRANSFER_START: str = "start_time"
DATA_TRANSFER_END: str = "end_time"
DATA_TRANSFER_ABS_DUR: str = "abs_duration"
DATA_TRANSFER_REL_DUR: str = "rel_duration"
DATA_TRANSFER_ABS_VOL: str = "abs_vol"
DATA_TRANSFER_REL_VOL: str = "rel_vol"
DATA_TRANSFER_AVG_RATE: str = "avg_rate"
DATA_TRANSFER_MAX_REL_VOL: str = "max_rel_vol"
DATA_TRANSFER_MAX_ABS_VOL: str = "max_abs_vol"
DATA_TRANSFER_MIN_REL_VOL: str = "min_rel_vol"
DATA_TRANSFER_MIN_ABS_VOL: str = "min_abs_vol"
DATA_TRANSFER_NUM_PROC: str = "num_processors"

STEP_STATS_SUM_ABS_DURATION: str = "sum_abs_duration"
STEP_STATS_SUM_REL_DURATION: str = "sum_rel_duration"
STEP_STATS_AVG_CPU: str = "avg_cpu"
STEP_STATS_AVG_ABS_MEM: str = "avg_abs_mem"
STEP_STATS_AVG_REL_MEM: str = "avg_rel_mem"
STEP_STATS_MAX_ABS_MEM: str = "max_abs_mem"
STEP_STATS_MAX_REL_MEM: str = "max_rel_mem"
STEP_STATS_AVG_ABS_IDLE_TIME: str = "avg_abs_idle_time"
STEP_STATS_AVG_REL_IDLE_TIME: str = "avg_rel_idle_time"
STEP_STATS_TOT_ABS_IDLE_TIME: str = "tot_abs_idle_time"
STEP_STATS_TOT_REL_IDLE_TIME: str = "tot_rel_idle_time"
STEP_STATS_MAX_ABS_IDLE_TIME: str = "max_abs_idle_time"
STEP_STATS_MAX_REL_IDLE_TIME: str = "max_rel_idle_time"
STEP_STATS_TOT_ABS_BUSY_TIME: str = "tot_abs_busy_time"
STEP_STATS_TOT_REL_BUSY_TIME: str = "tot_rel_busy_time"
STEP_STATS_AVG_ABS_BUSY_TIME: str = "avg_abs_busy_time"
STEP_STATS_AVG_REL_BUSY_TIME: str = "avg_rel_busy_time"
STEP_STATS_MAX_ABS_BUSY_TIME: str = "max_abs_busy_time"
STEP_STATS_MAX_REL_BUSY_TIME: str = "max_rel_busy_time"

PROCESSOR_STEP_STATS_PROC_NAME: str = "processor"
PROCESSOR_STEP_STATS_TOT_ABS_IDLE_TIME: str = "tot_abs_idle_time"
PROCESSOR_STEP_STATS_TOT_REL_IDLE_TIME: str = "tot_rel_idle_time"
PROCESSOR_STEP_STATS_TOT_ABS_BUSY_TIME: str = "tot_abs_busy_time"
PROCESSOR_STEP_STATS_TOT_REL_BUSY_TIME: str = "tot_rel_busy_time"
PROCESSOR_STEP_STATS_MAX_ABS_MEM: str = "max_abs_mem"
PROCESSOR_STEP_STATS_MAX_REL_MEM: str = "max_rel_mem"


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
        phases: pd.DataFrame = self._extract_process_steps()
        machine_utilization: pd.DataFrame = self._extract_cpu_and_memory_utilization()
        data_transfers: pd.DataFrame = self._extract_data_transfers()

        self._extract_process_step_statistics(phases, machine_utilization, data_transfers)

        # self._figure.show()

    def _extract_process_steps(self) -> pd.DataFrame:
        data: pd.DataFrame = pd.DataFrame({PROCESS_STEP_NAME: [],
                                           PROCESS_STEP_START: [],
                                           PROCESS_STEP_END: [],
                                           PROCESS_STEP_ABS_DUR: [],
                                           PROCESS_STEP_REL_DUR: []})

        for step_name in self._process_step_event_types():
            events: List[StatisticsEvent] = self._duration_events_of_type(step_name)
            if not events:
                continue

            start, end = self._relative_start_and_end_time(events)
            absolute_duration: float = end - start
            relative_duration: float = absolute_duration / self._process_duration
            data = data.append({PROCESS_STEP_NAME: step_name,
                                PROCESS_STEP_START: start,
                                PROCESS_STEP_END: end,
                                PROCESS_STEP_ABS_DUR: absolute_duration,
                                PROCESS_STEP_REL_DUR: relative_duration}, ignore_index=True)

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
        return data

    def _extract_cpu_and_memory_utilization(self) -> pd.DataFrame:
        data: pd.DataFrame = pd.DataFrame({MACHINE_UTILIZATION_TIME: [],
                                           MACHINE_UTILIZATION_AVG_CPU: [],
                                           MACHINE_UTILIZATION_MIN_CPU: [],
                                           MACHINE_UTILIZATION_MAX_CPU: [],
                                           MACHINE_UTILIZATION_TOT_REL_MEM: [],
                                           MACHINE_UTILIZATION_TOT_ABS_MEM: [],
                                           MACHINE_UTILIZATION_AVG_REL_MEM: [],
                                           MACHINE_UTILIZATION_AVG_ABS_MEM: [],
                                           MACHINE_UTILIZATION_MIN_REL_MEM: [],
                                           MACHINE_UTILIZATION_MIN_ABS_MEM: [],
                                           MACHINE_UTILIZATION_MAX_REL_MEM: [],
                                           MACHINE_UTILIZATION_MAX_ABS_MEM: []})

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

            data = data.append({MACHINE_UTILIZATION_TIME: sample_time,
                                MACHINE_UTILIZATION_AVG_CPU: avg_cpu,
                                MACHINE_UTILIZATION_MIN_CPU: min_cpu,
                                MACHINE_UTILIZATION_MAX_CPU: max_cpu,
                                MACHINE_UTILIZATION_TOT_REL_MEM: tot_mem / total_cluster_memory,
                                MACHINE_UTILIZATION_TOT_ABS_MEM: tot_mem,
                                MACHINE_UTILIZATION_AVG_REL_MEM: avg_abs_mem / average_processor_memory,
                                MACHINE_UTILIZATION_AVG_ABS_MEM: avg_abs_mem,
                                MACHINE_UTILIZATION_MIN_REL_MEM: min_rel_mem,
                                MACHINE_UTILIZATION_MIN_ABS_MEM: min_abs_mem,
                                MACHINE_UTILIZATION_MAX_REL_MEM: max_rel_mem,
                                MACHINE_UTILIZATION_MAX_ABS_MEM: max_abs_mem}, ignore_index=True)

            sample_time += self._sample_interval

        data.to_csv("cluster-utilization.csv", sep=";", index_label="index")

        # Memory
        self._figure.add_trace(go.Scatter(
            x=data[MACHINE_UTILIZATION_TIME],
            y=data[MACHINE_UTILIZATION_AVG_REL_MEM],
            mode="lines",
            showlegend=True,
            name="Memory",
            line_width=2,
        ), row=1, col=1)

        # CPU (Avg.)
        self._figure.add_trace(go.Scatter(
            x=data[MACHINE_UTILIZATION_TIME],
            y=data[MACHINE_UTILIZATION_AVG_CPU],
            mode="lines",
            showlegend=True,
            name="CPU (avg.)",
            line_width=2,
        ), row=1, col=1)

        # CPU (Max.)
        self._figure.add_trace(go.Scatter(
            x=data[MACHINE_UTILIZATION_TIME],
            y=data[MACHINE_UTILIZATION_MAX_CPU],
            mode="lines",
            showlegend=True,
            name="CPU (max.)",
            line_width=2,
        ), row=1, col=1)

        self._add_process_shapes(1, 1)
        return data

    def _interpolation_ratios(self, event_a: MeasurementEvent, event_b: MeasurementEvent, sample_time: float) -> Tuple[float, float]:
        if event_a.date_time == event_b.date_time:
            return 1.0, 0

        relative_time_a: float = (event_a.date_time - self._start_time).total_seconds()
        relative_time_b: float = (event_b.date_time - self._start_time).total_seconds()

        interval: float = relative_time_b - relative_time_a
        factor_a: float = (sample_time - relative_time_a) / interval

        return factor_a, 1 - factor_a

    def _extract_data_transfers(self) -> pd.DataFrame:
        data: pd.DataFrame = pd.DataFrame({DATA_TRANSFER_NAME: [],
                                           DATA_TRANSFER_START: [],
                                           DATA_TRANSFER_END: [],
                                           DATA_TRANSFER_ABS_DUR: [],
                                           DATA_TRANSFER_REL_DUR: [],
                                           DATA_TRANSFER_ABS_VOL: [],
                                           DATA_TRANSFER_REL_VOL: [],
                                           DATA_TRANSFER_AVG_RATE: [],
                                           DATA_TRANSFER_MAX_REL_VOL: [],
                                           DATA_TRANSFER_MAX_ABS_VOL: [],
                                           DATA_TRANSFER_MIN_REL_VOL: [],
                                           DATA_TRANSFER_MIN_ABS_VOL: [],
                                           DATA_TRANSFER_NUM_PROC: []})

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

            data = data.append({DATA_TRANSFER_NAME: transfer_name,
                                DATA_TRANSFER_START: start,
                                DATA_TRANSFER_END: end,
                                DATA_TRANSFER_ABS_DUR: duration,
                                DATA_TRANSFER_REL_DUR: duration / self._process_duration,
                                DATA_TRANSFER_ABS_VOL: received_bytes,
                                DATA_TRANSFER_AVG_RATE: received_bytes / duration,
                                DATA_TRANSFER_MAX_ABS_VOL: max_bytes,
                                DATA_TRANSFER_MAX_REL_VOL: max_bytes / received_bytes,
                                DATA_TRANSFER_MIN_ABS_VOL: min_bytes,
                                DATA_TRANSFER_MIN_REL_VOL: min_bytes / received_bytes,
                                DATA_TRANSFER_NUM_PROC: number_involved_processors}, ignore_index=True)

        for i in range(data.shape[0]):
            data.at[i, DATA_TRANSFER_REL_VOL] = data.at[i, DATA_TRANSFER_ABS_VOL] / total_bytes if total_bytes != 0 else 0.0

        data = data.sort_values(by=DATA_TRANSFER_START)
        data.to_csv("network-utilization.csv", sep=";", index_label="index")
        return data

    def _extract_process_step_statistics(self, process_steps: pd.DataFrame, machine_utilization: pd.DataFrame, data_transfers: pd.DataFrame) -> pd.DataFrame:
        data: pd.DataFrame = pd.DataFrame({PROCESS_STEP_NAME: [],
                                           PROCESS_STEP_START: [],
                                           PROCESS_STEP_END: [],
                                           PROCESS_STEP_ABS_DUR: [],
                                           PROCESS_STEP_REL_DUR: [],
                                           STEP_STATS_SUM_ABS_DURATION: [],
                                           STEP_STATS_SUM_REL_DURATION: [],
                                           STEP_STATS_AVG_CPU: [],
                                           STEP_STATS_AVG_ABS_MEM: [],
                                           STEP_STATS_AVG_REL_MEM: [],
                                           STEP_STATS_MAX_ABS_MEM: [],
                                           STEP_STATS_MAX_REL_MEM: [],
                                           STEP_STATS_TOT_ABS_IDLE_TIME: [],
                                           STEP_STATS_TOT_REL_IDLE_TIME: [],
                                           STEP_STATS_AVG_ABS_IDLE_TIME: [],
                                           STEP_STATS_AVG_REL_IDLE_TIME: [],
                                           STEP_STATS_MAX_ABS_IDLE_TIME: [],
                                           STEP_STATS_MAX_REL_IDLE_TIME: [],
                                           STEP_STATS_TOT_ABS_BUSY_TIME: [],
                                           STEP_STATS_TOT_REL_BUSY_TIME: [],
                                           STEP_STATS_AVG_ABS_BUSY_TIME: [],
                                           STEP_STATS_AVG_REL_BUSY_TIME: [],
                                           STEP_STATS_MAX_ABS_BUSY_TIME: [],
                                           STEP_STATS_MAX_REL_BUSY_TIME: []})

        processor_step_stats: pd.DataFrame = pd.DataFrame({PROCESS_STEP_NAME: [],
                                                           PROCESS_STEP_START: [],
                                                           PROCESS_STEP_END: [],
                                                           PROCESS_STEP_ABS_DUR: [],
                                                           PROCESS_STEP_REL_DUR: [],
                                                           PROCESSOR_STEP_STATS_PROC_NAME: [],
                                                           PROCESSOR_STEP_STATS_TOT_ABS_IDLE_TIME: [],
                                                           PROCESSOR_STEP_STATS_TOT_REL_IDLE_TIME: [],
                                                           PROCESSOR_STEP_STATS_TOT_ABS_BUSY_TIME: [],
                                                           PROCESSOR_STEP_STATS_TOT_REL_BUSY_TIME: [],
                                                           PROCESSOR_STEP_STATS_MAX_ABS_MEM: [],
                                                           PROCESSOR_STEP_STATS_MAX_REL_MEM: []})

        num_processors: int = len(self._events)
        for i in range(process_steps.shape[0]):
            step_name: str = process_steps.at[i, PROCESS_STEP_NAME]
            start: float = process_steps.at[i, PROCESS_STEP_START]
            end: float = process_steps.at[i, PROCESS_STEP_END]
            duration: float = process_steps.at[i, PROCESS_STEP_ABS_DUR]
            summed_duration: float = duration * num_processors

            step_machine_utilization: pd.DataFrame = machine_utilization.query(f"{MACHINE_UTILIZATION_TIME} >= {start} & {MACHINE_UTILIZATION_TIME} <= {end}")
            # step_data_transfer: pd.DataFrame = data_transfers.query(f"{DATA_TRANSFER_START} <= {end} & {DATA_TRANSFER_END} >= {start}")

            machine_avg: pd.DataFrame = step_machine_utilization.mean()
            machine_max: pd.DataFrame = step_machine_utilization.max()

            processor_step_stats = self._processor_stats_in(process_steps.iloc[i], processor_step_stats)
            p_stats: pd.DataFrame = processor_step_stats.query(f"{PROCESS_STEP_NAME} == '{step_name}'")
            p_stats_sum: pd.DataFrame = p_stats.sum()
            p_stats_max: pd.DataFrame = p_stats.max()

            data = data.append({PROCESS_STEP_NAME: step_name,
                                PROCESS_STEP_START: start,
                                PROCESS_STEP_END: end,
                                PROCESS_STEP_ABS_DUR: duration,
                                PROCESS_STEP_REL_DUR: process_steps.at[i, PROCESS_STEP_REL_DUR],
                                STEP_STATS_SUM_ABS_DURATION: summed_duration,
                                STEP_STATS_SUM_REL_DURATION: summed_duration / (self._process_duration * num_processors),
                                STEP_STATS_AVG_CPU: machine_avg[MACHINE_UTILIZATION_AVG_CPU],
                                STEP_STATS_AVG_ABS_MEM: machine_avg[MACHINE_UTILIZATION_AVG_ABS_MEM],
                                STEP_STATS_AVG_REL_MEM: machine_avg[MACHINE_UTILIZATION_AVG_REL_MEM],
                                STEP_STATS_MAX_ABS_MEM: machine_max[MACHINE_UTILIZATION_MAX_ABS_MEM],
                                STEP_STATS_MAX_REL_MEM: machine_max[MACHINE_UTILIZATION_MAX_REL_MEM],
                                STEP_STATS_TOT_ABS_IDLE_TIME: p_stats_sum[PROCESSOR_STEP_STATS_TOT_ABS_IDLE_TIME],
                                STEP_STATS_TOT_REL_IDLE_TIME: p_stats_sum[PROCESSOR_STEP_STATS_TOT_ABS_IDLE_TIME] / summed_duration,
                                STEP_STATS_AVG_ABS_IDLE_TIME: p_stats_sum[PROCESSOR_STEP_STATS_TOT_ABS_IDLE_TIME] / num_processors,
                                STEP_STATS_AVG_REL_IDLE_TIME: (p_stats_sum[PROCESSOR_STEP_STATS_TOT_ABS_IDLE_TIME] / num_processors) / duration,
                                STEP_STATS_MAX_ABS_IDLE_TIME: p_stats_max[PROCESSOR_STEP_STATS_TOT_ABS_IDLE_TIME],
                                STEP_STATS_MAX_REL_IDLE_TIME: p_stats_max[PROCESSOR_STEP_STATS_TOT_ABS_IDLE_TIME] / duration,
                                STEP_STATS_TOT_ABS_BUSY_TIME: p_stats_sum[PROCESSOR_STEP_STATS_TOT_ABS_BUSY_TIME],
                                STEP_STATS_TOT_REL_BUSY_TIME: p_stats_sum[PROCESSOR_STEP_STATS_TOT_ABS_BUSY_TIME] / summed_duration,
                                STEP_STATS_AVG_ABS_BUSY_TIME: p_stats_sum[PROCESSOR_STEP_STATS_TOT_ABS_BUSY_TIME] / num_processors,
                                STEP_STATS_AVG_REL_BUSY_TIME: (p_stats_sum[PROCESSOR_STEP_STATS_TOT_ABS_BUSY_TIME] / num_processors) / duration,
                                STEP_STATS_MAX_ABS_BUSY_TIME: p_stats_max[PROCESSOR_STEP_STATS_TOT_ABS_BUSY_TIME],
                                STEP_STATS_MAX_REL_BUSY_TIME: p_stats_max[PROCESSOR_STEP_STATS_TOT_ABS_BUSY_TIME] / duration}, ignore_index=True)

        data.to_csv("process-step-statistics.csv", sep=";", index_label="index")
        processor_step_stats.to_csv("processor-step-statistics.csv", sep=";", index_label="index")
        return data

    def _processor_stats_in(self, process_step: pd.DataFrame, processor_step_stats: pd.DataFrame) -> pd.DataFrame:
        start: float = process_step[PROCESS_STEP_START]
        end: float = process_step[PROCESS_STEP_END]
        duration: float = process_step[PROCESS_STEP_ABS_DUR]

        idle_threshold: float = 1 / 20.0

        events: Dict[str, List[MachineUtilizationEvent]] = self._measurement_events_of_type(MachineUtilizationEvent.event_identifier())
        for processor in events:
            processor_idle_time: float = 0.0
            processor_busy_time: float = 0.0
            processor_max_abs_mem: int = 0
            processor_max_rel_mem: float = 0.0
            processor_events: List[MachineUtilizationEvent] = events[processor]

            for i in range(len(processor_events) - 1):
                a: MachineUtilizationEvent = processor_events[i]
                b: MachineUtilizationEvent = processor_events[i + 1]

                rel_time_a: float = (a.date_time - self._start_time).total_seconds()
                rel_time_b: float = (b.date_time - self._start_time).total_seconds()
                measurement_duration: float = rel_time_b - rel_time_a

                if rel_time_a < start:
                    continue
                if rel_time_a > end:
                    break

                processor_max_abs_mem = max(processor_max_abs_mem, a.used_memory)
                processor_max_rel_mem = max(processor_max_rel_mem, a.used_memory_ratio)

                if a.cpu_ratio < idle_threshold and b.cpu_ratio < idle_threshold:
                    processor_idle_time += measurement_duration
                else:
                    processor_busy_time += measurement_duration

            processor_step_stats = processor_step_stats.append({PROCESS_STEP_NAME: process_step[PROCESS_STEP_NAME],
                                                                PROCESS_STEP_START: start,
                                                                PROCESS_STEP_END: end,
                                                                PROCESS_STEP_ABS_DUR: duration,
                                                                PROCESS_STEP_REL_DUR: process_step[PROCESS_STEP_REL_DUR],
                                                                PROCESSOR_STEP_STATS_PROC_NAME: processor,
                                                                PROCESSOR_STEP_STATS_TOT_ABS_IDLE_TIME: processor_idle_time,
                                                                PROCESSOR_STEP_STATS_TOT_REL_IDLE_TIME: processor_idle_time / duration,
                                                                PROCESSOR_STEP_STATS_TOT_ABS_BUSY_TIME: processor_busy_time,
                                                                PROCESSOR_STEP_STATS_TOT_REL_BUSY_TIME: processor_busy_time / duration,
                                                                PROCESSOR_STEP_STATS_MAX_ABS_MEM: processor_max_abs_mem,
                                                                PROCESSOR_STEP_STATS_MAX_REL_MEM: processor_max_rel_mem}, ignore_index=True)

        return processor_step_stats

    def _add_process_shapes(self, row: int, column: int) -> None:
        for shape in self._process_steps:
            self._figure.add_shape(shape, row=row, col=column)
            self._figure.layout.shapes[-1].update({"yref": "paper"})
