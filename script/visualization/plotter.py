from datetime import datetime
from typing import Dict, List, Tuple, Any

import plotly
import plotly.graph_objects as go
from plotly.subplots import make_subplots

from event import CalculationCompletedEvent
from event.base import StatisticsEvent, DurationEvent
from event.duration import *
from event.measurement import MachineUtilizationEvent


class Plotter:

    @staticmethod
    def _create_figure() -> plotly.subplots:
        figure: plotly.subplots = make_subplots(rows=1, cols=2,
                                                subplot_titles=("CPU & Memory Utilization", "Data Transferred",),
                                                horizontal_spacing=0.05,
                                                specs=[[{"secondary_y": False}, {"secondary_y": True}]])

        return figure

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

    def __init__(self, processor: str,
                 events: Dict[str, List[StatisticsEvent]],
                 show_annotations: bool = True,
                 show_process_steps: bool = True):
        self._show_annotations: bool = show_annotations
        self._show_process_steps: bool = show_process_steps
        self._processor: str = processor
        self._events: Dict[str, List[StatisticsEvent]] = events
        self._start_time, self._end_time = self._extract_start_and_end_time()
        self._x_axis_range: List[float] = [0.0, (self._end_time - self._start_time).total_seconds()]
        self._figure: plotly.subplots = Plotter._create_figure()
        self._process_step_shapes: List[Dict[str, Any]] = []
        self._process_step_annotations: List[Dict[str, Any]] = []
        self._measurement_interval: float = 0.01

    def _extract_start_and_end_time(self) -> Tuple[datetime, datetime]:
        interval_event: CalculationCompletedEvent = self._events[CalculationCompletedEvent.event_identifier()][0]
        return interval_event.start_time, interval_event.end_time

    def show(self) -> None:
        self._create_process_step_shapes()

        self._plot_cpu_and_memory_ratio()
        self._plot_data_sent_and_received()

        self._figure.update_layout({
            "title": self._processor,
            "paper_bgcolor": "rgba(0,0,0,0)",
            "plot_bgcolor": "rgba(0,0,0,0)",
            "legend": {
                "x": 0.0,
                "y": 1.2,
                "orientation": "h",
            }
        })
        self._figure.show()

    def _create_process_step_shapes(self) -> None:
        if not self._show_process_steps:
            return

        process_step_events: List[DurationEvent] = [self._events[x]
                                                    for x in Plotter._process_step_event_types()
                                                    if x in self._events]

        shape_height: float = 1.0 / (len(process_step_events) + 1)
        y0: float = shape_height
        for events in process_step_events:
            if not events:
                continue

            event: DurationEvent = events[0]
            x0: float = (event.start_time - self._start_time).total_seconds()
            x1: float = (event.end_time - self._start_time).total_seconds()
            duration_ratio: float = (x1 - x0) / (self._end_time - self._start_time).total_seconds()
            self._process_step_shapes.append({
                "type": "rect",
                "xref": "x",
                "yref": "paper",
                "x0": x0,
                "x1": x1,
                "y0": 0.0,
                "y1": 1.0,
                "opacity": 0.2,
                "layer": "below",
                "line_width": 0,
                "fillcolor": event.fill_color,
            })
            self._process_step_annotations.append({
                "x": x0 + ((x1 - x0) / 2),
                "y": 1.0,
                "xref": "x",
                "yref": "paper",
                "ax": 0,
                "ay": 0,
                "showarrow": False,
                "arrowhead": 0,
                "bgcolor": "rgba(0,0,0,0)",
                "bordercolor": event.fill_color,
                "borderwidth": 2,
                "opacity": 0.85,
                "text": "{} {:.3f}s ({:.2%})".format(event.simplified_name, x1 - x0, duration_ratio),
                "textangle": -90,
            })
            y0 += shape_height

    def _plot_cpu_and_memory_ratio(self) -> None:
        events: List[MachineUtilizationEvent] = self._events[MachineUtilizationEvent.event_identifier()]

        x_values: List[float] = [(x.date_time - self._start_time).total_seconds() for x in events]

        # Memory
        self._figure.add_trace(go.Scatter(
            x=x_values,
            y=[x.heap_ratio for x in events],
            hovertext=[x.memory_hover_text for x in events],
            mode="lines",
            showlegend=True,
            name="Heap",
            stackgroup="memory",
            line_width=2,
        ), row=1, col=1)

        self._figure.add_trace(go.Scatter(
            x=x_values,
            y=[x.stack_ratio for x in events],
            hovertext=[x.memory_hover_text for x in events],
            mode="lines",
            showlegend=True,
            name="Stack",
            stackgroup="memory",
            line_width=2,
        ), row=1, col=1)

        if self._show_annotations:
            max_heap_event: MachineUtilizationEvent = max(events, key=lambda x: x.heap_ratio)
            self._figure.add_annotation({
                "x": (max_heap_event.date_time - self._start_time).total_seconds(),
                "y": max_heap_event.heap_ratio,
                "xref": "x1",
                "yref": "y1",
                "ax": 0,
                "ay": 40,
                "showarrow": True,
                "arrowhead": 0,
                "bgcolor": "#ffffff",
                "bordercolor": "#000000",
                "borderwidth": 2,
                "text": "Max Heap: {:.2%}<br>{}".format(max_heap_event.heap_ratio,
                                                        max_heap_event.memory_hover_text)
            })

            max_stack_event: MachineUtilizationEvent = max(events, key=lambda x: x.stack_ratio)
            self._figure.add_annotation({
                "x": (max_stack_event.date_time - self._start_time).total_seconds(),
                "y": max_stack_event.stack_ratio + max_stack_event.heap_ratio,
                "xref": "x1",
                "yref": "y1",
                "ax": 0,
                "ay": -40,
                "showarrow": True,
                "arrowhead": 0,
                "bgcolor": "#ffffff",
                "bordercolor": "#000000",
                "borderwidth": 2,
                "text": "Max Stack: {:.2%}<br>{}".format(max_stack_event.stack_ratio,
                                                         max_stack_event.memory_hover_text)
            })

        # CPU
        self._figure.add_trace(go.Scatter(
            x=x_values,
            y=[x.cpu_ratio for x in events],
            mode="lines",
            showlegend=True,
            name="CPU",
            line_width=2,
        ), row=1, col=1)

        if self._show_annotations:
            max_cpu_ratio_event: MachineUtilizationEvent = max(events, key=lambda x: x.cpu_ratio)
            self._figure.add_annotation({
                "x": (max_cpu_ratio_event.date_time - self._start_time).total_seconds(),
                "y": max_cpu_ratio_event.cpu_ratio,
                "xref": "x1",
                "yref": "y1",
                "ax": 0,
                "ay": -40,
                "showarrow": True,
                "arrowhead": 0,
                "bgcolor": "#ffffff",
                "bordercolor": "#000000",
                "borderwidth": 2,
                "text": "Max CPU: {:.2%}".format(max_cpu_ratio_event.cpu_ratio),
            })

        self._append_process_shapes(1, 1)

        self._figure["layout"]["xaxis1"].update({
            "title": "Time (s)",
            "rangemode": "nonnegative",
            "range": self._x_axis_range,
        })
        self._figure["layout"]["yaxis1"].update({
            "title": "Utilization (%)",
            "tickformat": "%",
            "rangemode": "nonnegative",
        })

    def _plot_data_sent_and_received(self) -> None:
        events: List[DataTransferredEvent] = self._events[DataTransferredEvent.event_identifier()]

        bytes_received: List[int] = [0]
        bytes_sent: List[int] = [0]
        bytes_self: List[int] = [0]
        data_hover_texts: List[str] = [""]

        rate_receive: List[float] = [0.0]
        rate_send: List[float] = [0.0]
        rate_hover_texts: List[str] = [""]
        mibs_per_seconds: float = 1.0 / (1024**2 * self._measurement_interval)

        x_values: List[float] = [0.0]
        time_interval: float = (self._end_time - self._start_time).total_seconds()
        measurement_point: float = self._measurement_interval

        while measurement_point < time_interval:
            received: int = 0
            sent: int = 0
            loopback: int = 0

            for event in events:
                relative_start_time: float = (event.start_time - self._start_time).total_seconds()
                relative_end_time: float = (event.end_time - self._start_time).total_seconds()

                if relative_start_time >= measurement_point:
                    continue
                if relative_end_time <= measurement_point:
                    continue

                data_transferred_interpolated: float = event.transferred_bytes * self._measurement_interval / event.duration.total_seconds()
                if event.sender == self._processor == event.receiver:
                    loopback += int(data_transferred_interpolated)
                elif event.sender == self._processor:
                    sent += int(data_transferred_interpolated)
                elif event.receiver == self._processor:
                    received += int(data_transferred_interpolated)

            bytes_received.append(bytes_received[-1] + received / 1024**2)
            bytes_sent.append(bytes_sent[-1] + sent / 1024**2)
            bytes_self.append(bytes_self[-1] + loopback / 1024**2)
            data_hover_texts.append("Total: {:.3f} MiB<br>Self: {:.3f} MiB<br>Sent: {:.3f} MiB<br>Recv: {:.3f} MiB".format(
                bytes_received[-1] + bytes_sent[-1] + bytes_self[-1],
                bytes_self[-1],
                bytes_sent[-1],
                bytes_received[-1],
            ))

            rate_receive.append(received * mibs_per_seconds)
            rate_send.append(sent * mibs_per_seconds)
            rate_hover_texts.append("Total: {:.3f} MiB/s<br>end: {:.3f} MiB/S<br>Recv: {:.3f} MiB/s".format(
                rate_receive[-1] + rate_send[-1],
                rate_send[-1],
                rate_receive[-1]
            ))

            x_values.append(measurement_point)
            measurement_point += self._measurement_interval

        # Received
        self._figure.add_trace(go.Scatter(
            x=x_values,
            y=bytes_received,
            mode="lines",
            showlegend=True,
            name="Received",
            hovertext=data_hover_texts,
            stackgroup="data-transferred"
        ), row=1, col=2)
        self._figure.add_trace(go.Scatter(
            x=x_values,
            y=rate_receive,
            mode="lines",
            showlegend=True,
            name="Receive Rate",
            hovertext=rate_hover_texts,
            stackgroup="data-rate",
        ), row=1, col=2, secondary_y=True)

        # Sent
        self._figure.add_trace(go.Scatter(
            x=x_values,
            y=bytes_sent,
            mode="lines",
            showlegend=True,
            name="Sent",
            hovertext=data_hover_texts,
            stackgroup="data-transferred"
        ), row=1, col=2)
        self._figure.add_trace(go.Scatter(
            x=x_values,
            y=rate_send,
            mode="lines",
            showlegend=True,
            name="Send Rate",
            hovertext=rate_hover_texts,
            stackgroup="data-rate",
        ), row=1, col=2, secondary_y=True)

        # Self Transferred
        self._figure.add_trace(go.Scatter(
            x=x_values,
            y=bytes_self,
            mode="lines",
            showlegend=True,
            name="Self Transfer",
            hovertext=data_hover_texts,
            stackgroup="data-transferred"
        ), row=1, col=2)

        if self._show_annotations:
            grouped_events: Dict[str, List[DataTransferredEvent]] = {}
            for event in events:
                if event.simplified_name not in grouped_events:
                    grouped_events[event.simplified_name] = []

                grouped_events[event.simplified_name].append(event)

            for event_type in grouped_events:
                event_group: List[DataTransferredEvent] = grouped_events[event_type]
                event_type_start: float = (event_group[0].start_time - self._start_time).total_seconds()
                event_type_end: float = (event_group[-1].end_time - self._start_time).total_seconds()
                x_value: float = event_type_start + ((event_type_end - event_type_start) / 2)
                closest_measurement_index: int = min(range(len(x_values)), key=lambda i: abs(x_values[i] - x_value))
                text: str = f"{event_type}"

                self_events: List[DataTransferredEvent] = list(
                    filter(lambda x: x.sender == x.receiver == self._processor, event_group))
                if len(self_events) > 0:
                    self_start: float = (self_events[0].start_time - self._start_time).total_seconds()
                    self_end: float = (self_events[-1].end_time - self._start_time).total_seconds()
                    data_self: int = sum((x.transferred_bytes for x in self_events))
                    data_self_rate: float = data_self / (self_end - self_start)
                    text += "<br>Self: {:.3f} MiB ({:.3f} MiB/s)".format(data_self / float(1024 ** 2),
                                                                         data_self_rate / float(1024 ** 2))

                sent_events: List[DataTransferredEvent] = list(
                    filter(lambda x: x.sender != x.receiver and x.sender == self._processor, event_group))
                if len(sent_events) > 0:
                    sent_start: float = (sent_events[0].start_time - self._start_time).total_seconds()
                    sent_end: float = (sent_events[-1].end_time - self._start_time).total_seconds()
                    sent: int = sum((x.transferred_bytes for x in sent_events))
                    data_send_rate: float = sent / (sent_end - sent_start)
                    text += "<br>Sent: {:.3f} MiB ({:.3f} MiB/s)".format(sent / float(1024 ** 2),
                                                                         data_send_rate / float(1024 ** 2))

                receive_events: List[DataTransferredEvent] = list(
                    filter(lambda x: x.sender != x.receiver and x.receiver == self._processor, event_group))
                if len(receive_events) > 0:
                    receive_start: float = (receive_events[0].start_time - self._start_time).total_seconds()
                    receive_end: float = (receive_events[-1].end_time - self._start_time).total_seconds()
                    received: int = sum((x.transferred_bytes for x in receive_events))
                    data_receive_rate: float = received / (receive_end - receive_start)
                    text += "<br>Recv: {:.3f} MiB ({:.3f} MiB/s)".format(received / float(1024 ** 2),
                                                                         data_receive_rate / float(1024 ** 2))

                self._figure.add_annotation({
                    "x": x_values[closest_measurement_index],
                    "y": bytes_sent[closest_measurement_index] + bytes_received[closest_measurement_index] +
                         bytes_self[closest_measurement_index],
                    "xref": "x2",
                    "yref": "y2",
                    "ax": 0,
                    "ay": -40,
                    "showarrow": True,
                    "arrowhead": 0,
                    "bgcolor": "#ffffff",
                    "bordercolor": "#000000",
                    "borderwidth": 2,
                    "text": text
                })

        self._append_process_shapes(1, 2)

        self._figure["layout"]["xaxis2"].update({
            "title": "Time (s)",
            "rangemode": "nonnegative",
            "range": self._x_axis_range,
        })
        self._figure["layout"]["yaxis2"].update({
            "title": "Data (MiB)",
            "rangemode": "nonnegative",
        })
        self._figure["layout"]["yaxis3"].update({
            "overlaying": "y1",
            "side": "right",
            "anchor": "x2",
            "title": "Data Rate (MiB/s)",
            "rangemode": "nonnegative",
        })

    def _append_process_shapes(self, row: int, column: int):
        for shape in self._process_step_shapes:
            self._figure.add_shape(shape, row=row, col=column)
            self._figure.layout.shapes[-1].update({"yref": "paper"})

        for shape_annotation in self._process_step_annotations:
            self._figure.add_annotation(shape_annotation, row=row, col=column)
            self._figure.layout.annotations[-1].update({"yref": "paper"})
