from datetime import datetime
from typing import Dict

DATE_FORMAT = "%Y/%m/%d %H:%M:%S.%f"


def to_date_time(string: str) -> datetime:
    return datetime.strptime(string, DATE_FORMAT)


def _to_event(event_properties: str, event_factory):
    property_separator: str = "; "
    key_value_separator: str = " = "

    properties: Dict[str, str] = {}
    for prop in event_properties.split(property_separator):
        if not prop:
            continue

        key, value = prop.split(key_value_separator)
        properties[key.strip()] = value.strip()

    return event_factory(properties)


class UtilizationEvent:

    @staticmethod
    def event_name() -> str:
        return "Utilization"

    def __init__(self, properties: Dict[str, str]):
        self._properties: Dict[str, str] = properties

    @property
    def processor(self) -> str:
        return self._properties["Processor"]

    @property
    def date_time(self) -> datetime:
        return to_date_time(self._properties["DateTime"])

    @property
    def used_memory(self) -> float:
        return float(self._properties["UsedMemory"]) / float(self._properties["MaximumMemory"])

    @property
    def used_cpu(self) -> float:
        return float(self._properties["CPULoad"])

    @staticmethod
    def parse(properties: str) -> "UtilizationEvent":
        return _to_event(properties, UtilizationEvent)


class MessageExchangeUtilizationEvent:

    @staticmethod
    def event_name() -> str:
        return "MessageExchangeUtilization"

    def __init__(self, properties: Dict[str, str]):
        self._properties: Dict[str, str] = properties

    @property
    def processor(self) -> str:
        return self._properties["Processor"]

    @property
    def date_time(self) -> datetime:
        return to_date_time(self._properties["DateTime"])

    @property
    def remote_processor(self) -> str:
        return self._properties["RemoteProcessor"]

    @property
    def total_enqueued_messages(self) -> int:
        return int(self._properties["TotalNumberOfEnqueuedMessages"])

    @property
    def total_unacknowledged_message(self) -> int:
        return int(self._properties["TotalNumberOfUnacknowledgedMessages"])

    @property
    def largest_message_queue_size(self) -> int:
        return int(self._properties["LargestMessageQueueSize"])

    @property
    def largest_message_queue_receiver(self) -> str:
        return self._properties["LargestMessageQueueReceiver"]

    @property
    def average_queue_size(self) -> float:
        return float(self._properties["AverageNumberOfEnqueuedMessages"])

    @staticmethod
    def parse(properties: str) -> "MessageExchangeUtilizationEvent":
        return _to_event(properties, MessageExchangeUtilizationEvent)
