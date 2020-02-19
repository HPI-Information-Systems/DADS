from datetime import datetime
from typing import Dict

DATE_FORMAT = "%Y/%m/%d %H:%M:%S.%f"


def to_date_time(string: str) -> datetime:
    return datetime.strptime(string, DATE_FORMAT)


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
    def parse(utilization_event_properties: str) -> "UtilizationEvent":
        property_separator: str = "; "
        key_value_separator: str = " = "

        properties: Dict[str, str] = {}
        for prop in utilization_event_properties.split(property_separator):
            if not prop:
                continue
                
            key, value = prop.split(key_value_separator)
            properties[key.strip()] = value.strip()

        return UtilizationEvent(properties)
