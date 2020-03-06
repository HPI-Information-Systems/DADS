import inspect
import re
from abc import ABC, abstractmethod
from datetime import datetime
from typing import Dict, List, Any, Callable, Optional

__date_time_format: str = "%Y/%m/%d %H:%M:%S.%f"


def to_date_time(string: str) -> datetime:
    return datetime.strptime(string, __date_time_format)


class StatisticsEvent(ABC):
    __log_line_pattern: re = re.compile(r"^\[(?P<LogDateTime>\d{4}/\d{2}/\d{2}\s\d{2}:\d{2}:\d{2}\.\d{3})]"
                                        r"\s{2}-\s{2}"
                                        r"(?P<EventIdentifier>\w+?)\s{\s(?P<EventProperties>.+?)\s}$")

    __event_factory: Optional[Dict[str, Callable[[Dict[str, str]], "StatisticsEvent"]]] = None

    @staticmethod
    @abstractmethod
    def event_identifier() -> str:
        raise NotImplementedError

    @staticmethod
    def from_log_ling(log_line: str) -> Optional["StatisticsEvent"]:
        if not StatisticsEvent.__event_factory:
            StatisticsEvent._initialize_event_factory()

        match = StatisticsEvent.__log_line_pattern.match(log_line)
        event_identifier: str = match.group("EventIdentifier")

        if event_identifier not in StatisticsEvent.__event_factory:
            print(f"Skipping unknown event type \"{event_identifier}\"")
            return None

        event_properties: Dict[str, str] = StatisticsEvent._extract_properties(match.group("EventProperties"))
        return StatisticsEvent.__event_factory[event_identifier](event_properties)

    @staticmethod
    def _initialize_event_factory() -> None:
        StatisticsEvent.__event_factory = {}

        StatisticsEvent._add_sub_classes_to_event_factory(StatisticsEvent)

    @staticmethod
    def _add_sub_classes_to_event_factory(cls) -> None:
        if not inspect.isabstract(cls):
            StatisticsEvent.__event_factory[cls.event_identifier()] = cls.__call__

        for sub_class in cls.__subclasses__():
            StatisticsEvent._add_sub_classes_to_event_factory(sub_class)

    @staticmethod
    def _extract_properties(event_properties: str) -> Dict[str, str]:
        properties: Dict[str, str] = {}

        property_pairs: List[List[str]] = [x.strip().split(" = ")
                                           for x in event_properties.split(";")
                                           if x]

        for property_pair in property_pairs:
            properties[property_pair[0]] = property_pair[1]

        return properties

    def __init__(self, properties: Dict[str, str]):
        self._properties: Dict[str, str] = properties
        self._cached_properties: Dict[str, Any] = {}

    def __getitem__(self, item: str) -> str:
        return self._properties[item]

    def get(self, item: str, value_initializer: Callable[[str], Any]) -> Any:
        if item not in self._cached_properties:
            self._cached_properties[item] = value_initializer(self[item])

        return self._cached_properties[item]

    def get_without_property(self, item: str, value_initializer: Callable[[], Any]) -> Any:
        if item not in self._cached_properties:
            self._cached_properties[item] = value_initializer()

        return self._cached_properties[item]

    @property
    def processor(self) -> str:
        return self["Processor"]

    @abstractmethod
    def is_within(self, start_time: datetime, end_time: datetime) -> bool:
        raise NotImplementedError

    @property
    @abstractmethod
    def is_measurement_event(self) -> bool:
        raise NotImplementedError

    @property
    @abstractmethod
    def is_duration_event(self) -> bool:
        raise NotImplementedError
