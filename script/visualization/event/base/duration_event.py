from abc import ABC, abstractmethod
from datetime import timedelta, datetime

from event.base.statistics_event import to_date_time, StatisticsEvent


class DurationEvent(StatisticsEvent, ABC):

    @property
    def start_time(self) -> datetime:
        return self.get("StartTime", to_date_time)

    @property
    def end_time(self) -> datetime:
        return self.get("EndTime", to_date_time)

    @property
    def duration(self) -> timedelta:
        return self.get_without_property("cached_duration", lambda: self.end_time - self.start_time)

    def is_within(self, start_time: datetime, end_time: datetime) -> bool:
        return start_time <= self.start_time <= end_time and start_time <= self.end_time <= end_time

    @property
    def is_measurement_event(self) -> bool:
        return False

    @property
    def is_duration_event(self) -> bool:
        return True

    @property
    def fill_color(self) -> str:
        return "#000000"

    @property
    @abstractmethod
    def simplified_name(self) -> str:
        raise NotImplementedError
