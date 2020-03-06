from abc import ABC
from datetime import datetime

from event.base.statistics_event import StatisticsEvent, to_date_time


class MeasurementEvent(StatisticsEvent, ABC):

    @property
    def date_time(self) -> datetime:
        return self.get("DateTime", to_date_time)

    def is_within(self, start_time: datetime, end_time: datetime) -> bool:
        return start_time <= self.date_time <= end_time

    @property
    def is_measurement_event(self) -> bool:
        return True

    @property
    def is_duration_event(self) -> bool:
        return False
