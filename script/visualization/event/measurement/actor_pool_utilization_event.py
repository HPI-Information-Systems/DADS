from event.base import MeasurementEvent


class ActorPoolUtilizationEvent(MeasurementEvent):

    @staticmethod
    def event_identifier() -> str:
        return "ActorPoolUtilization"

    @property
    def workers(self) -> int:
        return self.get("Workers", lambda x: int(x))

    @property
    def available_workers(self) -> int:
        return self.get("AvailableWorkers", lambda x: int(x))

    @property
    def used_workers(self) -> int:
        return self.get_without_property("cached_used_workers", lambda: self.workers - self.available_workers)

    @property
    def workers_ratio(self) -> float:
        return self.get_without_property("cached_workers_ratio", lambda: self.used_workers / float(self.workers))

    @property
    def queue_size(self) -> int:
        return self.get("QueueSize", lambda x: int(x))
