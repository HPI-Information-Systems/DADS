from event.base.duration_event import DurationEvent


class EdgePartitionCreatedEvent(DurationEvent):

    @staticmethod
    def event_identifier() -> str:
        return "EdgePartitionCreated"

    @property
    def fill_color(self) -> str:
        return "#3e2723"  # Brown

    @property
    def simplified_name(self) -> str:
        return "EP"
