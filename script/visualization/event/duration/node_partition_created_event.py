from event.base.duration_event import DurationEvent


class NodePartitionCreatedEvent(DurationEvent):

    @staticmethod
    def event_identifier() -> str:
        return "NodePartitionCreationCompleted"

    @property
    def fill_color(self) -> str:
        return "#1b5e20"  # Green

    @property
    def simplified_name(self) -> str:
        return "NP"
