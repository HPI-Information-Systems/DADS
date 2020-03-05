from event.base.duration_event import DurationEvent


class NodesCreatedEvent(DurationEvent):

    @staticmethod
    def event_identifier() -> str:
        return "NodeCreationCompleted"

    @property
    def fill_color(self) -> str:
        return "#b71c1c"  # Red

    @property
    def simplified_name(self) -> str:
        return "N"
