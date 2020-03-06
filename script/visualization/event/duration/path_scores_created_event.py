from event.base.duration_event import DurationEvent


class PathScoresCreatedEvent(DurationEvent):

    @staticmethod
    def event_identifier() -> str:
        return "PathScoresCreated"

    @property
    def fill_color(self) -> str:
        return "#e65100"  # Orange

    @property
    def simplified_name(self) -> str:
        return "PS"
