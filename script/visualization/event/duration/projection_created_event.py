from event.base.duration_event import DurationEvent


class ProjectionCreatedEvent(DurationEvent):

    @staticmethod
    def event_identifier() -> str:
        return "ProjectionCreationCompleted"

    @property
    def fill_color(self) -> str:
        return "#880e4f"  # Pink

    @property
    def simplified_name(self) -> str:
        return "P"
