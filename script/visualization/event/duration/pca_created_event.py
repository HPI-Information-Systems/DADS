from event.base.duration_event import DurationEvent


class PCACreatedEvent(DurationEvent):

    @staticmethod
    def event_identifier() -> str:
        return "PCACreated"

    @property
    def fill_color(self) -> str:
        return "#f57f17"  # yellow

    @property
    def simplified_name(self) -> str:
        return "PCA"
