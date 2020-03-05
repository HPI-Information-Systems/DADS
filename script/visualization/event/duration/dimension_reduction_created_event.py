from event.base.duration_event import DurationEvent


class DimensionReductionCreatedEvent(DurationEvent):

    @staticmethod
    def event_identifier() -> str:
        return "DimensionReductionCompleted"

    @property
    def fill_color(self) -> str:
        return "#0d47a1"  # Blue

    @property
    def simplified_name(self) -> str:
        return "DR"
