from event.base import DurationEvent


class IntersectionsCreatedEvent(DurationEvent):

    @staticmethod
    def event_identifier() -> str:
        return "IntersectionsCreated"

    @property
    def fill_color(self) -> str:
        return "#4a148c"  # Purple

    @property
    def simplified_name(self) -> str:
        return "IC"
