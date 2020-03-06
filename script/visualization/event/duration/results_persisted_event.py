from event.base import DurationEvent


class ResultsPersistedEvent(DurationEvent):

    @staticmethod
    def event_identifier() -> str:
        return "ResultsPersisted"

    @property
    def fill_color(self) -> str:
        return "#bf360c"  # Deep Orange

    @property
    def simplified_name(self) -> str:
        return "R"
