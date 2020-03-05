from event.base.duration_event import DurationEvent


class NormalizedPathScoresCreatedEvent(DurationEvent):

    @staticmethod
    def event_identifier() -> str:
        return "PathScoreNormalizationCompleted"

    @property
    def fill_color(self) -> str:
        return "#006064"  # Cyan

    @property
    def simplified_name(self) -> str:
        return "NPS"
