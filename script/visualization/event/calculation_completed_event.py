from event.base import DurationEvent


class CalculationCompletedEvent(DurationEvent):

    @staticmethod
    def event_identifier() -> str:
        return "CalculationCompleted"

    @property
    def simplified_name(self) -> str:
        return "Calculation"
