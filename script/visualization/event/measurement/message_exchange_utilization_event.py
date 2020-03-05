from event.base import MeasurementEvent


class MessageExchangeUtilizationEvent(MeasurementEvent):

    @staticmethod
    def event_identifier() -> str:
        return "MessageExchangeUtilization"

    @property
    def remote_processor(self) -> str:
        return self["RemoteProcessor"]

    @property
    def total_queue_size(self) -> int:
        return self.get("TotalNumberOfEnqueuedMessages", lambda x: int(x))

    @property
    def total_unack_messages(self) -> int:
        return self.get("TotalNumberOfUnacknowledgedMessages", lambda x: int(x))

    @property
    def max_actor_queue_size(self) -> int:
        return self.get("LargestMessageQueueSize", lambda x: int(x))

    @property
    def max_actor_queue_receiver(self) -> str:
        return self["LargestMessageQueueReceiver"]

    @property
    def average_queue_size(self) -> float:
        return self.get("AverageNumberOfEnqueuedMessages", lambda x: float(x))
