from event.base.duration_event import DurationEvent


class DataTransferredEvent(DurationEvent):

    @staticmethod
    def event_identifier() -> str:
        return "DataTransferCompleted"

    @property
    def simplified_name(self) -> str:
        return self.get("Type", DataTransferredEvent._simple_type_name)

    @staticmethod
    def _simple_type_name(type_name: str) -> str:
        class_name: str = type_name.split("$")[1]
        class_name = str(class_name[len("Initialize"): -len("TransferMessage")])

        return class_name

    @property
    def sender(self) -> str:
        return self["Source"]

    @property
    def receiver(self) -> str:
        return self["Sink"]

    @property
    def transferred_bytes(self) -> int:
        return self.get("Bytes", lambda x: int(x))
