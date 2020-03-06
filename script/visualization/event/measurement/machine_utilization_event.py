from event.base import MeasurementEvent


class MachineUtilizationEvent(MeasurementEvent):

    @staticmethod
    def event_identifier() -> str:
        return "MachineUtilization"

    @property
    def total_memory(self) -> int:
        return self.get("MaximumMemory", lambda x: int(x))

    @property
    def heap(self) -> int:
        return self.get("UsedHeap", lambda x: int(x))

    @property
    def heap_ratio(self) -> float:
        return self.get_without_property("cached_used_heap_ratio", lambda: self.heap / float(self.total_memory))

    @property
    def stack(self) -> int:
        return self.get("UsedStack", lambda x: int(x))

    @property
    def stack_ratio(self) -> float:
        return self.get_without_property("cached_used_stack_ratio", lambda: self.stack / float(self.total_memory))

    @property
    def used_memory(self) -> int:
        return self.get_without_property("cached_total_used_memory", lambda: self.heap + self.stack)

    @property
    def used_memory_ratio(self) -> float:
        return self.get_without_property("cached_total_used_ratio",
                                         lambda: self.used_memory / float(self.total_memory))

    @property
    def cpu_ratio(self) -> float:
        return self.get("CPULoad", lambda x: float(x))

    @property
    def memory_hover_text(self) -> str:
        total_memory_in_mib: float = self.get_without_property("cached_total_memory_in_mib",
                                                               lambda: self.total_memory / float(1024 ** 2))
        used_heap_in_mib: float = self.get_without_property("cached_used_memory_in_mib",
                                                            lambda: self.heap / float(1024 ** 2))
        used_stack_in_mib: float = self.get_without_property("cached_used_stack_in_mib",
                                                             lambda: self.stack / float(1024 ** 2))

        return self.get_without_property("cached_memory_hover_text",
                                         lambda: "<b>Available</b> {:.3f} MiB<br><b>Stack</b> {:.3f} MiB ({:.2%})<br><b>Heap</b> {:.3f} MiB ({:.2%})"
                                         .format(total_memory_in_mib,
                                                 used_stack_in_mib, self.stack_ratio,
                                                 used_heap_in_mib, self.heap_ratio))
