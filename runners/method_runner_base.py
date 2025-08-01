from abc import ABC, abstractmethod

class MethodRunnerBase(ABC):
    @abstractmethod
    def initialize_sketches(self, window_id: int) -> None:
        pass

    @abstractmethod
    def insert_item(self, partition_id: int, item: str) -> None:
        pass

    @abstractmethod
    def finalize_window(self, window_id: int) -> dict:
        pass

    @abstractmethod
    def __str__(self) -> str:
        pass
