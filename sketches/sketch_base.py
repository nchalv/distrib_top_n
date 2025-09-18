from abc import ABC, abstractmethod
from typing import List, Tuple, Optional, TypeVar, Generic

T = TypeVar('T')


class SketchBase(ABC, Generic[T]):
    """
    Abstract base class for streaming frequency sketches.
    """

    def __init__(self, capacity: int):
        self.capacity = capacity

    @abstractmethod
    def insert(self, item: T) -> None:
        """
        Insert an item into the sketch.
        """
        pass

    @abstractmethod
    def topk(self, k: Optional[int] = None) -> List[Tuple[T, int]]:
        """
        Return the top-k elements (item, count). If k is None, return all.
        """
        pass

    @abstractmethod
    def total_count(self) -> int:
        """
        Return the total number of items counted in the sketch.
        """
        pass

