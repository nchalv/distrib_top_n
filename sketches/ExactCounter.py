from typing import TypeVar, List, Dict, Set, Optional, Tuple, Generic
from sketches.sketch_base import SketchBase
# ExactCounter provides exact frequency counting for a predefined set of keys.
# It is intended for use-cases where the domain (keys) is known and bounded,
# enabling O(1) inserts and O(1) queries by key, and O(n log n) top-k retrieval.

T = TypeVar('T')

class ExactCounter(SketchBase[T], Generic[T]):
    """Exact frequency counter constrained to a specified set of allowed keys.

    Attributes:
        keys: The set of allowed items that can be counted.
        size: Cached size of the allowed-key set.
        counter: Mapping from item -> exact count (for items seen and allowed).
    """

    def __init__(self, keys: Optional[Set[T]] = None):
        # Initialize with the domain of allowed keys. Only items in `keys`
        # will be counted; all others are ignored.
        self.keys: Set[T] = set() if keys is None else set(keys)
        self.size = len(self.keys)
        # Initialize the SketchBase capacity to the number of allowed keys
        super().__init__(capacity=self.size)
        # Initialize counts for all allowed keys to 0
        self.counter: Dict[T, int] = {k: 0 for k in self.keys}


    def contains(self, item: T) -> bool:
        # Return whether an item is within the allowed domain.
        # Complexity: O(1) average for hashable items.
        return item in self.keys

    def insert(self, item: T) -> None:
        # Insert a single occurrence of `item`.
        # If `item` is not in the allowed domain, this is a no-op.
        # Complexity: O(1) average for hashable items.
        if item in self.keys:
            self.counter[item] = self.counter.get(item, 0) + 1

    def total_count(self) -> int:
        # Return the total number of observed items (sum of all counts).
        # Complexity: O(m), where m is the number of distinct observed items.
        return sum(self.counter.values())

    def get_count(self, item: T) -> Optional[int]:
        """Get exact count for an item

        Returns:
            The exact frequency for `item` if it has been observed and is allowed,
            otherwise None.
        """
        if item not in self.keys:
            return None
        return int(self.counter.get(item, 0))

    def topk(self, k: Optional[int] = None) -> List[Tuple[T, int]]:
        # Return the k most frequent (item, count) pairs, sorted by descending count.
        # If k is None or k >= number of distinct observed items, returns all observed items.
        # Complexity: O(m log m), where m is number of distinct observed items.
        items: List[Tuple[T, int]] = sorted(self.counter.items(), key=lambda x: x[1], reverse=True)
        if k is None:
            return items
        return items[:k]

def aggregate_exact_counters(counters: List[ExactCounter[T]], capacity: Optional[int] = None) -> ExactCounter[T]:
    """Aggregate multiple ExactCounter instances into a single counter.

    Arguments:
        counters: The list of ExactCounter instances to merge.
        capacity: Optional limit on the number of items to keep in the merged result.
                  If provided, the merged counter will retain only the top `capacity`
                  items by total frequency across all counters.

    Returns:
        A new ExactCounter with:
          - keys equal to the union of all input counters' keys (or truncated to top-N if capacity is set),
          - counts equal to the sum across all input counters for each item.

    Notes:
        - Items not present in any input counters are not added.
        - If capacity is provided, the allowed keys of the resulting counter are restricted
          to the top items after aggregation.
        - This function assumes `counter` on each input is a standard mapping item->int.
    """
    # Merge counts from multiple ExactCounter instances.
    # - Combines keys as the union of all input counters' keys.
    # - Sums per-item counts across all counters.
    # - If capacity is provided, keeps only the top `capacity` items by count.
    if not counters:
        return ExactCounter(keys=set())

    # Union of all allowed keys
    all_keys: Set[T] = set()
    for c in counters:
        if hasattr(c, "keys") and isinstance(c.keys, set):
            all_keys |= c.keys

    merged: ExactCounter[T] = ExactCounter(keys=all_keys)

    # Aggregate counts
    agg: Dict[T, int] = {}
    for c in counters:
        if not hasattr(c, "counter") or not isinstance(c.counter, dict):
            continue
        for k, v in c.counter.items():
            agg[k] = agg.get(k, 0) + int(v)

    # Initialize merged counter with zeros for all union keys, then apply aggregated counts
    merged.counter = {k: 0 for k in merged.keys}
    for k, v in agg.items():
        merged.counter[k] = v

    # If capacity is set, use merged.topk to retain only top-N items
    if capacity is not None and capacity >= 0:
        top_items: List[Tuple[T, int]] = merged.topk(capacity)
        merged.keys = {k for k, _ in top_items}
        merged.size = len(merged.keys)
        merged.counter = {k: v for k, v in top_items}

    return merged


# Example usage:
if __name__ == "__main__":
    # Define the allowed key domain
    allowed_keys: Set[str] = {'a', 'b', 'c', 'd', 'e'}

    # Create exact counter constrained to allowed_keys
    counter = ExactCounter(keys=allowed_keys)

    # Process a list of items by inserting them one by one
    stream = ['a', 'b', 'a', 'c', 'b', 'a', 'd', 'e', 'a', 'b', 'x']  # 'x' is not allowed
    for it in stream:
        counter.insert(it)

    # Get results
    print("Total count:", counter.total_count())
    print("Top-3 items:", counter.topk(k=3))
    print("Count of 'a':", counter.get_count('a'))
    print("Count of 'x' (not allowed):", counter.get_count('x'))  # May be None per current implementation

    # Test aggregation
    counter1 = ExactCounter(keys=allowed_keys)
    for it in ['a', 'b', 'a']:
        counter1.insert(it)

    counter2 = ExactCounter(keys=allowed_keys)
    for it in ['b', 'c', 'a', 'c']:
        counter2.insert(it)

    merged = aggregate_exact_counters([counter1, counter2])
    print("Merged total count:", merged.total_count())
    print("Merged top-3 items:", merged.topk(k=3))

    # Optional: demonstrate capacity-limited aggregation (top-2 most frequent items retained)
    merged_top2 = aggregate_exact_counters([counter1, counter2], capacity=2)
    print("Merged (capacity=2) keys:", merged_top2.keys)
    print("Merged (capacity=2) items:", merged_top2.topk(k=2))
