from typing import Optional, Dict, Set, List, Tuple, Any, TypeVar, Generic
from sketches.sketch_base import SketchBase

T = TypeVar('T')

class StreamSummary(SketchBase[T], Generic[T]):
    """
    Space-Saving sketch for approximate heavy hitters.

    Maintains at most `capacity` items along with estimated counts and
    per-item overestimation values. Items are grouped into buckets by
    identical counts and the buckets are kept in a doubly linked list,
    enabling efficient updates when counts change.
    """

    class Bucket:
        __slots__ = ['count', 'elements', 'next', 'prev']

        def __init__(self, count: int) -> None:
            """
            Initialize a bucket storing items that share the same count.

            Args:
                count: The count shared by all items in this bucket.
            """
            self.count: int = count
            self.elements: Set[T] = set()
            self.next: Optional['StreamSummary[T].Bucket'] = None
            self.prev: Optional['StreamSummary[T].Bucket'] = None

    class Element:
        __slots__ = ['item', 'parent_bucket', 'overestimation']

        def __init__(self, item: T, overestimation: int = 0) -> None:
            """
            Metadata for a tracked item.

            Args:
                item: The item identifier (hashable).
                overestimation: The amount by which the item's count could be
                    overestimated due to capacity-constrained replacements.
            """
            self.item: T = item
            self.parent_bucket: Optional['StreamSummary[T].Bucket'] = None
            self.overestimation: int = overestimation

    def __init__(self, capacity: int) -> None:
        """
        Create a new StreamSummary.

        Args:
            capacity: Maximum number of items to track at once.
        """
        super().__init__(capacity=capacity)
        self.elements: Dict[T, 'StreamSummary[T].Element'] = {}
        self.buckets: Dict[int, 'StreamSummary[T].Bucket'] = {}
        self.min_bucket: Optional['StreamSummary[T].Bucket'] = None
        self.max_bucket: Optional['StreamSummary[T].Bucket'] = None
        self.max_overestimation: int = 0
        self.total_distinct_elements: int = 0

    def _insert_bucket_after(self, new_bucket: 'StreamSummary[T].Bucket', prev_bucket: Optional['StreamSummary[T].Bucket']) -> None:
        """
        Insert a bucket into the linked list right after `prev_bucket`.

        If `prev_bucket` is None, insert `new_bucket` at the head. Updates
        both head (min_bucket) and tail (max_bucket) pointers as needed.
        """
        if prev_bucket is None:
            new_bucket.next = self.min_bucket
            if self.min_bucket:
                self.min_bucket.prev = new_bucket
            self.min_bucket = new_bucket
            if self.max_bucket is None:
                self.max_bucket = new_bucket
        else:
            new_bucket.next = prev_bucket.next
            new_bucket.prev = prev_bucket
            if prev_bucket.next:
                prev_bucket.next.prev = new_bucket
            prev_bucket.next = new_bucket
            if prev_bucket == self.max_bucket:
                self.max_bucket = new_bucket

    def _remove_bucket(self, bucket: 'StreamSummary[T].Bucket') -> None:
        """
        Remove a bucket from the linked list and index.

        This method relinks neighbors, adjusts head/tail pointers,
        and deletes the bucket from the `buckets` map.
        """
        if bucket.prev:
            bucket.prev.next = bucket.next
        else:
            self.min_bucket = bucket.next
        if bucket.next:
            bucket.next.prev = bucket.prev
        else:
            self.max_bucket = bucket.prev
        del self.buckets[bucket.count]

    def _move_element(self, element: 'StreamSummary[T].Element', new_count: int) -> None:
        """
        Move an element to the bucket corresponding to `new_count`.

        Creates the target bucket if it does not exist, updates the element's
        parent bucket, and removes the old bucket if it becomes empty.
        """
        old_bucket = element.parent_bucket
        old_bucket.elements.remove(element.item)
        if new_count not in self.buckets:
            new_bucket = self.Bucket(new_count)
            self.buckets[new_count] = new_bucket
            prev = old_bucket
            curr = old_bucket.next
            while curr and curr.count < new_count:
                prev = curr
                curr = curr.next
            self._insert_bucket_after(new_bucket, prev)
        new_bucket = self.buckets[new_count]
        new_bucket.elements.add(element.item)
        element.parent_bucket = new_bucket
        if len(old_bucket.elements) == 0:
            self._remove_bucket(old_bucket)

    def insert(self, item: T) -> None:
        """
        Insert one occurrence of `item` into the sketch.

        If `item` is already tracked, its count increases by 1.
        Otherwise, if there is capacity available, `item` is added with count 1.
        If capacity is full, the current minimum-count item is replaced and
        `item` is inserted with count equal to that min-count + 1, and its
        overestimation set to the evicted min-count.
        """
        if item in self.elements:
            element = self.elements[item]
            self._move_element(element, element.parent_bucket.count + 1)
        else:
            if len(self.elements) < self.capacity:
                element = self.Element(item, overestimation=0)
                self.elements[item] = element
                if 1 not in self.buckets:
                    bucket = self.Bucket(1)
                    self.buckets[1] = bucket
                    self._insert_bucket_after(bucket, None)
                bucket = self.buckets[1]
                bucket.elements.add(item)
                element.parent_bucket = bucket
                self.total_distinct_elements += 1
            else:
                if not self.min_bucket:
                    return
                victim_item = next(iter(self.min_bucket.elements))
                del self.elements[victim_item]
                self.min_bucket.elements.remove(victim_item)
                new_count = self.min_bucket.count + 1
                overest = self.min_bucket.count
                element = self.Element(item, overestimation=overest)
                self.elements[item] = element
                self.max_overestimation = max(self.max_overestimation, overest)
                self.total_distinct_elements += 1
                if new_count not in self.buckets:
                    new_bucket = self.Bucket(new_count)
                    self.buckets[new_count] = new_bucket
                    self._insert_bucket_after(new_bucket, self.min_bucket)
                else:
                    new_bucket = self.buckets[new_count]
                new_bucket.elements.add(item)
                element.parent_bucket = new_bucket
                if len(self.min_bucket.elements) == 0:
                    self._remove_bucket(self.min_bucket)

    def insert_with_estimate(self, item: T, count: int, overestimation: int = 0) -> None:
        """
        Insert a new item with an explicit `count` and `overestimation`.

        Primarily used for merging partial sketches where external
        counts/overestimations are provided.
        """
        if item in self.elements:
            raise ValueError("insert_with_estimate expects item to be new.")
        element = self.Element(item, overestimation=overestimation)
        self.elements[item] = element
        self.max_overestimation = max(self.max_overestimation, overestimation)
        self.total_distinct_elements += 1
        if count not in self.buckets:
            bucket = self.Bucket(count)
            self.buckets[count] = bucket
            prev = None
            curr = self.min_bucket
            while curr and curr.count < count:
                prev = curr
                curr = curr.next
            self._insert_bucket_after(bucket, prev)
        else:
            bucket = self.buckets[count]
        bucket.elements.add(item)
        element.parent_bucket = bucket

    def merge_element(self, item: T, count: int, overestimation: int) -> None:
        """
        Merge a single external element into the sketch.

        If the item exists, its count and overestimation are incremented;
        otherwise it is inserted with the provided `count` and `overestimation`.
        """
        if item in self.elements:
            element = self.elements[item]
            new_count = element.parent_bucket.count + count
            new_overest = element.overestimation + overestimation
            self._move_element(element, new_count)
            element.overestimation = new_overest
            self.max_overestimation = max(self.max_overestimation, new_overest)
        else:
            self.insert_with_estimate(item, count, overestimation)

    def get_overestimation(self, item: T) -> int:
        """
        Return the overestimation currently associated with `item`.

        If `item` is not tracked, returns 0.
        """
        element = self.elements.get(item)
        return element.overestimation if element else 0

    def get_max_overestimation(self) -> int:
        """
        Return the maximum overestimation across all tracked items.
        """
        return self.max_overestimation

    def total_count(self) -> int:
        """
        Return the sum of counts represented by the sketch.

        This is computed as sum over buckets of (bucket.count * bucket.size).
        """
        return sum(bucket.count * len(bucket.elements) for bucket in self.buckets.values())

    def distinct_count(self) -> int:
        """
        Return the number of distinct items the sketch has seen.

        This increases when new items are first inserted, but does not decrease
        when items are evicted/replaced due to capacity constraints.
        """
        return self.total_distinct_elements

    def topk(self, k: Optional[int] = None) -> List[Tuple[T, int]]:
        """
        Return up to `k` items in descending count order.

        Returns tuples of (item, count). If `k` is None,
        returns all currently tracked items.
        """
        items: List[Tuple[T, int]] = []
        curr = self.max_bucket
        while curr:
            for item in curr.elements:
                items.append((item, curr.count))
            curr = curr.prev
        return items[:k] if k is not None else items

    def contains(self, item: T) -> bool:
        """
        Return True if `item` is currently tracked in the sketch, else False.
        """
        return item in self.elements


def aggregate_summaries(
    summaries: List[StreamSummary],
    capacity: int,
    n: Optional[int] = None
) -> Tuple[StreamSummary, Dict[str, Any]]:
    """
    Merge multiple StreamSummary sketches and compute telemetry required by Section 5.1.

    Args:
        summaries: List of StreamSummary sketches to merge.
        capacity: Capacity of the resulting merged sketch.
        n: Number of top items to identify. Defaults to capacity.

    Returns:
        merged: The merged StreamSummary.
        stats: Dictionary with telemetry:
            - 'N': total global count
            - 'item_stats': list of (item, f_hat, p_hat, p_floor, p_ceil, omega)
            - 'R_t': map of item -> set of reporting summary indices
            - 'top_n': top-n items by p_hat
            - 'C_t': candidate set (top-n ∪ challengers)
            - 'omega_min': minimum omega over C_t
            - 'tau_sp': 95% quantile of residual (1 - omega) over C_t
    """
    if n is None:
        n = capacity

    N = sum(summary.total_count() for summary in summaries)

    stats_map: Dict[Any, Tuple[int, int, int]] = {}  # item -> (f_hat, C, overestimation)
    R_t: Dict[Any, Set[int]] = {}

    for i, summary in enumerate(summaries):
        summary_total = summary.total_count()
        for item, element in summary.elements.items():
            count = element.parent_bucket.count
            f, c, o = stats_map.get(item, (0, 0, 0))
            stats_map[item] = (f + count, c + summary_total, o + element.overestimation)
            R_t.setdefault(item, set()).add(i)

    item_stats: List[Tuple[Any, int, float, float, float, float]] = []
    for item, (f_hat, C_i, over) in stats_map.items():
        p_hat = f_hat / N if N > 0 else 0
        omega = C_i / N if N > 0 else 0
        p_floor = p_hat - (over / N) if N > 0 else 0
        p_ceil = p_hat + ((N - C_i) / N) if N > 0 else 0
        item_stats.append((item, f_hat, p_hat, p_floor, p_ceil, omega))

    # top-n by p_hat
    top_n = sorted(item_stats, key=lambda x: (-x[2], repr(x[0])))[:n]
    top_n_keys = {x[0] for x in top_n}

    # C_t = top-n ∪ {k not in top-n where p_ceil > 1/n}
    C_t = top_n + [x for x in item_stats if x[0] not in top_n_keys and x[4] > 1 / n]

    # omega_min over C_t
    omega_min = min((x[5] for x in C_t), default=0.0)

    # tau_sp = 95% quantile of residuals (1 - omega) over C_t
    tau_sp = 0.0
    if C_t:
        residuals = sorted(1 - x[5] for x in C_t)
        sp_index = int(len(residuals) * 0.95)
        tau_sp = residuals[min(sp_index, len(residuals) - 1)]

    # Merge sketches
    merged = StreamSummary(capacity)
    for summary in summaries:
        for item, element in summary.elements.items():
            merged.merge_element(item, element.parent_bucket.count, element.overestimation)

    return merged, {
        'N': N,
        'item_stats': item_stats,
        'R_t': R_t,
        'top_n': top_n,
        'C_t': C_t,
        'omega_min': omega_min,
        'tau_sp': tau_sp
    }

if __name__ == "__main__":
    # === Stream 1 ===
    stream_main = ['a', 'b', 'a', 'c', 'b', 'a', 'd', 'e', 'a', 'b', 'x', 'f', 'g']
    ss = StreamSummary(capacity=5)
    for it in stream_main:
        ss.insert(it)

    print("=== Single Sketch Info ===")
    print("Stream 1:", stream_main)
    print("Total count (estimated):", ss.total_count())
    print("Distinct seen (tracked or replaced):", ss.distinct_count())
    print("Current top-3 (item, count):", ss.topk(3))
    print("Max overestimation:", ss.get_max_overestimation())
    print("Tracked items and overestimation:")
    for key in ['a', 'x']:
        print(f"  - {key}: contains={ss.contains(key)}, overestimation={ss.get_overestimation(key)}")

    # === Stream 2 ===
    stream_1 = ['a', 'b', 'a', 'z', 'z']
    ss1 = StreamSummary(capacity=4)
    for it in stream_1:
        ss1.insert(it)

    # === Stream 3 ===
    stream_2 = ['b', 'c', 'a', 'c', 'y']
    ss2 = StreamSummary(capacity=4)
    for it in stream_2:
        ss2.insert(it)

    # Merge and compute global stats
    merged, stats = aggregate_summaries([ss1, ss2], capacity=5, n=3)

    print("\n=== Merged Sketch Info ===")
    print("Stream 2:", stream_1)
    print("Stream 3:", stream_2)
    print("Merged sketch top-3 (item, count):", merged.topk(3))
    print("Merged total count:", merged.total_count())

    print("\n=== Global Telemetry ===")
    print(f"Global total count N: {stats['N']}")
    print(f"Minimum omega among candidates (omega_min): {stats['omega_min']:.4f}")
    print(f"Tau_sp (95% quantile of 1 - omega over C_t): {stats['tau_sp']:.4f}")

    print("\nTop-3 items by p_hat (item, f_hat, p_hat, p_floor, p_ceil, omega):")
    for row in stats['top_n']:
        print("  -", row)

    print("\nCandidate set C_t (top_n ∪ challengers):")
    for row in stats['C_t']:
        print("  -", row)

    print("\nReporting sets R_t (item → set of summaries):")
    for item, sources in stats['R_t'].items():
        print(f"  - {item}: {sources}")
