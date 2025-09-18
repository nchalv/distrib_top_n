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
            prev = None
            curr = self.min_bucket
            while curr and curr.count < new_count:
                prev = curr
                curr = curr.next
            self._insert_bucket_after(new_bucket, prev)
        else:
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
                else:
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


def aggregate_summaries(summaries: List[StreamSummary], capacity: int, n: Optional[int] = None) -> Tuple[StreamSummary, Dict[str, Any]]:
    """
    Merge multiple StreamSummary sketches and compute global statistics.

    Args:
        summaries: The worker sketches to merge.
        capacity: Capacity of the merged sketch to be produced.
        n: Number of top items (by global probability) to report. Defaults to `capacity`.

    Returns:
        A pair (merged, stats) where:
          - merged: StreamSummary containing the merged state.
          - stats: Dictionary with:
              N: Global total count across all workers.
              U: Union set of all keys seen in any worker.
              f_hat: Global estimated count per key (sum of per-worker estimates).
              p_hat: Global probability per key (f_hat / N).
              C: Coverage mass per key (sum of worker totals that include the key).
              omega: Coverage ratio per key (C / N).
              top_n: The top-n keys by p_hat (ties broken by repr(key) for determinism).
              omega_10p: The 10th percentile of omega among the top-n keys.
    """
    if n is None:
        n = capacity

    N: int = sum(summary.total_count() for summary in summaries)

    U: Set[T] = set()
    f_hat: Dict[T, int] = {}
    C: Dict[T, int] = {}

    for i, summary in enumerate(summaries):
        for bucket in summary.buckets.values():
            for item in bucket.elements:
                if item not in U:
                    U.add(item)
                    f_hat[item] = 0
                    C[item] = 0

    for i, summary in enumerate(summaries):
        N_i = summary.total_count()
        for bucket in summary.buckets.values():
            for item in bucket.elements:
                if item in summary.elements:
                    f_hat[item] += bucket.count
                    C[item] += N_i

    p_hat: Dict[T, float] = {}
    omega: Dict[T, float] = {}
    for item in U:
        p_hat[item] = f_hat[item] / N if N > 0 else 0
        omega[item] = C[item] / N if N > 0 else 0

    all_items: List[T] = list(U)
    all_items.sort(key=lambda k: (-p_hat[k], repr(k)))
    top_n: List[T] = all_items[:n]

    omega_values: List[float] = [omega[k] for k in top_n]
    omega_10p: float = 0
    if omega_values:
        sorted_omega: List[float] = sorted(omega_values)
        index_10p: int = max(0, min(len(sorted_omega) - 1, int(0.15 * len(sorted_omega))))
        omega_10p = sorted_omega[index_10p]

    merged: StreamSummary = StreamSummary(capacity)
    for summary in summaries:
        for bucket in summary.buckets.values():
            for item in bucket.elements:
                element = summary.elements[item]
                merged.merge_element(
                    item,
                    bucket.count,
                    element.overestimation
                )

    global_stats: Dict[str, Any] = {
        'N': N,
        'U': U,
        'f_hat': f_hat,
        'p_hat': p_hat,
        'C': C,
        'omega': omega,
        'top_n': top_n,
        'omega_10p': omega_10p
    }

    return merged, global_stats

if __name__ == "__main__":
    # Example usage of StreamSummary and aggregation

    # Create a sketch with capacity 5
    ss = StreamSummary(capacity=5)

    # Stream some items
    data_stream = ['a', 'b', 'a', 'c', 'b', 'a', 'd', 'e', 'a', 'b', 'x', 'f', 'g']
    for it in data_stream:
        ss.insert(it)

    # Query results
    print("Total count (estimated):", ss.total_count())
    print("Distinct seen (tracked or replaced):", ss.distinct_count())
    print("Current top-3 (item, count, overestimation):", ss.topk(3))
    print("Max overestimation:", ss.get_max_overestimation())

    # Demonstrate contains and per-item overestimation
    for key in ['a', 'x']:
        print(f"Contains '{key}':", ss.contains(key), "overestimation:", ss.get_overestimation(key))

    # Build two additional sketches and merge them
    ss1 = StreamSummary(capacity=4)
    for it in ['a', 'b', 'a', 'z', 'z']:
        ss1.insert(it)

    ss2 = StreamSummary(capacity=4)
    for it in ['b', 'c', 'a', 'c', 'y']:
        ss2.insert(it)

    merged, stats = aggregate_summaries([ss1, ss2], capacity=5, n=3)
    print("\nMerged sketch top-3 (item, count, overestimation):", merged.topk(3))
    print("Merged total count:", merged.total_count())

    # Global stats preview
    print("Global N:", stats['N'])
    print("Global top_n by probability:", stats['top_n'])
    print("omega_10p among top_n:", stats['omega_10p'])
