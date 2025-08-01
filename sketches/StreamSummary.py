from sketches.sketch_base import SketchBase

class StreamSummary:
    class Bucket:
        __slots__ = ['count', 'elements', 'next', 'prev']
        def __init__(self, count):
            self.count = count
            self.elements = set()
            self.next = None
            self.prev = None

    class Element:
        __slots__ = ['item', 'parent_bucket', 'overestimation']
        def __init__(self, item, overestimation=0):
            self.item = item
            self.parent_bucket = None
            self.overestimation = overestimation

        # [Bucket list operations: _insert_bucket_after, _remove_bucket, _move_element]
        # [Core methods: insert, insert_with_estimate, get_overestimation, etc.]
        # ... (retain existing bucket/element management logic) ...

    def merge_element(self, item, count, overestimation):
        """Efficiently merge an element during aggregation"""
        if item in self.elements:
            element = self.elements[item]
            new_count = element.parent_bucket.count + count
            new_overest = element.overestimation + overestimation
            self._move_element(element, new_count)
            element.overestimation = new_overest
        else:
            self.insert_with_estimate(item, count, overestimation)

    def __init__(self, capacity):
        self.capacity = capacity
        self.elements = {}          # item -> Element
        self.buckets = {}           # count -> Bucket
        self.min_bucket = None
        self.max_bucket = None
        self.max_overestimation = 0

    def _insert_bucket_after(self, new_bucket, prev_bucket):
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

    def _remove_bucket(self, bucket):
        if bucket.prev:
            bucket.prev.next = bucket.next
        else:
            self.min_bucket = bucket.next
        if bucket.next:
            bucket.next.prev = bucket.prev
        else:
            self.max_bucket = bucket.prev
        del self.buckets[bucket.count]

    def _move_element(self, element, new_count):
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

    def insert(self, item):
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
            else:
                if not self.min_bucket:
                    return
                victim_item = next(iter(self.min_bucket.elements))
                victim_element = self.elements[victim_item]
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

    def insert_with_estimate(self, item, count, overestimation=0):
        """Insert a key with an explicit count and overestimation."""
        if item in self.elements:
            raise ValueError("insert_with_estimate expects item to be new.")
        element = self.Element(item, overestimation=overestimation)
        self.elements[item] = element
        self.max_overestimation = max(self.max_overestimation, overestimation)

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

    def get_overestimation(self, item):
        element = self.elements.get(item)
        return element.overestimation if element else 0

    def get_max_overestimation(self):
        return self.max_overestimation

    def total_count(self):
        return sum(bucket.count * len(bucket.elements) for bucket in self.buckets.values())

    def topk(self, k=None):
        items = []
        curr = self.max_bucket
        while curr:
            for item in curr.elements:
                items.append((item, curr.count))
            curr = curr.prev
        return items[:k] if k is not None else items


def aggregate_summaries(summaries, capacity):
    """Merge worker sketches using direct bucket/element iteration (O(Σ|buckets|))"""
    merged = StreamSummary(capacity)
    for summary in summaries:
        for bucket in summary.buckets.values():
            for item in bucket.elements:
                element = summary.elements[item]
                merged.merge_element(
                    item,
                    bucket.count,
                    element.overestimation
                )
    return merged
