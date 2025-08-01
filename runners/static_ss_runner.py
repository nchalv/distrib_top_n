from sketches.StreamSummary import StreamSummary, aggregate_summaries
from runners.method_runner_base import MethodRunnerBase

class StaticSSRunner(MethodRunnerBase):
    def __init__(self, m, n, verbose=False):
        self.m = m
        self.n = n
        self.q = n
        self.verbose = verbose
        self.stream_summaries = []
        self.estimated_counts_and_freqs = {}
        self.sketch_class = StreamSummary



    def initialize_sketches(self, window_id: int):
        self.stream_summaries = [self.sketch_class(capacity=self.q) for _ in range(self.m)]

    def insert_item(self, partition_id: int, item: str):
        self.stream_summaries[partition_id].insert(item)


    def finalize_window(self, window_id: int) -> dict:
        agg = aggregate_summaries(self.stream_summaries, self.n)
        total = agg.total_count()
        sorted_items = sorted(agg.topk(), key=lambda x: -x[1])
        self.estimated_counts_and_freqs = {
            key: (count, count / total)
            for key, count in sorted_items
        }


    def __str__(self) -> str:
        """Pretty-prints all attributes, excluding specified ones."""
        excluded_attrs = {'verbose', 'stream_summaries', 'estimated_counts_and_freqs'}  # Customize this set
        attributes = {
            key: value
            for key, value in vars(self).items()
            if key not in excluded_attrs
        }
        # Indent nested structures for readability
        pretty_attrs = pformat(attributes, indent=2, width=80, depth=2)
        return f"{self.__class__.__name__}(\n{pretty_attrs}\n)"
