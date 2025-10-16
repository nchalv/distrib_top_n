from sketches.stream_summary import StreamSummary, aggregate_summaries
from runners.method_runner_base import MethodRunnerBase
from metrics.divergence import compute_jsd
#from policies.adaptive_policy import AdaptivePolicy
from pprint import pformat

class AdaptiveSSRunner(MethodRunnerBase):
    def __init__(self, m, n, alpha, policy=None, verbose=False):
        self.m = m
        self.n = n
        self.q = n
        self.alpha = alpha
        self.verbose = verbose
        self.sketch_class = StreamSummary
        self.stream_summaries = []
        self.estimated_counts_and_freqs = {}
        self.prev_freqs = {}
        self.freqs = {}
        self.L_prev = 0.0
        self.L_t = 0.0
        self.L = 0.0

    def initialize_sketches(self, window_id: int):
        self.stream_summaries = [self.sketch_class(capacity=self.q) for _ in range(self.m)]


    def insert_item(self, partition_id: int, item: str):
        self.stream_summaries[partition_id].insert(item)

    def finalize_window(self, window_id: int) -> dict:
        self.L_prev = self.L_t
        self.prev_freqs = self.freqs
        agg, stats = aggregate_summaries(self.stream_summaries, self.n*self.m, self.n)
        total = agg.total_count()
        sorted_items = sorted(agg.topk(), key=lambda x: -x[1])
        self.estimated_counts_and_freqs = {
            key: (count, count / total)
            for key, count in sorted_items
        }
        top_g = stats["top_g"]
        #print(stats['item_stats'])
        omega_min = min(stats['item_stats'], key=lambda x: x[5])[5]
        print(omega_min)
        # print("top_g = "+str(top_g))
        # print("top_n = "+str(stats["top_n"]))
        r=0.15
        print("q_new = "+str((self.n/r)*(2-omega_min)))
        self._update_L()
        self._update_L_t()
        self._update_q()
        print("q = "+str(self.q))



    def _update_L(self):
        worker_divergences = []
        for ss in self.stream_summaries:
            total = ss.total_count()
            local_dist = {
                k: ss.elements[k].parent_bucket.count / total
                for k in ss.elements
            } if total > 0 else {}
            estimated_relative_freqs = {
                key: rel for key, (_, rel) in self.estimated_counts_and_freqs.items()
            }
            jsd_value = compute_jsd(local_dist, estimated_relative_freqs)
            worker_divergences.append(jsd_value)
        self.L = max(worker_divergences, default=0)

    def _update_L_t(self):
        self.L_t = self.alpha*self.L_prev+(1-alpha)*compute_jsd(self.prev_freqs, self.freqs) if self.prev_freqs else 0.0

    def _update_q(self):
        self.q = self.n * (1 + self.L + self.L_t)

    def summarize():
        return(f"q = {self.q}, L = {self.L}, L_t = {self.L_t}")

    def __str__(self) -> str:
        """Pretty-prints all attributes, excluding specified ones."""
        excluded_attrs = {'L_prev', 'stream_summaries', 'prev_freqs', 'freqs'}  # Customize this set
        attributes = {
            key: value
            for key, value in vars(self).items()
            if key not in excluded_attrs
        }
        # Indent nested structures for readability
        pretty_attrs = pformat(attributes, indent=2, width=80, depth=2)
        return f"{self.__class__.__name__}(\n{pretty_attrs}\n)"
