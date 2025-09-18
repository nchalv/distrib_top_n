from typing import TypeVar, Generic, Optional, Dict, Set, List, Tuple, Any
from sketch_base import SketchBase
from ExactCounter import ExactCounter
from StreamSummary import StreamSummary

T = TypeVar("T")


class CombinedAggregatedSketch(SketchBase[T], Generic[T]):
    """
    Aggregates an already-aggregated ExactCounter and an already-aggregated StreamSummary
    into a unified, queryable sketch with combined capacity and statistics.

    Assumptions:
      - The exact and approximate components hold disjoint key sets.
      - Exact component has no overestimation and per-item coverage omega = 1.0.

    Provided stats:
      - N: global total count across both components
      - U: union of all keys
      - f_hat: estimated counts per key
      - p_hat: estimated probabilities per key (f_hat / N)
      - omega: coverage ratio per key (1.0 for exact keys; optional for approx keys if provided)
      - top_n: keys with highest p_hat (deterministic tie-breaking by repr(key))
      - omega_10p: 10th percentile of omega across top_n keys (using available omega values)
    """

    def __init__(
        self,
        exact: ExactCounter[T],
        summary: StreamSummary[T],
        n: Optional[int] = None,
        summary_stats: Optional[Dict[str, Any]] = None,
    ) -> None:
        # Combined capacity equals sum of the two ingredients
        super().__init__(capacity=exact.size + summary.capacity)

        self.exact = exact
        self.summary = summary
        self.n = n or (exact.size + summary.capacity)

        # Build unified counts map (f_hat)
        self.f_hat: Dict[T, int] = {}

        # Exact counts (no overestimation)
        for k, v in exact.counter.items():
            # Only include keys with nonzero capacity domain; counts may be zero
            self.f_hat[k] = int(v)

        # Approximate counts from StreamSummary
        for item, count in summary.topk(None):
            # Respect disjointness assumption: exact and approx keys are disjoint
            self.f_hat[item] = int(count)

        # Total count across both components
        self.N: int = sum(self.f_hat.values())

        # Probability estimates
        self.p_hat: Dict[T, float] = {k: (self.f_hat[k] / self.N) if self.N > 0 else 0.0 for k in self.f_hat}

        # Coverage estimates
        self.omega: Dict[T, Optional[float]] = {}

        # Exact items have full coverage
        for k in exact.keys:
            self.omega[k] = 1.0

        # Approximate items coverage:
        # If stats from the StreamSummary aggregation were provided and include omega,
        # use them; otherwise, leave as None.
        approx_omega: Optional[Dict[T, float]] = None
        if summary_stats and isinstance(summary_stats.get("omega"), dict):
            approx_omega = summary_stats["omega"]  # type: ignore[assignment]

        for item, _ in summary.topk(None):
            if approx_omega is not None and item in approx_omega:
                self.omega[item] = float(approx_omega[item])
            else:
                self.omega[item] = None

        # Union of keys
        self.U: Set[T] = set(self.f_hat.keys())

        # Determine top-n by probability (stable tie-breaker by repr(item))
        all_items: List[T] = list(self.U)
        all_items.sort(key=lambda k: (-self.p_hat[k], repr(k)))
        self.top_n: List[T] = all_items[: self.n]

        # Compute 10th percentile of omega among top-n (ignoring None)
        omega_values: List[float] = [self.omega[k] for k in self.top_n if self.omega.get(k) is not None]  # type: ignore[list-item]
        self.omega_10p: float = 0.0
        if omega_values:
            omega_values_sorted = sorted(omega_values)
            idx = max(0, min(len(omega_values_sorted) - 1, int(0.10 * len(omega_values_sorted))))
            self.omega_10p = omega_values_sorted[idx]

    # SketchBase interface

    def insert(self, item: T) -> None:
        # This combined sketch represents an aggregation result and is immutable by design.
        # If needed, route inserts based on membership. For now, disallow mutation.
        raise NotImplementedError("CombinedAggregatedSketch is immutable; insert is not supported.")

    def topk(self, k: Optional[int] = None) -> List[Tuple[T, int]]:
        # Return globally top-k by f_hat
        items = sorted(self.f_hat.items(), key=lambda kv: kv[1], reverse=True)
        return items if k is None else items[:k]

    def total_count(self) -> int:
        return self.N

    # Convenience accessors

    def get_stats(self) -> Dict[str, Any]:
        return {
            "N": self.N,
            "U": self.U,
            "f_hat": self.f_hat,
            "p_hat": self.p_hat,
            "omega": self.omega,
            "top_n": self.top_n,
            "omega_10p": self.omega_10p,
        }


if __name__ == "__main__":
    # Example usage of CombinedAggregatedSketch
    from typing import Set

    # 1) Build an "aggregated" exact counter (domain-limited, disjoint from summary keys)
    exact_keys: Set[str] = {"a", "b", "c"}
    exact = ExactCounter[str](keys=exact_keys)
    for it in ["a", "a", "b", "a", "c", "b", "a"]:
        exact.insert(it)

    # 2) Build an "aggregated" stream summary (approximate)
    summary = StreamSummary[str](capacity=5)
    # Use items disjoint from the exact counter's domain
    for it in ["x", "y", "x", "z", "y", "x", "y", "y", "w"]:
        summary.insert(it)

    # 3) Create the combined aggregated sketch
    combined = CombinedAggregatedSketch(exact=exact, summary=summary, n=5)

    # 4) Inspect core stats and queries
    stats = combined.get_stats()
    print("Combined capacity:", combined.capacity)
    print("Total N:", stats["N"])
    print("Union U:", stats["U"])
    print("Top-5 by count:", combined.topk(5))
    print("p_hat (first 5):", list(stats["p_hat"].items())[:5])
    print("omega (coverage) sample:", {k: stats["omega"][k] for k in list(stats["omega"].keys())[:5]})
    print("top_n by probability:", stats["top_n"])
    print("omega_10p among top_n:", stats["omega_10p"])
