from typing import TypeVar, Generic, Optional, Dict, List, Tuple, Iterable, Any, Set
from sketch_base import SketchBase           # ✅ your ABC
# Adjust these imports to your repo layout (same modules you showed earlier)
from ExactCounter import ExactCounter
from StreamSummary import StreamSummary

T = TypeVar('T')

class HybridSketch(SketchBase[T], Generic[T]):
    """
    HybridSketch: exact-on-a-known-domain + Space–Saving for the rest.

    This sketch composes:
    • ExactCounter(S): exact frequency counts for a predefined key set S (the "exact side-car").
    • StreamSummary(q): a Space–Saving sketch of capacity q that ingests only keys not in S.

    Routing invariant
    -----------------
    Every streamed item x is routed to exactly one component:
    • if x ∈ S  → ExactCounter
    • if x ∉ S  → StreamSummary
    Hence the two components see disjoint streams. The Hybrid tracks:
    N           : total items seen (hybrid-level counter; robust even when q == 0)
    routed_to_SS: number of items routed to the StreamSummary

    Per-key queries
    ---------------
    f(k):
    • if k ∈ S          → exact count (integer ≥ 0)
    • if k ∉ S and tracked by SS → Space–Saving counter for k (estimate)
    • otherwise         → None (k has not been tracked by SS)
    Note: for exact keys, zeros are retained (k listed with count 0 until first occurrence).

    Top-k
    -----
    topk(k) returns the union of:
    • all (k, count) for k ∈ S (including zeros),
    • all (k, count) for k currently tracked by SS,
    sorted by (count desc, repr(k)) for determinism.

    Residual mass bounds
    --------------------
    Let E^t be the exact domain (S) and S_i^t the set of SS-tracked keys at worker i.
    Space–Saving guarantees, for each tracked key k:
    f_i(k) ∈ [  max(0,  ĥf_i(k) − ε_i(k)),  ĥf_i(k)  ],
    with ε_i(k) ≤ N_i / q, where N_i is the number of items routed to SS at worker i.

    Define global accounted mass bounds for the hybrid:
    A_min =  ∑_{k∈E^t} f(k)  +  ∑_{k∈SS} max(0, count(k) − overestimation(k))
    A_max =  ∑_{k∈E^t} f(k)  +  routed_to_SS
    where “SS” iterates over all currently tracked SS keys in this hybrid instance.

    Then the residual (unaccounted) mass R satisfies:
    R_LB = max(0, N − A_max)  ≤  R  ≤  max(0, N − A_min) = R_UB.

    Notes:
    • With proper routing, A_max uses routed_to_SS (not just ∑ SS counters),
        ensuring R_LB = 0 even when q == 0 or a sketch drops inserts.
    • A_min tightens as q increases or streams shorten (smaller overestimation).

    Complexity (amortized)
    ----------------------
    insert(x)   : O(1) average (hash + SS update)
    topk(k)     : O(|S| + q + log(|S|+q)) due to sorting for output determinism
    total_count : O(1)

    This class implements SketchBase:
    - insert(item: T) -> None
    - topk(k: Optional[int]) -> List[Tuple[T, int]]
    - total_count() -> int
    and provides residuals() for (N, A_min, A_max, R_LB, R_UB) diagnostics.
    """


    def __init__(self, exact_keys: Iterable[T], q: int):
        self._exact_keys: Set[T] = set(exact_keys)
        self.exact = ExactCounter(self._exact_keys)
        self.ss = StreamSummary(capacity=q)

        # Hybrid-level accounting, robust even if the SS drops inserts (e.g., q==0)
        self._N_seen: int = 0            # total items seen by the hybrid
        self._N_routed_ss: int = 0       # total items routed to SS (out-of-domain)

        # Capacity semantics for the hybrid: exact domain size + SS capacity
        super().__init__(capacity=len(self._exact_keys) + q)

    # ---------- SketchBase required: insert ----------
    def insert(self, item: T) -> None:
        self._N_seen += 1
        if self.exact.contains(item):
            self.exact.insert(item)
        else:
            self._N_routed_ss += 1
            self.ss.insert(item)

    # Convenience bulk insert
    def extend(self, items: Iterable[T]) -> None:
        for x in items:
            self.insert(x)

    # ---------- Per-key helpers (optional API) ----------
    def get_count(self, item: T) -> Optional[int]:
        """
        f(k): exact if item in exact domain; SS estimate if currently tracked by SS; else None.
        (Zeros are kept for exact keys.)
        """
        if self.exact.contains(item):
            c = self.exact.get_count(item)  # int (>=0) for in-domain items
            return 0 if c is None else c
        e = self.ss.elements.get(item)
        return e.parent_bucket.count if e else None

    def contains(self, item: T) -> bool:
        return self.exact.contains(item) or (item in self.ss.elements)

    # ---------- SketchBase required: topk ----------
    def topk(self, k: Optional[int] = None) -> List[Tuple[T, int]]:
        """
        Top-k by reported f:
        - include ALL exact-domain keys (zeros included)
        - include all currently tracked SS keys
        Sorted by (count desc, repr(key)) for determinism.
        """
        items: List[Tuple[T, int]] = []

        # exact: include zeros
        items.extend((k_, c_) for k_, c_ in self.exact.counter.items())

        # SS: walk buckets max->min; deterministic order within buckets
        curr = self.ss.max_bucket
        while curr:
            for it in sorted(curr.elements, key=repr):
                items.append((it, curr.count))
            curr = curr.prev

        items.sort(key=lambda kv: (-kv[1], repr(kv[0])))
        return items if k is None else items[:k]

    # ---------- SketchBase required: total_count ----------
    def total_count(self) -> int:
        """
        N: total items seen by the hybrid (independent of SS internals/q).
        """
        return self._N_seen

    # ---------- Residual bounds (optional API) ----------
    def residuals(self) -> Dict[str, Any]:
        """
        Residual bounds using Space–Saving guarantees and routing.

        Returns:
          N        : total items seen
          A_min    : minimum plausible accounted mass (exact + sum max(c - eps, 0))
          A_max    : maximum plausible accounted mass (exact + routed_to_ss)
          R_LB     : lower bound on residual = max(0, N - A_max)  (== 0 with proper routing)
          R_UB     : upper bound on residual = max(0, N - A_min)
          extras   : exact_mass, ss_est_mass, ss_lb_mass, eps_max, tracked m, routed_to_ss
        """
        N = self.total_count()

        # exact side
        exact_mass = self.exact.total_count()

        # SS side
        m = len(self.ss.elements)
        ss_est_mass = self.ss.total_count()  # sum of SS counters (== routed when q>=1)
        ss_lb_mass = 0
        eps_max = 0
        for e in self.ss.elements.values():
            c = e.parent_bucket.count
            eps = e.overestimation
            ss_lb_mass += max(c - eps, 0)
            if eps > eps_max:
                eps_max = eps

        # bounds per derivation
        A_min = exact_mass + ss_lb_mass
        # IMPORTANT: A_max should use how many items were routed to SS, not just sum of counters
        A_max = exact_mass + self._N_routed_ss

        R_LB = max(0, N - A_max)  # with routing + correct N, this is exactly 0
        R_UB = max(0, N - A_min)

        return {
            "N": N,
            "A_min": A_min,
            "A_max": A_max,
            "R_LB": R_LB,
            "R_UB": R_UB,
            # diagnostics
            "exact_mass": exact_mass,
            "ss_tracked": m,
            "ss_est_mass": ss_est_mass,
            "ss_lb_mass": ss_lb_mass,
            "ss_eps_max": eps_max,
            "routed_to_ss": self._N_routed_ss,
        }

    # ---------- convenience ----------
    @property
    def exact_keys(self) -> Set[T]:
        return set(self._exact_keys)

    @property
    def ss_capacity(self) -> int:
        return self.ss.capacity
