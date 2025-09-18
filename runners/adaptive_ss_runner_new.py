from sketches.StreamSummary import StreamSummary, aggregate_summaries
from runners.method_runner_base import MethodRunnerBase
from metrics.divergence import compute_jsd
#from policies.adaptive_policy import AdaptivePolicy
from pprint import pformat

class AdaptiveSSRunnerNew(MethodRunnerBase):
    def __init__(self, m, n, alpha, policy=None, verbose=False):
        self.m = m
        self.n = n
        self.q = n
        self.verbose = verbose
        self.sketch_class = StreamSummary
        self.stream_summaries = []
        self.counters = [[] for _ in range(self.m)]
        self.estimated_counts_and_freqs = {}
        self.prev_freqs = {}
        self.freqs = {}

    def initialize_sketches(self, window_id: int):
        self.stream_summaries = [self.sketch_class(capacity=self.q) for _ in range(self.m)]


    def insert_item(self, partition_id: int, item: str):
        if not _augment_tuple_count(self.counters[partition_id], item, 1):
            self.stream_summaries[partition_id].insert(item)

    def finalize_window(self, window_id: int) -> dict:
        agg, stats = aggregate_summaries(self.stream_summaries, self.n)
        agg_list = _consolidate_same_order_lists(self.counters)
        total = agg.total_count() + _sum_counts(agg_list)
        #total_distinct_elements = agg.distinct_count()
        sorted_items = sorted(agg.topk(), key=lambda x: -x[1])
        ssorted_items = _consolidate_lists(agg_list, sorted_items)
        template = [(item, 0, 0) for item, _, _ in ssorted_items[:int(self.n*1.5)]]
        ssorted_items = ssorted_items[:self.n]

        self.estimated_counts_and_freqs = {
            key: (count, count / total)
            for key, count, _ in ssorted_items
        }
        self.counters = [template[:] for _ in range(self.m)]




    def __str__(self) -> str:
        """Pretty-prints all attributes, excluding specified ones."""
        excluded_attrs = {'stream_summaries'}  # Customize this set
        attributes = {
            key: value
            for key, value in vars(self).items()
            if key not in excluded_attrs
        }
        # Indent nested structures for readability
        pretty_attrs = pformat(attributes, indent=2, width=80, depth=2)
        return f"{self.__class__.__name__}(\n{pretty_attrs}\n)"


def _augment_tuple_count(tuples_list, key, x):
    """
    Augment the count for a specific key in a list of tuples.

    Args:
        tuples_list: List[Tuple[Any, int]] - the list of (key, count) tuples
        key: Any - the key to search for
        x: int - the amount to add to the count

    Returns:
        bool: True if the key was found and updated, False otherwise
    """
    for i, (item_key, count, overestimation) in enumerate(tuples_list):
        if item_key == key:
            tuples_list[i] = (key, count + x, overestimation)
            return True
    return False


def _consolidate_same_order_lists(list_of_lists):
    """
    Consolidate multiple lists of tuples where all lists have the same items in the same order.

    Args:
        list_of_lists: List[List[Tuple[Any, int, int]]] where all inner lists have
                      the same items in the same order

    Returns:
        List[Tuple[Any, int, int]]: Consolidated list with summed counts and overestimations
    """
    if not list_of_lists:
        return []

    # Use the first list as reference for item order
    reference_list = list_of_lists[0]
    result = []

    # For each position, sum the counts and overestimations across all lists
    for i in range(len(reference_list)):
        item = reference_list[i][0]
        total_count = 0
        total_overest = 0

        for lst in list_of_lists:
            # Ensure all lists have the same item at this position
            if i < len(lst) and lst[i][0] == item:
                total_count += lst[i][1]
                total_overest += lst[i][2]
            else:
                # Handle mismatch - this shouldn't happen if order is guaranteed
                raise ValueError(f"Item mismatch at position {i}: expected {item}")

        result.append((item, total_count, total_overest))

    # The result will maintain the same order as the input lists
    return result



def _consolidate_lists(list1, list2):
    """
    Consolidate two lists of tuples (Any, int, int) and sort by the second element (count).
    """
    consolidated = {}

    # Process first list
    for item, count, overest in list1:
        if item in consolidated:
            consolidated[item][0] += count
            consolidated[item][1] += overest
        else:
            consolidated[item] = [count, overest]

    # Process second list
    for item, count, overest in list2:
        if item in consolidated:
            consolidated[item][0] += count
            consolidated[item][1] += overest
        else:
            consolidated[item] = [count, overest]

    # Convert to list and sort by count descending
    result = [(item, counts[0], counts[1]) for item, counts in consolidated.items()]
    result.sort(key=lambda x: (-x[1], x[0]))

    return result

def _sum_counts(tuples_list):
    """
    Sum all count values using list comprehension.
    """
    return sum([count for _, count, _ in tuples_list])
