import argparse
from typing import List, Dict, Any
import numpy as np
from scipy.optimize import linear_sum_assignment
import random
from collections import defaultdict


from visualisation.data_visualiser import (
    plot_frequency_distribution_with_hh,
    plot_key_partition_distribution,
    plot_partition_skew
)

from data.distributions import (
    ZipfianDistributionGenerator,
    UniformDistributionGenerator,
    NormalDistributionGenerator,
    FlattenedHHDistributionGenerator
)
from data.partitioning import assign_partitions
from data.data_utils import save_compressed_pickle


# === Scenario Runner ===

def run_scenario(scenario, total_items, num_keys):
    gens = {
        'uniform': UniformDistributionGenerator,
        'normal': NormalDistributionGenerator,
        'flattened': FlattenedHHDistributionGenerator,
        'zipfian': ZipfianDistributionGenerator
    }

    def interpolate_distributions_smoothly(dist_a, dist_b, steps):
        keys = set(dist_a).union(dist_b)
        windows = []
        for t in range(1, steps + 1):
            alpha = t / (steps + 1)
            blended = {k: int(round((1 - alpha) * dist_a.get(k, 0) + alpha * dist_b.get(k, 0)))
                    for k in keys}
            windows.append({k: v for k, v in blended.items() if v > 0})
        return windows

    output = []
    num_windows = 0
    for step in scenario:
        G, params = gens[step['type']], step.get('params', {})
        gen = G(**params) if params else G()
        win_n = step.get('n', 10)
        dur = step['duration']
        trans = step.get('transition', {})
        if trans:
            G_from = gens[trans['from']]
            gen_from = G_from(**trans['from_params'])
            dist_a = gen_from.generate(total_items, num_keys)
            dist_b = gen.generate(total_items, num_keys)
            intermediate = interpolate_distributions_smoothly(dist_a, dist_b, trans['transition_windows'])
            for t_dist in intermediate:
                output.append(('transition', t_dist, win_n))
                num_windows += 1

        for _ in range(dur):
            output.append((step['type'], gen.generate(total_items, num_keys), win_n))
            num_windows += 1

    return output, num_windows

def smooth_key_transitions(windows):
    result = []
    prev_keys = None
    prev_freqs = None
    for idx, (dtype, freq_dict, n) in enumerate(windows):
        curr_keys = list(freq_dict)
        curr_freqs = np.array([freq_dict[k] for k in curr_keys])
        if idx == 0:
            result.append((dtype, {f'key_{i+1}': curr_freqs[i] for i in range(len(curr_keys))}, n))
            prev_keys = [f'key_{i+1}' for i in range(len(curr_keys))]
            prev_freqs = curr_freqs
            continue
        size = max(len(prev_freqs), len(curr_freqs))
        A = np.pad(prev_freqs, (0, size - len(prev_freqs)))
        B = np.pad(curr_freqs, (0, size - len(curr_freqs)))
        cost = np.abs(A[:, None] - B[None, :])
        r, c = linear_sum_assignment(cost)
        remap = {}
        used = set()
        for i, j in zip(r, c):
            if i < len(prev_keys) and j < len(curr_keys):
                remap[prev_keys[i]] = curr_freqs[j]
                used.add(j)
        next_id = max([int(k.split('_')[1]) for k in remap] + [0]) + 1
        for j in range(len(curr_keys)):
            if j not in used:
                remap[f'key_{next_id}'] = curr_freqs[j]
                next_id += 1
        prev_keys = list(remap)
        prev_freqs = np.array([remap[k] for k in prev_keys])
        result.append((dtype, remap, n))
    return result


def reconstruct_streams(
    windowed_data: Dict[int, Dict[int, Dict[str, int]]],
    seed: int = 42,
    preserve_window_order: bool = True
) -> Dict[int, Dict[int, List[str]]]:
    """
    Reconstructs streams for multiple windows and partitions with reproducible ordering.

    Args:
        windowed_data: Nested dictionary {window: {partition: {key: frequency}}}
        seed: Random seed for reproducibility
        preserve_window_order: If True, maintains window sequence in shuffling

    Returns:
        Dictionary {window: {partition: [ordered_keys]}}
    """
    random.seed(seed)
    reconstructed = defaultdict(dict)

    # Determine window processing order
    windows = sorted(windowed_data.keys()) if preserve_window_order else windowed_data.keys()

    for window in windows:
        partitioned_data = windowed_data[window]

        for partition, key_counts in partitioned_data.items():
            # Create list with keys repeated by their frequencies
            stream = []
            for key, count in key_counts.items():
                stream.extend([key] * count)

            # Shuffle while maintaining reproducibility
            random.shuffle(stream)
            reconstructed[window][partition] = stream

    return dict(reconstructed)



def prepare_and_store_data(seed=42, total_items=10_000, num_keys = 1_000, n = 100, m=10,
        scenario= [],
        skewed_fraction = 0.5,
        path = 'stream_data.pkl.gz',
        summ_path = 'summ_data.pkl.gz',
        plot_distr = True,
        save_info: tuple[str | None, str | None] = ('./plots/data', 'png')
    ):

    if scenario == []:
        scenario = {'type': 'uniform', 'duration': 2, 'params': {}, 'n': n}
    raw_windows, num_windows = run_scenario(scenario, total_items, num_keys)
    smoothed_windows = smooth_key_transitions(raw_windows)
    stream = {w: {} for w in range(num_windows)}
    save_compressed_pickle(smoothed_windows, summ_path)

    for idx, (dist_type, freq_dist, win_n) in enumerate(smoothed_windows):
        if plot_distr:
            plot_frequency_distribution_with_hh(freq_dist, win_n, save_info, f"Window {idx+1}: {dist_type.capitalize()}")

        partitioned_window = assign_partitions(
            freq_dist,
            num_partitions=m,
            top_n=win_n,
            skewed_fraction=skewed_fraction
        )
        stream[idx] = partitioned_window
        if plot_distr:
            plot_partition_skew(partitioned_window, f"Window {idx+1}", save_info)
            plot_key_partition_distribution('key_947', partitioned_window, m, f"Window {idx+1}", save_info)

    save_compressed_pickle(reconstruct_streams(stream, seed), path)



# === Main Script ===
if __name__ == "__main__":
    SEED = 42

    total_items = 10_000
    num_keys = 1_000
    n = 100
    m=10
    skewed_fraction = 0.5

    scenario = [
        {'type': 'uniform', 'duration': 2, 'params': {}, 'n': n},
        {'type': 'normal', 'duration': 4, 'params': {'n': n}, 'n': n,
         'transition': {'from': 'uniform', 'transition_windows': 4, 'from_params': {}, 'n': n}},
        {'type': 'flattened', 'duration': 4, 'params': {'n': n, 'num_hh': 5}, 'n': n,
         'transition': {'from': 'normal', 'transition_windows': 10, 'from_params': {'n': n}, 'n': n}},
        {'type': 'zipfian', 'duration': 3, 'params': {'s': 1.5}, 'n': n,
         'transition': {'from': 'flattened', 'transition_windows': 10, 'from_params': {'n': n, 'num_hh': 5}, 'n': n}},
        {'type': 'zipfian', 'duration': 2, 'params': {'s': 2.0}, 'n': n,
         'transition': {'from': 'zipfian', 'transition_windows': 5, 'from_params': {'s': 1.5}, 'n': n}},
        {'type': 'normal', 'duration': 2, 'params': {'n': n}, 'n': n,
         'transition': {'from': 'zipfian', 'transition_windows': 15, 'from_params': {'s': 2.0}, 'n': n}},
    ]
    prepare_and_store_data(SEED, total_items, num_keys, n, m, scenario, skewed_fraction, data_path = 'stream_data.pkl.gz', summ_path = 'summ_data.pkl.gz')
