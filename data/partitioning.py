import random
from typing import List, Dict
import math


def assign_partitions(
    freq_dist,
    num_partitions,
    top_n=100,
    skewed_fraction=0.5,
    skew_ratio=0.75,
    skew_jitter=0.15
):
    """
    For the top-n keys, partitions each key's frequency between partitions:
      - Uniform: assigned equally to all partitions.
      - Skewed: θ (≈70%±20%) assigned to a small partition subset, rest diffused.
    Returns: {partition_id: {key: assigned_count, ...}, ...}
    """
    keys_sorted = sorted(freq_dist.items(), key=lambda x: x[1], reverse=True)
    top_keys = [k for k, _ in keys_sorted[:top_n]]
    partitioned = {p: {} for p in range(num_partitions)}

    for k in top_keys:
        freq = freq_dist[k]
        is_skewed = random.random() < skewed_fraction
        if is_skewed:
            skew_part_count = random.randint(
                math.ceil(num_partitions / 6), math.floor(num_partitions / 3)
            )
            skew_partitions = random.sample(range(num_partitions), skew_part_count)
            rest_partitions = [p for p in range(num_partitions) if p not in skew_partitions]

            theta = random.uniform(skew_ratio - skew_jitter, skew_ratio + skew_jitter)
            skew_mass = int(round(freq * theta))
            rest_mass = freq - skew_mass

            # Distribute skewed mass nearly evenly
            for i, p in enumerate(skew_partitions):
                share = skew_mass // skew_part_count + (
                    1 if i < skew_mass % skew_part_count else 0
                )
                if share > 0:
                    partitioned[p][k] = partitioned[p].get(k, 0) + share

            # Randomly assign remaining mass in other partitions
            targets = rest_partitions if rest_partitions else skew_partitions
            for i in range(rest_mass):
                p = random.choice(targets)
                partitioned[p][k] = partitioned[p].get(k, 0) + 1
        else:
            # Uniform: round robin partitioning
            base = freq // num_partitions
            remainder = freq % num_partitions
            for i in range(num_partitions):
                share = base + (1 if i < remainder else 0)
                if share > 0:
                    partitioned[i][k] = partitioned[i].get(k, 0) + share

    # Assign all other keys uniformly
    for k, freq in freq_dist.items():
        if k in top_keys:
            continue
        base = freq // num_partitions
        remainder = freq % num_partitions
        for i in range(num_partitions):
            share = base + (1 if i < remainder else 0)
            if share > 0:
                partitioned[i][k] = partitioned[i].get(k, 0) + share

    return partitioned

