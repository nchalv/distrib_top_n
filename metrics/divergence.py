import numpy as np
from scipy.spatial.distance import jensenshannon

def compute_jsd(p: dict, q: dict, base: float = 2.0) -> float:
    """
    Compute Jensen–Shannon divergence between two probability distributions.
    Returns JSD^2 to represent divergence (bounded in [0, 1]).
    """
    all_keys = set(p.keys()) | set(q.keys())
    p_vec = np.array([p.get(k, 0.0) for k in all_keys])
    q_vec = np.array([q.get(k, 0.0) for k in all_keys])

    p_sum = p_vec.sum()
    q_sum = q_vec.sum()

    if p_sum > 0:
        p_vec /= p_sum
    if q_sum > 0:
        q_vec /= q_sum

    return float(jensenshannon(p_vec, q_vec, base=base) ** 2)


def compute_spatial_divergence(stream_summaries, global_freqs) -> float:
    """
    Compute the maximum JSD between local partition distributions and global distribution.
    """
    divergences = []
    for ss in stream_summaries:
        total = ss.total_count()
        local_dist = {
            k: ss.elements[k].parent_bucket.count / total
            for k in ss.elements
        } if total > 0 else {}
        divergences.append(compute_jsd(local_dist, global_freqs))
    return max(divergences, default=0.0)


def compute_temporal_divergence(prev_freqs, curr_freqs, alpha=0.5, prev_L_t=0.0):
    """
    Compute temporal divergence (JSD) between previous and current global distributions,
    smoothed with exponential moving average using alpha.
    """
    jsd = compute_jsd(prev_freqs, curr_freqs)
    return alpha * prev_L_t + (1 - alpha) * jsd

