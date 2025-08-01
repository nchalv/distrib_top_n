import math

def compute_entropy(freqs: dict) -> float:
    """
    Compute Shannon entropy for a given probability distribution.
    """
    return -sum(p * math.log(p, 2) for p in freqs.values() if p > 0)


def normalize_entropy(entropy: float, num_elements: int) -> float:
    """
    Normalize entropy relative to the number of unique elements (max entropy = log2(n)).
    Returns value in [0, 1].
    """
    if num_elements <= 1:
        return 0.0
    return entropy / math.log(num_elements, 2)

