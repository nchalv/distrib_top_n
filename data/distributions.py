import numpy as np
from abc import ABC, abstractmethod
import random
from typing import Dict


# === Generator Interface ===

class FrequencyDistributionGenerator(ABC):
    @abstractmethod
    def generate(self, total_items: int, num_keys: int):
        pass

# === Distribution Generators ===

class UniformDistributionGenerator(FrequencyDistributionGenerator):
    def generate(self, total_items: int, num_keys: int):
        base = total_items // num_keys
        remainder = total_items % num_keys
        freqs = [base + 1 if i < remainder else base for i in range(num_keys)]
        return {f'key_{i+1}': freqs[i] for i in range(num_keys)}

class NormalDistributionGenerator(FrequencyDistributionGenerator):
    def __init__(self, n): self.n = n
    def generate(self, total_items: int, num_keys: int):
        center = 0.05 * num_keys
        std = 0.031 * num_keys
        x = np.linspace(0, num_keys - 1, num_keys)
        freqs = np.exp(-0.5 * ((x - center) / std) ** 2)
        freqs = freqs / freqs.sum() * total_items
        freqs = np.floor(freqs).astype(int)
        for i in range(total_items - freqs.sum()):
            freqs[i % num_keys] += 1
        return {f'key_{i+1}': freq for i, freq in enumerate(freqs)}

class ZipfianDistributionGenerator(FrequencyDistributionGenerator):
    def __init__(self, s=1.2): self.s = s
    def generate(self, total_items: int, num_keys: int):
        ranks = np.arange(1, num_keys + 1)
        weights = 1 / ranks**self.s
        weights /= weights.sum()
        freqs = weights * total_items
        freqs = np.floor(freqs).astype(int)
        for i in range(total_items - freqs.sum()):
            freqs[i % num_keys] += 1
        return {f'key_{i+1}': freq for i, freq in enumerate(freqs)}

class FlattenedHHDistributionGenerator(FrequencyDistributionGenerator):
    def __init__(self, n=10, num_hh=5, flatness=0.5):
        self.n = n; self.num_hh = num_hh; self.flatness = flatness

    def _generate_flattened_hh_distribution(self, n, total_items, num_hh, flatness):
        if num_hh > n - 1: num_hh = n - 1
        min_hh = total_items // n + 1
        max_possible = 2 * (total_items - 1) / num_hh - min_hh
        max_hh = int(min_hh + flatness * (max_possible - min_hh))
        hh_items = [max(max_hh - i * (max_hh - min_hh) // max(num_hh - 1, 1), min_hh) for i in range(num_hh)]
        total_hh = sum(hh_items)
        non_hh_items = total_items - total_hh
        return hh_items, non_hh_items

    def _generate_smooth_linear_non_hh(self, non_hh, min_hh, min_freq=1):
        top = min_hh - 1
        k = 0
        while True:
            k += 1
            seq = np.floor(np.linspace(top, min_freq, k)).astype(int)
            seq = seq[seq > 0]
            if seq.sum() > non_hh:
                k -= 1
                break
        seq = np.floor(np.linspace(top, min_freq, k)).astype(int)
        seq = seq[seq > 0]
        seq[0] = min(seq[0], top)
        diff = non_hh - seq.sum()
        idx = 1
        while diff != 0:
            if diff > 0:
                seq[idx] += 1
                diff -= 1
            elif diff < 0 and seq[idx] > min_freq:
                seq[idx] -= 1
                diff += 1
            idx = (idx + 1) % len(seq)
        return seq.tolist()

    def generate(self, total_items: int, num_keys: int):
        hh, non_hh_total = self._generate_flattened_hh_distribution(self.n, total_items, self.num_hh, self.flatness)
        if not hh: return {f'key_{i+1}': non_hh_total}
        min_hh = min(hh)
        non_hh_freqs = self._generate_smooth_linear_non_hh(non_hh_total, min_hh)
        all_freqs = hh + non_hh_freqs
        np.random.shuffle(all_freqs)
        return {f'key_{i+1}': f for i, f in enumerate(all_freqs)}
