import random
from collections import defaultdict
import hashlib

class LDSketch:
    def __init__(self, r, w, T):
        self.r = r
        self.w = w
        self.T = T
        self.V = [[0 for _ in range(w)] for _ in range(r)]  # Total values
        self.A = [[{} for _ in range(w)] for _ in range(r)]  # Associative arrays
        self.L = [[1 for _ in range(w)] for _ in range(r)]  # Max length li,j
        self.E = [[0 for _ in range(w)] for _ in range(r)]   # Estimation error
        self.hash_functions = [self._make_hash(i) for i in range(r)]

    def _make_hash(self, seed):
        def h(x):
            hval = hashlib.sha256((str(seed) + str(x)).encode()).hexdigest()
            return int(hval, 16) % self.w
        return h

    def update(self, key, value):
        for i in range(self.r):
            j = self.hash_functions[i](key)
            self._update_bucket(key, value, i, j)

    def _update_bucket(self, x, vx, i, j):
        self.V[i][j] += vx
        Ai = self.A[i][j]
        li = self.L[i][j]
        ei = self.E[i][j]

        if x in Ai:
            Ai[x] += vx
            return

        if len(Ai) < li:
            Ai[x] = vx
            return

        k = self.V[i][j] // self.T
        required_li = (k + 1) * (k + 2) - 1

        if li < required_li:
            self.L[i][j] = required_li
            Ai[x] = vx
            return

        # Eviction step
        e_hat = min(vx, min(Ai.values()))
        self.E[i][j] += e_hat

        # Subtract e_hat from all
        for y in list(Ai):
            Ai[y] -= e_hat
            if Ai[y] <= 0:
                del Ai[y]

        if vx > e_hat:
            Ai[x] = vx - e_hat
            self.L[i][j] += 1
    #
    # def estimate(self, key):
    #     ests = []
    #     for i in range(self.r):
    #         j = self.hash_functions[i](key)
    #         Ai = self.A[i][j]
    #         ei = self.E[i][j]
    #         ests.append(Ai.get(key, 0) + ei)
    #     return min(ests)

    def estimate(self, key):
    lower_bounds = []
    upper_bounds = []
    for i in range(self.r):
        j = self.hash_functions[i](key)
        Ai = self.A[i][j]
        ei = self.E[i][j]
        low = Ai.get(key, 0)
        upper = low + ei
        lower_bounds.append(low)
        upper_bounds.append(upper)
    return (min(lower_bounds) + min(upper_bounds)) / 2

    def heavy_hitters(self, threshold):
        """Return keys with estimated count ≥ threshold."""
        candidates = set()
        for row in self.A:
            for bucket in row:
                candidates.update(bucket.keys())
        return [(key, self.estimate(key)) for key in candidates if self.estimate(key) >= threshold]
