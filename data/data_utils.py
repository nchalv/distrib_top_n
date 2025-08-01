import gzip
import pickle
from typing import Any


def save_compressed_pickle(obj: Any, filepath: str) -> None:
    """
    Save a Python object to a compressed .pkl.gz file.
    """
    with gzip.open(filepath, "wb") as f:
        pickle.dump(obj, f, protocol=pickle.HIGHEST_PROTOCOL)


def load_compressed_pickle(filepath: str) -> Any:
    """
    Load a Python object from a compressed .pkl.gz file.
    """
    with gzip.open(filepath, "rb") as f:
        return pickle.load(f)

