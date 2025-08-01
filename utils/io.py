import gzip
import json
import pickle


def save_jsonl_gz(data, filepath):
    import numpy as np

    def to_serializable(obj):
        if isinstance(obj, np.generic):
            return obj.item()
        elif isinstance(obj, np.ndarray):
            return obj.tolist()
        elif isinstance(obj, dict):
            return {str(k): to_serializable(v) for k, v in obj.items()}
        elif isinstance(obj, (list, tuple, set)):
            return [to_serializable(i) for i in obj]
        elif isinstance(obj, (int, float, str, bool)) or obj is None:
            return obj
        else:
            # Final fallback
            return str(obj)

    with gzip.open(filepath, "wt", encoding="utf-8") as f:
        for item in data:
            serializable_item = to_serializable(item)
            json.dump(serializable_item, f)
            f.write("\n")




def load_jsonl_gz(filepath):
    with gzip.open(filepath, "rt", encoding="utf-8") as f:
        return [json.loads(line) for line in f]


def save_compressed_pickle(obj, filepath):
    with gzip.open(filepath, "wb") as f:
        pickle.dump(obj, f)


def load_compressed_pickle(filepath):
    with gzip.open(filepath, "rb") as f:
        return pickle.load(f)


def load_compressed_stream_summary(stream_file, summary_file):
    stream = load_compressed_pickle(stream_file)
    summary = load_compressed_pickle(summary_file)
    return stream, summary

