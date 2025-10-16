import math
from typing import List, Dict, Any
from data.data_utils import load_compressed_pickle
from metrics.entropy import compute_entropy, normalize_entropy
from metrics.metric_utils import compute_topn_metrics
from runners.method_runner_base import MethodRunnerBase
from runners.adaptive_ss_runner import AdaptiveSSRunner
from runners.adaptive_ss_runner_new import AdaptiveSSRunnerNew
from runners.static_ss_runner import StaticSSRunner
from typing import Dict, List, Tuple
from pathlib import Path
import gzip
import pickle
from sketches.stream_summary import StreamSummary, aggregate_summaries
from visualisation.result_visualiser import plot_actual_vs_estimated


def evaluate_method(
    method_name: str,
    runner: MethodRunnerBase,
    stream_file: str,
    summary_file: str,
    n: int,
    m: int,
    window_size: int,
    entropy_threshold: float = 1.0,
    verbose: bool = True,
    plot_est: bool = True
) -> List[Dict[str, Any]]:
    """
    Evaluate a sketch-based method across a stream of windows.

    Args:
        method_name: Identifier for the method (e.g., 'adaptive_ss').
        runner: An instance of a class implementing MethodRunner.
        stream_file: Path to compressed stream file.
        summary_file: Path to partitioned frequency summary file.
        n: Number of downstream nodes.
        m: Number of upstream partitions.
        window_size: Items per window.
        entropy_threshold: Skip evaluation if normalized entropy exceeds this.
        verbose: Whether to print progress.
    Returns:
        List of dicts with evaluation metrics for each window.
    """

    def load_compressed_stream_summary(filepath: str) -> List[Tuple[str, Dict[str, int], int]]:
        """
        Read list of (string, dict, int) tuples from a gzipped pickle file

        """
        if not Path(filepath).exists():
            raise FileNotFoundError(f"File {filepath} not found")

        try:
            with gzip.open(filepath, 'rb') as f:
                data = pickle.load(f)
                # Validate the loaded data
                if not isinstance(data, list):
                    raise ValueError("Loaded data is not a list")
                for item in data:
                    if not (isinstance(item, tuple) and len(item) == 3):
                        raise ValueError("Items must be 3-element tuples")
                    if not (isinstance(item[0], str) and
                        isinstance(item[1], dict) and
                        isinstance(item[2], int)):
                        raise ValueError("Tuple format must be (str, dict, int)")

                return data
        except pickle.UnpicklingError as e:
            print(f"Failed to unpickle {filepath}: {str(e)}")
            raise
        except Exception as e:
            print(f"Unexpected error reading {filepath}: {str(e)}")
            raise

    def load_compressed_stream(filepath: str) -> Dict[int, Dict[int, List[str]]]:
        """Safe loading with error handling"""
        print(filepath)
        if not Path(filepath).exists():
            raise FileNotFoundError(f"File {filepath} not found")

        try:
            with gzip.open(filepath, 'rb') as f:
                return pickle.load(f)
        except Exception as e:
            print(f"Error loading {filepath}: {e}")
            raise


    try:
        stream_data = load_compressed_stream(stream_file)
        stream_summ = load_compressed_stream_summary(summary_file)
    except Exception as e:
        print(f"Failed to process stream data: {e}")

    results = []
    estimated_freqs_prev = {}

###TODO:
    for window, partitions in stream_data.items():
        runner.initialize_sketches(window_id=window)
        print(f"\nWindow {window+1} Summary:")
        total_events = sum(len(stream) for stream in partitions.values())
        # print(f"Total partitions: {len(partitions)}")
        print(f"Total events: {total_events}")

        for partition_num, stream in partitions.items():
            # print(f"  Adding {len(stream)} events to partition {partition_num}'s StreamSummary")
            # Add each element to its corresponding StreamSummary
            for element in stream:
                runner.insert_item(partition_num, element)

        runner.finalize_window(window)
        estimated_top_n = sorted([
            (k, f, p) for k, (f, p) in runner.estimated_counts_and_freqs.items() if p > 1 / n
        ], key=lambda x: -x[1])[:n]

        window_size = sum(stream_summ[window][1].values())
        actual_freqs = {
            key: (count, count / window_size)
            for key, count in stream_summ[window][1].items()
        }
        actual_top_n = sorted([
            (k, f, p) for k, (f, p) in actual_freqs.items() if p > 1 / n
        ], key=lambda x: -x[1])[:n]

        if isinstance(runner, AdaptiveSSRunner):
            desc = "AdaptiveSS"
        elif isinstance(runner, StaticSSRunner):
            desc = "StaticSS"
        elif isinstance(runner, AdaptiveSSRunnerNew):
            desc = "AdaptiveSSNew"

        if (actual_top_n or estimated_top_n) and plot_est:
            plot_actual_vs_estimated(actual_top_n, actual_freqs, estimated_top_n, window, int((1/n)*window_size), n, desc)


        #print(runner)

# ###TODO
#     for w, (stream_window, summary_window) in enumerate(zip(stream_data, summary_data)):
#         if verbose:
#             print(f"� Evaluating window {w + 1}/{len(stream_data)}")
#
#         # Compute actual frequencies
#         actual_counts = {}
#         total_actual = 0
#         print(summary_window)
#         for partition in summary_window:
#             for key, count in partition.items():
#                 actual_counts[key] = actual_counts.get(key, 0) + count
#                 total_actual += count
#         actual_freqs = {k: v / total_actual for k, v in actual_counts.items()}
#
#         # Run sketching method
#         runner.initialize_sketches(window_id=w)
#         for partition_id, stream in enumerate(stream_window):
#             runner.insert_batch(partition_id, stream)
#         result = runner.finalize_window(window_id=w)
#
#         # Estimated frequencies
#         estimated_total = result.get("total", sum(count for _, count in result.get("items", [])))
#         estimated_counts_and_freqs = {
#             k: (count, count / estimated_total)
#             for k, count in result.get("items", [])
#         }
#         estimated_freqs = {
#             k: rel for k, (_, rel) in estimated_counts_and_freqs.items()
#         }
#
#         # Entropy filtering
#         # entropy = result.get("entropy", compute_entropy(estimated_freqs))
#         # norm_entropy = result.get("norm_entropy", normalize_entropy(entropy, len(estimated_freqs)))
#         # if norm_entropy > entropy_threshold:
#         #     if verbose:
#         #         print(f"⚠️ Skipping window {w+1} due to high normalized entropy: {norm_entropy:.4f}")
#         #     skipped = {
#         #         "window": w + 1,
#         #         "distribution": "skipped",
#         #         "actual_top_n": [],
#         #         "estimated_top_n": [],
#         #         "precision": None,
#         #         "recall": None,
#         #         "f1_score": None,
#         #         "avg_absolute_error": None,
#         #         "avg_relative_error": None,
#         #         "rmse": None,
#         #     }
#         #
#         #     for field in ["entropy", "norm_entropy", "q_est", "L", "L_t"]:
#         #         if field in result:
#         #             skipped[field] = result[field]
#         #     if "q_est" in result:
#         #         skipped["partition_memory"] = m * result["q_est"]
#         #         skipped["aggregator_memory"] = n
#         #         skipped["communication_cost"] = m * result["q_est"]
#         #
#         #     results.append(skipped)
#         #     estimated_freqs_prev = {}
#         #     continue
#
#         # Compute top-n metrics
#         actual_top_n = sorted([
#             (k, count, actual_freqs[k])
#             for k, count in actual_counts.items()
#             if actual_freqs[k] > 1 / n
#         ], key=lambda x: -x[1])[:n]
#
#         estimated_top_n = sorted([
#             (k, count, estimated_freqs[k])
#             for k, (count, _) in estimated_counts_and_freqs.items()
#             if estimated_freqs[k] > 1 / n
#         ], key=lambda x: -x[1])[:n]
#
#         metrics = compute_topn_metrics(actual_top_n, estimated_top_n, actual_freqs)
#
#         result_entry = {
#             "window": w + 1,
#             "distribution": "valid",
#             "actual_top_n": actual_top_n,
#             "estimated_top_n": estimated_top_n,
#             **metrics
#         }
#
#         for field in ["entropy", "norm_entropy", "q_est", "L", "L_t"]:
#             if field in result:
#                 result_entry[field] = result[field]
#         if "q_est" in result:
#             result_entry["partition_memory"] = m * result["q_est"]
#             result_entry["aggregator_memory"] = n
#             result_entry["communication_cost"] = m * result["q_est"]
#
#         results.append(result_entry)
#         estimated_freqs_prev = estimated_freqs.copy()

    return ""
