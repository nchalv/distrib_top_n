import math

def compute_precision_recall_f1(actual_set, estimated_set):
    true_positives = len(actual_set & estimated_set)
    false_positives = len(estimated_set - actual_set)
    false_negatives = len(actual_set - estimated_set)

    precision = true_positives / (true_positives + false_positives) if (true_positives + false_positives) else 0.0
    recall    = true_positives / (true_positives + false_negatives) if (true_positives + false_negatives) else 0.0
    f1        = 2 * precision * recall / (precision + recall) if (precision + recall) else 0.0

    return precision, recall, f1


def compute_avg_absolute_error(actual_dict, estimated_dict):
    keys = set(actual_dict)
    abs_errors = [
        abs(estimated_dict.get(k, 0.0) - actual_dict.get(k, 0.0))
        for k in keys
    ]
    return sum(abs_errors) / len(abs_errors) if abs_errors else 0.0


def compute_avg_relative_error(actual_dict, estimated_dict):
    rel_errors = [
        abs(estimated_dict[k] - actual_dict[k]) / actual_dict[k]
        for k in actual_dict if k in estimated_dict and actual_dict[k] > 0
    ]
    return sum(rel_errors) / len(rel_errors) if rel_errors else 0.0


def compute_rmse(actual_dict, estimated_dict):
    common_keys = set(actual_dict) & set(estimated_dict)
    sq_errors = [
        (estimated_dict[k] - actual_dict[k]) ** 2
        for k in common_keys
    ]
    return math.sqrt(sum(sq_errors) / len(sq_errors)) if sq_errors else 0.0


def compute_topn_metrics(actual_top_n, estimated_top_n, actual_freqs):
    actual_dict = {k: p for k, _, p in actual_top_n}
    estimated_dict = {k: p for k, _, p in estimated_top_n}

    actual_set = set(actual_dict)
    estimated_set = set(estimated_dict)

    precision, recall, f1 = compute_precision_recall_f1(actual_set, estimated_set)
    avg_abs_error = compute_avg_absolute_error(actual_dict, estimated_dict)
    avg_rel_error = compute_avg_relative_error(actual_dict, estimated_dict)
    rmse = compute_rmse(actual_dict, estimated_dict)

    return {
        "precision": precision,
        "recall": recall,
        "f1_score": f1,
        "avg_absolute_error": avg_abs_error,
        "avg_relative_error": avg_rel_error,
        "rmse": rmse,
    }

