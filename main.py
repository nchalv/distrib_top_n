import os
from data.generate_data import prepare_and_store_data
from evaluation import evaluate_method
from runners.adaptive_ss_runner import AdaptiveSSRunner
from runners.adaptive_ss_runner_new import AdaptiveSSRunnerNew
from runners.static_ss_runner import StaticSSRunner
from sketches.stream_summary import StreamSummary
from utils.io import save_jsonl_gz

# === Experiment Parameters ===
SEED = 42

window_size = 10_000
num_keys = 1_000
n = 100
m=10
skewed_fraction = 0.8

q_min=n
alpha =.7
entropy_enabled=False
verbose = True

output_dir = "generated"
stream_file = os.path.join(output_dir, "stream.pkl.gz")
summary_file = os.path.join(output_dir, "summary.pkl.gz")
results_file = os.path.join(output_dir, "eval_adaptive_ss.jsonl.gz")

plot_distr = True
plot_skew = True
plot_data_config =  ('./plots/data', 'png')

# === Scenario Definition (Legacy Format, Fully Supported) ===

scenario = [
    {'type': 'uniform', 'duration': 5, 'params': {}, 'n': n},
    {'type': 'normal', 'duration': 5, 'params': {'n': n}, 'n': n,
        'transition': {'from': 'uniform', 'transition_windows': 10, 'from_params': {}, 'n': n}}#,
    # {'type': 'flattened', 'duration': 4, 'params': {'n': n, 'num_hh': 5}, 'n': n,
    #  'transition': {'from': 'normal', 'transition_windows': 10, 'from_params': {'n': n}, 'n': n}},
    # {'type': 'zipfian', 'duration': 3, 'params': {'s': 1.5}, 'n': n,
    #  'transition': {'from': 'flattened', 'transition_windows': 10, 'from_params': {'n': n, 'num_hh': 5}, 'n': n}},
    # {'type': 'zipfian', 'duration': 2, 'params': {'s': 2.0}, 'n': n,
    #  'transition': {'from': 'zipfian', 'transition_windows': 5, 'from_params': {'s': 1.5}, 'n': n}},
    # {'type': 'normal', 'duration': 2, 'params': {'n': n}, 'n': n,
    #  'transition': {'from': 'zipfian', 'transition_windows': 15, 'from_params': {'s': 2.0}, 'n': n}},
]

# === Step 1: Generate Stream + Ground Truth Summary ===
# total_items = window_size * sum(step["duration"] + step.get("transition", {}).get("transition_windows", 0)
#                                 for step in scenario)

#prepare_and_store_data(SEED, window_size, num_keys, n, m, scenario, skewed_fraction, stream_file, summary_file, plot_distr, plot_data_config)








# === Step 2: Instantiate Runner for Adaptive SpaceSaving ===
runner = AdaptiveSSRunner(
   n=n,
   m=m,
   alpha=0.5
)

# runner = AdaptiveSSRunnerNew(
#    n=n,
#    m=m,
#    alpha=0.5
# )
# runner = StaticSSRunner(
#     n=n,
#     m=m,
#     verbose = False
# )

if isinstance(runner, AdaptiveSSRunner):
    method_name = "adaptive_ss"
elif isinstance(runner, StaticSSRunner):
    method_name = "static_ss"
elif isinstance(runner, AdaptiveSSRunnerNew):
    method_name = "static_ss_new"

# === Step 3: Evaluate Method ===
results = evaluate_method(
    method_name=method_name,
    runner=runner,
    stream_file=stream_file,
    summary_file=summary_file,
    n=n,
    m=m,
    window_size=window_size,
    entropy_threshold=1.0,
    verbose=True,
    plot_est=True
)

# # === Step 4: Save Results ===
# save_jsonl_gz(results, results_file)
# print(f"✅ Evaluation complete. Results saved to: {results_file}")
