import matplotlib.pyplot as plt
import matplotlib.font_manager as fm
import matplotlib.patches as mpatches

def plot_actual_vs_estimated(actual_top_n, actual_freqs, estimated_top_n, window, threshold_value, n, desc):
    # Set Roboto as the default font
    try:
        if any('Roboto' in f.name for f in fm.fontManager.ttflist):
            plt.rcParams['font.family'] = 'Roboto'
        else:
            roboto_url = "https://github.com/google/fonts/raw/main/apache/roboto/Roboto-Regular.ttf"
            roboto_path = fm.urlretrieve(roboto_url, "Roboto-Regular.ttf")[0]
            roboto_prop = fm.FontProperties(fname=roboto_path)
            plt.rcParams['font.family'] = roboto_prop.get_name()
    except:
        print("Roboto not found - falling back to default font")

    # Prepare data
    combined_keys = list({k for k, _, _ in actual_top_n} | {k for k, _, _ in estimated_top_n})
    actual_freq_dict = {k: f for k, f, _ in actual_top_n}
    estimated_freq_dict = {k: f for k, f, _ in estimated_top_n}

    # Fill missing actuals
    for k in combined_keys:
        if k not in actual_freq_dict and k in actual_freqs:
            actual_freq_dict[k] = actual_freqs[k][0]

    # Sort keys by actual frequency
    sorted_keys = sorted(combined_keys, key=lambda k: actual_freq_dict.get(k, 0), reverse=True)
    actual_vals = [actual_freq_dict.get(k, 0) for k in sorted_keys]
    estimated_vals = [estimated_freq_dict.get(k, 0) for k in sorted_keys]

    # Create figure with modern styling
    plt.figure(figsize=(14, 7), facecolor='white')
    ax = plt.gca()
    ax.set_facecolor('#f5f6f7')

    # Set bar width and positions
    width = 0.4
    x = range(len(sorted_keys))

    # Determine colors for actual bars based on threshold
    actual_colors = ['#e74c3c' if val > threshold_value else '#95a5a6' for val in actual_vals]
    actual_edge_colors = ['#c0392b' if val >= threshold_value else '#7f8c8d' for val in actual_vals]

    # Plot bars with conditional coloring
    actual_bars = ax.bar(x, actual_vals, width,
                        color=actual_colors, edgecolor=actual_edge_colors,
                        linewidth=0.5, alpha=0.9, label='Actual')

    estimated_bars = ax.bar([i + width for i in x], estimated_vals, width,
                           color='#3498db', edgecolor='#2980b9',
                           linewidth=0.5, alpha=0.9, label='Estimated')

    # Add grid and remove top/right spines
    ax.yaxis.grid(True, linestyle='--', linewidth=0.5, alpha=0.7)
    for spine in ['top', 'right']:
        ax.spines[spine].set_visible(False)

    # Set dynamic y-axis limit
    max_val = max(max(actual_vals) if actual_vals else 0,
                 max(estimated_vals) if estimated_vals else 0)
    y_lim_top = int((max_val * 1.15) // 10 + 1) * 10
    ax.set_ylim(0, y_lim_top)

    # Draw threshold line if visible
    threshold_in_view = 0 < threshold_value <= y_lim_top
    if threshold_in_view:
        ax.axhline(y=threshold_value, color='#e74c3c', linestyle='--', linewidth=1, alpha=0.7)
        ax.annotate(
            f'1/{n} threshold = {int(threshold_value):,}',
            xy=(len(sorted_keys)*0.9, threshold_value),
            xytext=(20, 20),
            textcoords='offset points',
            color='#e74c3c',
            ha='center',
            va='bottom',
            arrowprops=dict(arrowstyle="->", color='#e74c3c', alpha=0.7),
            bbox=dict(boxstyle='round,pad=0.5', fc='white', alpha=0.9)
        )

    # Labels and title with consistent styling
    ax.set_ylabel('Absolute Frequency (f)', fontsize=10, labelpad=10)
    ax.set_title(f'Window {window + 1} - Actual vs Estimated Top-N Frequencies',
                fontsize=14, pad=20, fontweight='bold')

    # X-ticks with rotation if many keys
    ax.set_xticks([i + width/2 for i in x])
    if len(sorted_keys) > 20:
        ax.set_xticklabels(sorted_keys, rotation=90, fontsize=8, ha='center')
    else:
        ax.set_xticklabels(sorted_keys, fontsize=9)

    # Add data values on top of bars if there's space
    if len(sorted_keys) <= 15:
        for bars in [actual_bars, estimated_bars]:
            for bar in bars:
                height = bar.get_height()
                if height > 0:
                    ax.text(bar.get_x() + bar.get_width()/2., height,
                            f'{int(height):,}',
                            ha='center', va='bottom', fontsize=8)

    # Create custom legend entries
    hh_patch = mpatches.Patch(color='#e74c3c', label=f'Actual ≥ 1/{n} threshold - HH')
    other_patch = mpatches.Patch(color='#95a5a6', label=f'Actual < 1/{n} threshold - non-HH')
    estimated_patch = mpatches.Patch(color='#3498db', label='Estimated')

    ax.legend(handles=[hh_patch, other_patch, estimated_patch],
             loc="upper right", framealpha=1, facecolor='white')

    plt.tight_layout()
    #plt.show()
    plt.savefig('./plots/'+f"w {window+1} {desc}".lower().replace(" ", "_")[:15])
    plt.close
