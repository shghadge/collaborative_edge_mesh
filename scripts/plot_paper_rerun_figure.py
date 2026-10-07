#!/usr/bin/env python3
"""Draw the revised AIBThings Fig. 3 from the rerun's summary.csv.

Top row: convergence time; bottom row: network overhead. One column per
partition split. Points are means over 10 runs; error bars are the sample
standard deviation.
"""

import argparse
import csv
from pathlib import Path

import matplotlib

matplotlib.use("Agg")
import matplotlib.pyplot as plt  # noqa: E402
from run_paper_rerun import OUT_DIR  # noqa: E402

SUMMARY = OUT_DIR / "summary.csv"

METHODS = {
    "full_state": ("Full State", "#d62728", "o"),
    "optimized_full_state": ("Optimized Full State", "#17becf", "s"),
    "naive_key_delta": ("Naive Key-Delta", "#ff7f0e", "^"),
    "true_delta": ("True Delta-State", "#2ca02c", "D"),
}
SPLITS = ("50/50", "70/30", "80/20")
NODES = (5, 10, 15)
OFFSETS = {
    "full_state": -0.45,
    "optimized_full_state": -0.15,
    "naive_key_delta": 0.15,
    "true_delta": 0.45,
}


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__.splitlines()[0])
    parser.add_argument("--out", type=Path, default=OUT_DIR / "fig3_rerun")
    args = parser.parse_args()

    data = {}
    with SUMMARY.open(newline="", encoding="utf-8") as handle:
        for row in csv.DictReader(handle):
            data[(int(row["nodes"]), row["split"], row["method"])] = row

    plt.rcParams.update({"font.family": "serif", "font.size": 8})
    fig, axes = plt.subplots(2, 3, figsize=(7.16, 4.4), sharey="row")
    rows = (
        ("conv_mean_s", "conv_sd_s", "Convergence Time", "Time (seconds)"),
        (
            "sync_mean_mb",
            "sync_sd_mb",
            "Network Overhead",
            "Data Transmitted (megabytes)",
        ),
    )
    for r, (mean_key, sd_key, title, ylabel) in enumerate(rows):
        for c, split in enumerate(SPLITS):
            ax = axes[r][c]
            for method, (label, color, marker) in METHODS.items():
                xs = [n + OFFSETS[method] for n in NODES]
                means = [float(data[(n, split, method)][mean_key]) for n in NODES]
                sds = [float(data[(n, split, method)][sd_key]) for n in NODES]
                ax.errorbar(
                    xs,
                    means,
                    yerr=sds,
                    label=label,
                    color=color,
                    marker=marker,
                    markersize=4,
                    linewidth=1.2,
                    elinewidth=0.8,
                    capsize=2.5,
                )
            ax.set_title(f"{title} ({split} Split)", fontsize=8.5, fontweight="bold")
            ax.set_xticks(NODES)
            ax.set_xlim(3.5, 16.5)
            top = max(
                float(data[(n, s, m)][mean_key]) + float(data[(n, s, m)][sd_key])
                for n in NODES
                for s in SPLITS
                for m in METHODS
            )
            ax.set_ylim(0, top * 1.05)
            ax.grid(True, linewidth=0.4, alpha=0.5)
            if r == 1:
                ax.set_xlabel("Number of Nodes")
            if c == 0:
                ax.set_ylabel(ylabel)

    handles, labels = axes[0][0].get_legend_handles_labels()
    fig.legend(
        handles,
        labels,
        loc="upper center",
        ncol=4,
        frameon=False,
        bbox_to_anchor=(0.5, 1.0),
    )
    fig.tight_layout(rect=(0, 0, 1, 0.95))
    args.out.parent.mkdir(parents=True, exist_ok=True)
    path = args.out.with_suffix(".png")
    fig.savefig(path, dpi=300, bbox_inches="tight")
    print(path)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
