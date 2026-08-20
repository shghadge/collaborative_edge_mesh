#!/usr/bin/env python3
import argparse
from pathlib import Path

import matplotlib

matplotlib.use("Agg")
import matplotlib.pyplot as plt
import pandas as pd

METHOD_LABELS = {
    "full_state": "Full-state anti-entropy",
    "true_delta": "Hybrid delta anti-entropy",
    "centralized": "Centralized baseline",
}
COLORS = {
    "full_state": "#4472C4",
    "true_delta": "#ED7D31",
    "centralized": "#70AD47",
}


def _load(results_dir: Path) -> pd.DataFrame:
    paths = sorted(results_dir.glob("*/*/eval-*.csv"))
    if not paths:
        raise SystemExit("no evaluation CSV files found")
    frame = pd.concat((pd.read_csv(path) for path in paths), ignore_index=True)
    numeric = [
        "node_count",
        "write_success_rate",
        "write_latency_p95_ms",
        "convergence_time_s",
        "bytes_per_accepted_event",
        "convergence_wait_seconds",
        "target_offered_rate_events_s",
        "accepted_throughput_events_s",
    ]
    for field in numeric:
        frame[field] = pd.to_numeric(frame.get(field), errors="coerce")
    converged = (
        frame["converged_within_timeout"]
        .astype(str)
        .str.lower()
        .map({"true": 1.0, "false": 0.0})
    )
    over_budget = (
        converged.eq(1.0)
        & frame["convergence_time_s"].notna()
        & frame["convergence_wait_seconds"].notna()
        & frame["convergence_time_s"].gt(frame["convergence_wait_seconds"])
    )
    frame.loc[over_budget, "convergence_time_s"] = float("nan")
    frame["converged_flag"] = converged.mask(over_budget, 0.0)
    return frame


def _save(fig: plt.Figure, path: Path) -> None:
    fig.tight_layout()
    fig.savefig(path, dpi=300, bbox_inches="tight")
    fig.savefig(path.with_suffix(".pdf"), bbox_inches="tight")
    plt.close(fig)


def _method_lines(
    ax: plt.Axes,
    frame: pd.DataFrame,
    metric: str,
    ylabel: str,
    *,
    log_scale: bool = False,
) -> None:
    for method in ("full_state", "true_delta", "centralized"):
        selected = frame[frame["sync_mode"] == method]
        if selected.empty:
            continue
        grouped = selected.groupby("node_count")[metric]
        median = grouped.median()
        q1 = grouped.quantile(0.25)
        q3 = grouped.quantile(0.75)
        ax.errorbar(
            median.index,
            median.values,
            yerr=[median.values - q1.values, q3.values - median.values],
            marker="o",
            capsize=3,
            label=METHOD_LABELS[method],
            color=COLORS[method],
        )
    ax.set_xlabel("Edge nodes")
    ax.set_ylabel(ylabel)
    ax.grid(axis="y", alpha=0.25)
    if log_scale:
        ax.set_yscale("log")
        positive = pd.to_numeric(frame[metric], errors="coerce")
        positive = positive[positive > 0]
        if not positive.empty:
            ax.set_ylim(bottom=float(positive.min()) * 0.7)
    handles, labels = ax.get_legend_handles_labels()
    if handles:
        ax.legend(handles, labels, frameon=False)


def _availability(frame: pd.DataFrame, out_dir: Path) -> None:
    selected = frame[
        (frame["scenario"] == "partition_heal")
        & (frame["network_profile"] == "hard_partition")
        & (frame["partition_split"] == "50/50")
        & (frame["event_id_retention_limit"] == 100)
        & (frame["delta_log_limit"] == 1000)
        & (frame["docker_cpus"] == 1)
        & (frame["docker_memory"] == "512m")
    ]
    fig, ax = plt.subplots(figsize=(7.2, 4.2))
    _method_lines(ax, selected, "write_success_rate", "Accepted write fraction")
    ax.set_ylim(0, 1.05)
    ax.set_title("Write availability during partition and healing")
    _save(fig, out_dir / "availability_partition.png")


def _convergence(frame: pd.DataFrame, out_dir: Path) -> None:
    selected = frame[
        (frame["scenario"] == "partition_heal")
        & (frame["network_profile"] == "hard_partition")
        & (frame["sync_mode"].isin(["full_state", "true_delta"]))
        & (frame["partition_split"] == "50/50")
        & (frame["event_id_retention_limit"] == 100)
        & (frame["delta_log_limit"] == 1000)
        & (frame["docker_cpus"] == 1)
        & (frame["docker_memory"] == "512m")
    ]
    fig, axes = plt.subplots(1, 2, figsize=(10.5, 4.0))
    for method in ("full_state", "true_delta"):
        method_rows = selected[selected["sync_mode"] == method]
        success = method_rows.groupby("node_count")["converged_flag"].mean()
        axes[0].plot(
            success.index,
            success.values,
            marker="o",
            label=METHOD_LABELS[method],
            color=COLORS[method],
        )
    axes[0].set_xlabel("Edge nodes")
    axes[0].set_ylabel("Converged-run fraction")
    axes[0].set_ylim(-0.05, 1.05)
    axes[0].set_title("Combined convergence success")
    axes[0].grid(axis="y", alpha=0.25)
    axes[0].legend(frameon=False)
    _method_lines(
        axes[1],
        selected,
        "convergence_time_s",
        "Observer convergence (s)",
    )
    axes[1].set_title("Time among converged runs")
    _save(fig, out_dir / "convergence_by_scale.png")


def _communication(frame: pd.DataFrame, out_dir: Path) -> None:
    selected = frame[
        (frame["scenario"].isin(["baseline", "partition_heal"]))
        & (frame["network_profile"] == "hard_partition")
        & ((frame["scenario"] == "baseline") | (frame["partition_split"] == "50/50"))
        & (frame["event_id_retention_limit"] == 100)
        & (frame["delta_log_limit"] == 1000)
        & (frame["docker_cpus"] == 1)
        & (frame["docker_memory"] == "512m")
    ]
    fig, axes = plt.subplots(1, 2, figsize=(10.5, 4.0), sharey=True)
    for ax, scenario, title in (
        (axes[0], "baseline", "Clean network"),
        (axes[1], "partition_heal", "Partition and healing"),
    ):
        _method_lines(
            ax,
            selected[selected["scenario"] == scenario],
            "bytes_per_accepted_event",
            "Protocol bytes per accepted event",
            log_scale=True,
        )
        ax.set_title(title)
    axes[1].set_ylabel("")
    _save(fig, out_dir / "communication_cost.png")


def _load_curve(frame: pd.DataFrame, out_dir: Path) -> None:
    selected = frame[
        (frame["scenario"] == "high_load")
        & frame["target_offered_rate_events_s"].notna()
    ]
    fig, axes = plt.subplots(1, 2, figsize=(10.5, 4.0))
    for nodes in sorted(selected["node_count"].dropna().unique()):
        node_rows = selected[selected["node_count"] == nodes]
        grouped = node_rows.groupby("target_offered_rate_events_s")
        throughput = grouped["accepted_throughput_events_s"].median()
        latency = grouped["write_latency_p95_ms"].median()
        label = f"{int(nodes)} nodes"
        axes[0].plot(throughput.index, throughput.values, marker="o", label=label)
        axes[1].plot(latency.index, latency.values, marker="o", label=label)
    axes[0].plot([0, 100], [0, 100], linestyle="--", color="0.5", label="Ideal")
    axes[0].set_xlabel("Target offered rate (events/s)")
    axes[0].set_ylabel("Accepted throughput (events/s)")
    axes[0].set_title("Throughput response")
    axes[1].set_xlabel("Target offered rate (events/s)")
    axes[1].set_ylabel("p95 accepted-write latency (ms)")
    axes[1].set_title("Latency response")
    for ax in axes:
        ax.grid(alpha=0.25)
        handles, labels = ax.get_legend_handles_labels()
        if handles:
            ax.legend(handles, labels, frameon=False)
    _save(fig, out_dir / "load_response.png")


def main() -> int:
    parser = argparse.ArgumentParser(description="Plot reviewer experiment results.")
    parser.add_argument("results_dir", type=Path)
    parser.add_argument("--out-dir", type=Path)
    args = parser.parse_args()

    out_dir = args.out_dir or args.results_dir / "figures"
    out_dir.mkdir(parents=True, exist_ok=True)
    frame = _load(args.results_dir)
    _availability(frame, out_dir)
    _convergence(frame, out_dir)
    _communication(frame, out_dir)
    _load_curve(frame, out_dir)
    for path in sorted(out_dir.glob("*.png")):
        print(path)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
