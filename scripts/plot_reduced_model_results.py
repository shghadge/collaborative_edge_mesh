#!/usr/bin/env python3
import argparse
from pathlib import Path

import matplotlib

matplotlib.use("Agg")
import matplotlib.pyplot as plt
import numpy as np
import pandas as pd
from matplotlib.colors import ListedColormap
from matplotlib.patches import Patch

METHODS = (
    "full_state",
    "optimized_full_state",
    "naive_key_delta",
    "true_delta",
)
METHOD_LABELS = {
    "full_state": "Full state",
    "optimized_full_state": "Optimized full state",
    "naive_key_delta": "Naive key delta",
    "true_delta": "True delta",
}
METHOD_SHORT = {
    "full_state": "FS",
    "optimized_full_state": "OFS",
    "naive_key_delta": "NKD",
    "true_delta": "TD",
}
METHOD_COLORS = {
    "full_state": "#0072B2",
    "optimized_full_state": "#009E73",
    "naive_key_delta": "#D55E00",
    "true_delta": "#CC79A7",
}
NETWORK_ORDER = ("clean_lan", "constrained_wifi", "degraded", "severe")
NETWORK_LABELS = {
    "clean_lan": "Clean LAN",
    "constrained_wifi": "Constrained Wi-Fi",
    "degraded": "Degraded",
    "severe": "Severe",
}
PARTITION_ORDER = ("50/50", "70/30", "80/20")
CPU_ORDER = (0.25, 0.5, 1.0)
MEMORY_ORDER = (100.0, 256.0, 512.0)
NUMERIC_FIELDS = (
    "node_count",
    "partition_duration_seconds",
    "network_delay_ms",
    "network_jitter_ms",
    "network_loss_percent",
    "network_bandwidth_mbps",
    "cpu_limit_cores",
    "memory_limit_mib",
    "cpu_throttling_ratio",
    "target_offered_rate_events_s",
    "actual_offered_rate_events_s",
    "accepted_throughput_events_s",
    "attempted_events",
    "accepted_events",
    "rejected_events",
    "failed_requests",
    "write_latency_p50_ms",
    "write_latency_p95_ms",
    "write_latency_max_ms",
    "convergence_wait_seconds",
    "observed_completion_time_s",
    "time_to_first_remote_node_p95_s",
    "time_to_last_node_p95_s",
    "fully_observed_tracking_ids_count",
    "missing_at_cutoff_count",
    "tracking_coverage_percent",
    "unique_merkle_roots",
    "sync_bytes_total",
    "bytes_per_accepted_event",
    "gossip_summary_sent_bytes",
    "gossip_summary_received_bytes",
    "full_state_repair_bytes",
    "delta_repair_bytes",
    "total_state_size_bytes",
    "cpu_mean_percent",
    "cpu_peak_percent",
    "memory_mean_mib",
    "memory_peak_mib",
    "network_rx_bytes",
    "network_tx_bytes",
)
REQUIRED_FIELDS = {
    "case_id",
    "repetition",
    "sweep",
    "sync_mode",
    "node_count",
    "network_profile",
    "cpu_limit_cores",
    "memory_limit_mib",
    "accepted_throughput_events_s",
    "write_latency_p95_ms",
    "observed_completion_time_s",
    "converged_within_timeout",
    "semantic_state_converged",
    "tracking_coverage_percent",
    "sync_bytes_total",
    "bytes_per_accepted_event",
    "gossip_summary_sent_bytes",
    "full_state_repair_bytes",
    "delta_repair_bytes",
    "cpu_mean_percent",
    "memory_peak_mib",
}


def _boolean_flag(series: pd.Series) -> pd.Series:
    return series.astype(str).str.lower().map({"true": 1.0, "false": 0.0})


def load_frame(path: Path) -> pd.DataFrame:
    frame = pd.read_csv(path, low_memory=False)
    missing = sorted(REQUIRED_FIELDS - set(frame.columns))
    if missing:
        raise SystemExit(f"missing required columns: {', '.join(missing)}")
    for field in NUMERIC_FIELDS:
        if field in frame:
            frame[field] = pd.to_numeric(frame[field], errors="coerce")
    frame["combined_flag"] = _boolean_flag(frame["converged_within_timeout"])
    frame["semantic_flag"] = _boolean_flag(frame["semantic_state_converged"])
    frame["oom_flag"] = _boolean_flag(frame["oom_killed"])
    frame["tracking_fraction"] = frame["tracking_coverage_percent"] / 100.0
    return frame


def save_figure(fig: plt.Figure, path: Path) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    fig.savefig(path, dpi=300, bbox_inches="tight")
    fig.savefig(path.with_suffix(".pdf"), bbox_inches="tight")
    plt.close(fig)


def add_method_legend(fig: plt.Figure, axes: np.ndarray | list[plt.Axes]) -> None:
    flat_axes = np.asarray(axes, dtype=object).ravel()
    handles, labels = flat_axes[0].get_legend_handles_labels()
    if handles:
        fig.legend(
            handles,
            labels,
            loc="upper center",
            ncol=4,
            frameon=False,
            bbox_to_anchor=(0.5, 1.01),
        )


def plot_metric(
    ax: plt.Axes,
    frame: pd.DataFrame,
    x_field: str,
    metric: str,
    ylabel: str,
    *,
    x_order: tuple[str, ...] | list[str] | None = None,
    rate: bool = False,
    log_scale: bool = False,
) -> None:
    for method in METHODS:
        rows = frame[frame["sync_mode"] == method]
        if rows.empty:
            continue
        grouped = rows.groupby(x_field, observed=True)[metric]
        center = grouped.mean() * 100.0 if rate else grouped.median()
        if x_order is None:
            center = center.sort_index()
            x_values = center.index.to_numpy(dtype=float)
        else:
            available = [value for value in x_order if value in center.index]
            center = center.reindex(available)
            x_values = np.array([x_order.index(value) for value in available])
        ax.plot(
            x_values,
            center.to_numpy(dtype=float),
            marker="o",
            linewidth=1.8,
            markersize=4,
            color=METHOD_COLORS[method],
            label=METHOD_LABELS[method],
        )
        if not rate:
            lower = grouped.quantile(0.25).reindex(center.index).to_numpy(dtype=float)
            upper = grouped.quantile(0.75).reindex(center.index).to_numpy(dtype=float)
            ax.fill_between(
                x_values,
                lower,
                upper,
                color=METHOD_COLORS[method],
                alpha=0.12,
                linewidth=0,
            )
    if x_order is not None:
        ax.set_xticks(range(len(x_order)), x_order)
    if rate:
        ax.set_ylim(-2, 102)
    if log_scale:
        ax.set_yscale("log")
    ax.set_ylabel(ylabel)
    ax.grid(alpha=0.25)


def plot_scale(frame: pd.DataFrame, out_dir: Path) -> None:
    selected = frame[frame["sweep"] == "scale"]
    fig, axes = plt.subplots(2, 3, figsize=(14, 8.2), sharex=True)
    specs = (
        ("combined_flag", "Combined convergence (%)", True, False),
        ("observed_completion_time_s", "Observed completion (s)", False, False),
        ("bytes_per_accepted_event", "Synchronization bytes/event", False, True),
        (
            "accepted_throughput_events_s",
            "Accepted throughput (events/s)",
            False,
            False,
        ),
        ("cpu_mean_percent", "Mean CPU (%)", False, False),
        ("memory_peak_mib", "Peak memory (MiB)", False, False),
    )
    for ax, (metric, ylabel, rate, log_scale) in zip(axes.ravel(), specs):
        plot_metric(
            ax,
            selected,
            "node_count",
            metric,
            ylabel,
            rate=rate,
            log_scale=log_scale,
        )
        ax.set_xlabel("Node count")
    fig.suptitle("Scale performance under clean networking", y=1.035, fontsize=14)
    add_method_legend(fig, axes)
    fig.tight_layout()
    save_figure(fig, out_dir / "01_scale" / "scale_performance.png")


def plot_load(frame: pd.DataFrame, out_dir: Path) -> None:
    selected = frame[frame["sweep"] == "load"]
    for nodes in (5, 15, 30):
        rows = selected[selected["node_count"] == nodes]
        fig, axes = plt.subplots(2, 2, figsize=(11, 8.2), sharex=True)
        specs = (
            (
                "accepted_throughput_events_s",
                "Accepted throughput (events/s)",
                False,
                False,
            ),
            ("write_latency_p95_ms", "p95 write latency (ms)", False, True),
            ("combined_flag", "Combined convergence (%)", True, False),
            ("bytes_per_accepted_event", "Synchronization bytes/event", False, True),
        )
        for ax, (metric, ylabel, rate, log_scale) in zip(axes.ravel(), specs):
            plot_metric(
                ax,
                rows,
                "target_offered_rate_events_s",
                metric,
                ylabel,
                rate=rate,
                log_scale=log_scale,
            )
            ax.set_xlabel("Target offered rate (events/s)")
        rate_values = sorted(rows["target_offered_rate_events_s"].dropna().unique())
        axes[0, 0].plot(
            rate_values,
            rate_values,
            linestyle="--",
            color="0.35",
            linewidth=1.2,
            label="Ideal throughput",
        )
        fig.suptitle(f"Load response at {nodes} nodes", y=1.035, fontsize=14)
        add_method_legend(fig, axes)
        fig.tight_layout()
        save_figure(fig, out_dir / "02_load" / f"load_response_{nodes}n.png")


def plot_network(frame: pd.DataFrame, out_dir: Path) -> None:
    selected = frame[frame["sweep"] == "network"].copy()
    profile_labels = [NETWORK_LABELS[value] for value in NETWORK_ORDER]
    for nodes in (10, 25, 50):
        rows = selected[selected["node_count"] == nodes]
        fig, axes = plt.subplots(2, 2, figsize=(12, 8.2), sharex=True)
        specs = (
            ("semantic_flag", "Semantic convergence (%)", True, False),
            ("combined_flag", "Combined convergence (%)", True, False),
            ("observed_completion_time_s", "Observed completion (s)", False, True),
            ("bytes_per_accepted_event", "Synchronization bytes/event", False, True),
        )
        for ax, (metric, ylabel, rate, log_scale) in zip(axes.ravel(), specs):
            plot_metric(
                ax,
                rows,
                "network_profile",
                metric,
                ylabel,
                x_order=list(NETWORK_ORDER),
                rate=rate,
                log_scale=log_scale,
            )
            ax.set_xticks(range(len(profile_labels)), profile_labels, rotation=18)
        fig.suptitle(f"Network robustness at {nodes} nodes", y=1.035, fontsize=14)
        add_method_legend(fig, axes)
        fig.tight_layout()
        save_figure(fig, out_dir / "03_network" / f"network_robustness_{nodes}n.png")


def plot_partitions(frame: pd.DataFrame, out_dir: Path) -> None:
    selected = frame[frame["sweep"] == "partition_shape"]
    for nodes in (15, 30):
        rows = selected[selected["node_count"] == nodes]
        fig, axes = plt.subplots(2, 3, figsize=(14, 8.2), sharex=True)
        specs = (
            ("tracking_fraction", "Mean tracking coverage (%)", True, False),
            ("semantic_flag", "Semantic convergence (%)", True, False),
            ("combined_flag", "Combined convergence (%)", True, False),
            ("observed_completion_time_s", "Observed completion (s)", False, False),
            ("write_latency_p95_ms", "p95 write latency (ms)", False, True),
            ("bytes_per_accepted_event", "Synchronization bytes/event", False, True),
        )
        for ax, (metric, ylabel, rate, log_scale) in zip(axes.ravel(), specs):
            plot_metric(
                ax,
                rows,
                "partition_split",
                metric,
                ylabel,
                x_order=list(PARTITION_ORDER),
                rate=rate,
                log_scale=log_scale,
            )
            ax.set_xlabel("Partition split")
        fig.suptitle(f"Partition recovery at {nodes} nodes", y=1.035, fontsize=14)
        add_method_legend(fig, axes)
        fig.tight_layout()
        save_figure(fig, out_dir / "04_partitions" / f"partition_recovery_{nodes}n.png")


def _annotate_heatmap(ax: plt.Axes, matrix: np.ndarray) -> None:
    for row in range(matrix.shape[0]):
        for column in range(matrix.shape[1]):
            value = matrix[row, column]
            label = "—" if np.isnan(value) else str(int(value))
            color = "white" if not np.isnan(value) and value >= 30 else "black"
            ax.text(column, row, label, ha="center", va="center", color=color)


def plot_resource_envelopes(frame: pd.DataFrame, out_dir: Path) -> None:
    selected = frame[frame["sweep"] == "resource_interaction"]
    grouped = (
        selected.groupby(
            [
                "sync_mode",
                "network_profile",
                "cpu_limit_cores",
                "memory_limit_mib",
                "node_count",
            ],
            observed=True,
        )["combined_flag"]
        .mean()
        .reset_index(name="combined_rate")
    )
    for method in METHODS:
        fig, axes = plt.subplots(2, 2, figsize=(10.5, 8.5))
        image = None
        for ax, profile in zip(axes.ravel(), NETWORK_ORDER):
            rows = grouped[
                (grouped["sync_mode"] == method)
                & (grouped["network_profile"] == profile)
            ]
            matrix = np.full((len(CPU_ORDER), len(MEMORY_ORDER)), np.nan)
            for cpu_index, cpu in enumerate(CPU_ORDER):
                for memory_index, memory in enumerate(MEMORY_ORDER):
                    eligible = rows[
                        np.isclose(rows["cpu_limit_cores"], cpu)
                        & np.isclose(rows["memory_limit_mib"], memory)
                        & (rows["combined_rate"] >= 0.95)
                    ]
                    if not eligible.empty:
                        matrix[cpu_index, memory_index] = eligible["node_count"].max()
            image = ax.imshow(matrix, vmin=0, vmax=50, cmap="viridis", aspect="auto")
            _annotate_heatmap(ax, matrix)
            ax.set_title(NETWORK_LABELS[profile])
            ax.set_xticks(range(len(MEMORY_ORDER)), [str(int(v)) for v in MEMORY_ORDER])
            ax.set_yticks(range(len(CPU_ORDER)), [str(v) for v in CPU_ORDER])
            ax.set_xlabel("Memory limit (MiB)")
            ax.set_ylabel("CPU limit (cores)")
        if image is not None:
            fig.colorbar(
                image,
                ax=axes.ravel().tolist(),
                label="Maximum node count with ≥95% combined convergence",
                fraction=0.025,
                pad=0.04,
            )
        fig.suptitle(
            f"Reliable resource envelope — {METHOD_LABELS[method]}",
            y=0.98,
            fontsize=14,
        )
        fig.subplots_adjust(top=0.9, bottom=0.08, left=0.08, right=0.88, hspace=0.32)
        save_figure(
            fig,
            out_dir / "05_resources" / f"resource_envelope_{method}.png",
        )


def plot_resource_comparison(frame: pd.DataFrame, out_dir: Path) -> None:
    rows = frame[
        (frame["sweep"] == "resource_interaction")
        & (frame["node_count"] == 15)
        & (frame["network_profile"] == "degraded")
    ].copy()
    rows["resource_label"] = rows.apply(
        lambda row: f"{row['cpu_limit_cores']:g}/{row['memory_limit_mib']:g}", axis=1
    )
    resource_order = [
        f"{cpu:g}/{memory:g}" for cpu in CPU_ORDER for memory in MEMORY_ORDER
    ]
    fig, axes = plt.subplots(2, 3, figsize=(15, 8.5), sharex=True)
    specs = (
        ("combined_flag", "Combined convergence (%)", True, False),
        (
            "accepted_throughput_events_s",
            "Accepted throughput (events/s)",
            False,
            False,
        ),
        ("write_latency_p95_ms", "p95 write latency (ms)", False, True),
        ("bytes_per_accepted_event", "Synchronization bytes/event", False, True),
        ("cpu_throttling_ratio", "CPU throttling ratio", False, False),
        ("memory_peak_mib", "Peak memory (MiB)", False, False),
    )
    for ax, (metric, ylabel, rate, log_scale) in zip(axes.ravel(), specs):
        plot_metric(
            ax,
            rows,
            "resource_label",
            metric,
            ylabel,
            x_order=resource_order,
            rate=rate,
            log_scale=log_scale,
        )
        ax.set_xticks(
            range(len(resource_order)), resource_order, rotation=45, ha="right"
        )
        ax.set_xlabel("CPU cores / memory MiB")
    fig.suptitle(
        "Resource sensitivity at 15 nodes, degraded network, and 25 events/s",
        y=1.035,
        fontsize=14,
    )
    add_method_legend(fig, axes)
    fig.tight_layout()
    save_figure(fig, out_dir / "05_resources" / "resource_comparison_15n_degraded.png")


def representative_contexts(frame: pd.DataFrame) -> list[tuple[str, pd.DataFrame]]:
    return [
        (
            "Scale\n15 nodes",
            frame[(frame["sweep"] == "scale") & (frame["node_count"] == 15)],
        ),
        (
            "Load\n15n, 100 events/s",
            frame[
                (frame["sweep"] == "load")
                & (frame["node_count"] == 15)
                & (frame["target_offered_rate_events_s"] == 100)
            ],
        ),
        (
            "Network\n25n, degraded",
            frame[
                (frame["sweep"] == "network")
                & (frame["node_count"] == 25)
                & (frame["network_profile"] == "degraded")
            ],
        ),
        (
            "Partition\n15n, 50/50",
            frame[
                (frame["sweep"] == "partition_shape")
                & (frame["node_count"] == 15)
                & (frame["partition_split"] == "50/50")
            ],
        ),
        (
            "Resources\n15n, degraded\n0.25 CPU / 100 MiB",
            frame[
                (frame["sweep"] == "resource_interaction")
                & (frame["node_count"] == 15)
                & (frame["network_profile"] == "degraded")
                & np.isclose(frame["cpu_limit_cores"], 0.25)
                & np.isclose(frame["memory_limit_mib"], 100)
            ],
        ),
    ]


def plot_communication(frame: pd.DataFrame, out_dir: Path) -> None:
    contexts = representative_contexts(frame)
    components = (
        ("gossip_summary_sent_bytes", "Gossip summaries", "#999999"),
        ("full_state_repair_bytes", "Full-state repair", "#0072B2"),
        ("delta_repair_bytes", "Delta repair", "#CC79A7"),
    )
    fig, axes = plt.subplots(2, 3, figsize=(15, 8.8))
    for ax, (title, rows) in zip(axes.ravel(), contexts):
        x_values = np.arange(len(METHODS))
        bottom = np.zeros(len(METHODS))
        for field, label, color in components:
            values = (
                np.array(
                    [
                        rows[rows["sync_mode"] == method][field].mean()
                        for method in METHODS
                    ]
                )
                / 1_000_000
            )
            ax.bar(x_values, values, bottom=bottom, label=label, color=color)
            bottom += values
        for index, method in enumerate(METHODS):
            totals = rows[rows["sync_mode"] == method]["sync_bytes_total"] / 1_000_000
            mean = totals.mean()
            q1 = totals.quantile(0.25)
            q3 = totals.quantile(0.75)
            ax.vlines(index, q1, q3, color="black", linewidth=1.5)
            ax.scatter(index, mean, color="black", s=15, zorder=3)
        ax.set_title(title)
        ax.set_xticks(x_values, [METHOD_SHORT[method] for method in METHODS])
        ax.set_ylabel("Synchronization traffic (MB)")
        ax.grid(axis="y", alpha=0.25)
    axes.ravel()[-1].axis("off")
    handles, labels = axes.ravel()[0].get_legend_handles_labels()
    fig.legend(
        handles,
        labels,
        loc="upper center",
        ncol=3,
        frameon=False,
        bbox_to_anchor=(0.5, 1.0),
    )
    fig.suptitle(
        "Mean synchronization components; black markers show total mean and IQR",
        y=1.045,
        fontsize=14,
    )
    fig.tight_layout(rect=(0, 0, 1, 0.91))
    save_figure(fig, out_dir / "06_communication" / "synchronization_breakdown.png")

    fig, axes = plt.subplots(2, 3, figsize=(15, 8.8))
    for ax, (title, rows) in zip(axes.ravel(), contexts):
        values = [
            rows[rows["sync_mode"] == method]["bytes_per_accepted_event"].dropna()
            for method in METHODS
        ]
        boxes = ax.boxplot(values, patch_artist=True, showfliers=False)
        for patch, method in zip(boxes["boxes"], METHODS):
            patch.set_facecolor(METHOD_COLORS[method])
            patch.set_alpha(0.7)
        ax.set_title(title)
        ax.set_xticks(range(1, len(METHODS) + 1), [METHOD_SHORT[m] for m in METHODS])
        ax.set_ylabel("Synchronization bytes/event")
        ax.set_yscale("log")
        ax.grid(axis="y", alpha=0.25)
    axes.ravel()[-1].axis("off")
    fig.suptitle(
        "Synchronization cost distributions across 30 repetitions", fontsize=14
    )
    fig.tight_layout()
    save_figure(
        fig,
        out_dir / "06_communication" / "synchronization_cost_distributions.png",
    )

    fig, axes = plt.subplots(2, 3, figsize=(15, 8.8))
    width = 0.36
    for ax, (title, rows) in zip(axes.ravel(), contexts):
        x_values = np.arange(len(METHODS))
        rx = [
            rows[rows["sync_mode"] == method]["network_rx_bytes"].median() / 1e6
            for method in METHODS
        ]
        tx = [
            rows[rows["sync_mode"] == method]["network_tx_bytes"].median() / 1e6
            for method in METHODS
        ]
        ax.bar(x_values - width / 2, rx, width, label="RX", color="#56B4E9")
        ax.bar(x_values + width / 2, tx, width, label="TX", color="#E69F00")
        ax.set_title(title)
        ax.set_xticks(x_values, [METHOD_SHORT[method] for method in METHODS])
        ax.set_ylabel("Interface traffic (MB)")
        ax.set_yscale("log")
        ax.grid(axis="y", alpha=0.25)
    axes.ravel()[-1].axis("off")
    handles, labels = axes.ravel()[0].get_legend_handles_labels()
    fig.legend(
        handles,
        labels,
        loc="upper center",
        ncol=2,
        frameon=False,
        bbox_to_anchor=(0.5, 1.0),
    )
    fig.suptitle(
        "Observed network-interface traffic (not synchronization components)",
        y=1.045,
        fontsize=14,
    )
    fig.tight_layout(rect=(0, 0, 1, 0.91))
    save_figure(fig, out_dir / "06_communication" / "network_interface_traffic.png")


def plot_pareto(frame: pd.DataFrame, out_dir: Path) -> None:
    contexts = representative_contexts(frame)
    fig, axes = plt.subplots(2, 3, figsize=(15, 8.8))
    for ax, (title, rows) in zip(axes.ravel(), contexts):
        for method in METHODS:
            method_rows = rows[rows["sync_mode"] == method]
            x_value = method_rows["bytes_per_accepted_event"].median()
            y_value = method_rows["write_latency_p95_ms"].median()
            throughput = method_rows["accepted_throughput_events_s"].median()
            convergence = method_rows["combined_flag"].mean() * 100
            ax.scatter(
                x_value,
                y_value,
                s=max(45, throughput * 3),
                color=METHOD_COLORS[method],
                edgecolor="black",
                linewidth=0.6,
                alpha=0.85,
            )
            ax.annotate(
                f"{METHOD_SHORT[method]}\n{convergence:.0f}%",
                (x_value, y_value),
                xytext=(5, 4),
                textcoords="offset points",
                fontsize=8,
            )
        ax.set_title(title)
        ax.set_xlabel("Synchronization bytes/event")
        ax.set_ylabel("p95 write latency (ms)")
        ax.set_xscale("log")
        ax.set_yscale("log")
        ax.grid(alpha=0.25)
    axes.ravel()[-1].axis("off")
    legend = [
        Patch(facecolor=METHOD_COLORS[m], label=METHOD_LABELS[m]) for m in METHODS
    ]
    fig.legend(
        handles=legend,
        loc="upper center",
        ncol=4,
        frameon=False,
        bbox_to_anchor=(0.5, 1.0),
    )
    fig.suptitle(
        "Latency–communication trade-off; labels show combined convergence",
        y=1.045,
        fontsize=14,
    )
    fig.tight_layout(rect=(0, 0, 1, 0.91))
    save_figure(fig, out_dir / "07_decision" / "performance_cost_tradeoffs.png")


def decision_contexts(frame: pd.DataFrame) -> list[tuple[str, pd.DataFrame]]:
    contexts: list[tuple[str, pd.DataFrame]] = []
    for nodes in (5, 15, 30, 50):
        contexts.append(
            (
                f"Scale: {nodes}n",
                frame[(frame["sweep"] == "scale") & (frame["node_count"] == nodes)],
            )
        )
    for nodes, rate in ((15, 25), (15, 50), (15, 100), (30, 100)):
        contexts.append(
            (
                f"Load: {nodes}n @ {rate}/s",
                frame[
                    (frame["sweep"] == "load")
                    & (frame["node_count"] == nodes)
                    & (frame["target_offered_rate_events_s"] == rate)
                ],
            )
        )
    for profile in NETWORK_ORDER:
        contexts.append(
            (
                f"Network: 25n {NETWORK_LABELS[profile]}",
                frame[
                    (frame["sweep"] == "network")
                    & (frame["node_count"] == 25)
                    & (frame["network_profile"] == profile)
                ],
            )
        )
    for nodes, split in ((15, "50/50"), (15, "80/20"), (30, "50/50")):
        contexts.append(
            (
                f"Partition: {nodes}n {split}",
                frame[
                    (frame["sweep"] == "partition_shape")
                    & (frame["node_count"] == nodes)
                    & (frame["partition_split"] == split)
                ],
            )
        )
    for nodes, cpu, memory in (
        (15, 0.25, 100),
        (15, 0.5, 256),
        (15, 1.0, 512),
        (30, 0.25, 100),
        (30, 1.0, 512),
    ):
        contexts.append(
            (
                f"Resources: {nodes}n {cpu:g}c/{memory}MiB degraded",
                frame[
                    (frame["sweep"] == "resource_interaction")
                    & (frame["node_count"] == nodes)
                    & (frame["network_profile"] == "degraded")
                    & np.isclose(frame["cpu_limit_cores"], cpu)
                    & np.isclose(frame["memory_limit_mib"], memory)
                ],
            )
        )
    return contexts


def _winner(
    summary: pd.DataFrame,
    metric: str,
    *,
    maximize: bool,
    eligible: list[str],
) -> tuple[str, int, str]:
    if not eligible:
        return "None", 4, "None"
    values = summary.loc[eligible, metric].dropna()
    if values.empty:
        return "None", 4, "None"
    best = values.max() if maximize else values.min()
    winners = [method for method, value in values.items() if np.isclose(value, best)]
    full_label = " / ".join(METHOD_LABELS[method] for method in winners)
    if len(winners) > 1:
        return full_label, 5, "Tie"
    method = winners[0]
    return full_label, METHODS.index(method), METHOD_SHORT[method]


def plot_suitability(frame: pd.DataFrame, out_dir: Path) -> None:
    objectives = (
        ("Reliability", "combined_rate", True),
        ("Throughput", "throughput", True),
        ("Latency", "latency", False),
        ("Sync bytes", "communication", False),
        ("CPU", "cpu", False),
        ("Memory", "memory", False),
    )
    records = []
    matrix_rows = []
    text_rows = []
    context_labels = []
    for context, rows in decision_contexts(frame):
        if rows.empty:
            continue
        summary = rows.groupby("sync_mode", observed=True).agg(
            combined_rate=("combined_flag", "mean"),
            throughput=("accepted_throughput_events_s", "median"),
            latency=("write_latency_p95_ms", "median"),
            communication=("bytes_per_accepted_event", "median"),
            cpu=("cpu_mean_percent", "median"),
            memory=("memory_peak_mib", "median"),
        )
        eligible = [
            method
            for method in METHODS
            if method in summary.index and summary.loc[method, "combined_rate"] >= 0.95
        ]
        record = {
            "context": context,
            "eligible_methods": " / ".join(METHOD_LABELS[m] for m in eligible)
            or "None",
            "maximum_combined_convergence_percent": round(
                summary["combined_rate"].max() * 100, 3
            ),
        }
        codes = []
        texts = []
        for title, metric, maximize in objectives:
            objective_eligible = eligible
            winner, code, short = _winner(
                summary,
                metric,
                maximize=maximize,
                eligible=objective_eligible,
            )
            record[f"{title.lower().replace(' ', '_')}_winner"] = winner
            codes.append(code)
            texts.append(short)
        records.append(record)
        matrix_rows.append(codes)
        text_rows.append(texts)
        context_labels.append(context)

    output_dir = out_dir / "07_decision"
    output_dir.mkdir(parents=True, exist_ok=True)
    pd.DataFrame(records).to_csv(output_dir / "method_suitability.csv", index=False)

    matrix = np.asarray(matrix_rows)
    colors = [METHOD_COLORS[method] for method in METHODS] + ["#BDBDBD", "#F0E442"]
    fig, ax = plt.subplots(figsize=(12, max(7.5, len(context_labels) * 0.42)))
    ax.imshow(matrix, cmap=ListedColormap(colors), vmin=-0.5, vmax=5.5, aspect="auto")
    for row in range(matrix.shape[0]):
        for column in range(matrix.shape[1]):
            code = matrix[row, column]
            text_color = "black" if code in (4, 5) else "white"
            ax.text(
                column,
                row,
                text_rows[row][column],
                ha="center",
                va="center",
                color=text_color,
                fontweight="bold",
                fontsize=8,
            )
    ax.set_xticks(range(len(objectives)), [item[0] for item in objectives])
    ax.set_yticks(range(len(context_labels)), context_labels)
    ax.tick_params(axis="x", top=True, labeltop=True, bottom=False, labelbottom=False)
    ax.set_title(
        "Method suitability by objective\nOnly methods with ≥95% combined convergence are eligible",
        pad=36,
        fontsize=14,
    )
    legend = [
        Patch(
            facecolor=METHOD_COLORS[m], label=f"{METHOD_SHORT[m]} — {METHOD_LABELS[m]}"
        )
        for m in METHODS
    ]
    legend.extend(
        [
            Patch(facecolor="#BDBDBD", label="None reliable"),
            Patch(facecolor="#F0E442", label="Tie"),
        ]
    )
    ax.legend(
        handles=legend,
        loc="upper center",
        bbox_to_anchor=(0.5, -0.045),
        ncol=3,
        frameon=False,
    )
    fig.tight_layout()
    save_figure(fig, output_dir / "method_suitability_matrix.png")


def main() -> int:
    parser = argparse.ArgumentParser(
        description="Generate grouped method-comparison plots from the reduced run dataset."
    )
    parser.add_argument("input_csv", type=Path)
    parser.add_argument("--out-dir", type=Path)
    args = parser.parse_args()

    frame = load_frame(args.input_csv)
    out_dir = args.out_dir or args.input_csv.parent / "comparison_plots"
    out_dir.mkdir(parents=True, exist_ok=True)

    plot_scale(frame, out_dir)
    plot_load(frame, out_dir)
    plot_network(frame, out_dir)
    plot_partitions(frame, out_dir)
    plot_resource_envelopes(frame, out_dir)
    plot_resource_comparison(frame, out_dir)
    plot_communication(frame, out_dir)
    plot_pareto(frame, out_dir)
    plot_suitability(frame, out_dir)

    for path in sorted(out_dir.rglob("*.png")):
        print(path)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
