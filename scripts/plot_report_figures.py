#!/usr/bin/env python3
"""Generate report-ready Docker evaluation figures.

This replaces the older one-off Docker plotting helpers. It intentionally uses
only pandas and matplotlib from the project's `report` extra:

    uv sync --extra report
"""

from __future__ import annotations

import argparse
from dataclasses import dataclass
from pathlib import Path
from typing import Iterable

import matplotlib.pyplot as plt
import pandas as pd

RESULT_ROOT = Path("results/evaluation_full_matrix_20260626_restart")
PLOT_ROOT = RESULT_ROOT / "plots" / "report"

CSV_PATHS = {
    "p2_full_state": Path(
        "p2_baseline_comparison/full_state_retention_100_hard_partition/"
        "eval-43907eaf.csv"
    ),
    "p2_true_delta": Path(
        "p2_baseline_comparison/true_delta_retention_100_hard_partition/"
        "eval-aaa0be80.csv"
    ),
    "p3_retention_10": Path(
        "p3_retention_sensitivity/true_delta_retention_10_hard_partition/"
        "eval-8aa3fd8e.csv"
    ),
    "p3_retention_100": Path(
        "p3_retention_sensitivity/true_delta_retention_100_hard_partition/"
        "eval-43ef70b9.csv"
    ),
    "p4_degraded_30s": Path(
        "p4_degraded_network/true_delta_retention_100_degraded_300ms_10loss/"
        "eval-f9f303e6.csv"
    ),
    "p4_degraded_120s": Path(
        "p4_degraded_network_long_wait/"
        "true_delta_retention_100_degraded_300ms_10loss/eval-1a4f43e9.csv"
    ),
    "p5_constrained": Path(
        "p5_resource_constraint/"
        "true_delta_retention_100_hard_partition_cpu_0.2_mem_128m/"
        "eval-1f1ced47.csv"
    ),
    "p6_stress": Path(
        "p6_stress_load/true_delta_retention_100_hard_partition/eval-35542bad.csv"
    ),
    "p7_long_partition": Path(
        "p7_long_partition/true_delta_retention_10_hard_partition/eval-c90f022e.csv"
    ),
    "p8_delta_gap_boundary": Path(
        "p8_delta_gap_boundary/true_delta_retention_3_hard_partition/eval-3c622aa3.csv"
    ),
}

NUMERIC_COLUMNS = (
    "node_count",
    "event_id_retention_limit",
    "accepted_events",
    "convergence_time_s",
    "missing_at_cutoff_count",
    "time_to_last_node_s",
    "tracking_coverage_percent",
    "sync_bytes_total",
    "bytes_per_accepted_event",
    "repair_pull_count",
    "delta_pulls_gap",
)

SCENARIO_LABELS = {
    "baseline": "Baseline",
    "partition_heal": "Partition heal",
    "high_load": "High load",
}


@dataclass(frozen=True)
class Dataset:
    name: str
    path: Path
    frame: pd.DataFrame


def _read_csv(root: Path, key: str) -> Dataset:
    path = root / CSV_PATHS[key]
    if not path.exists():
        raise FileNotFoundError(f"Missing {key}: {path}")
    frame = pd.read_csv(path)
    for column in NUMERIC_COLUMNS:
        if column in frame.columns:
            frame[column] = pd.to_numeric(frame[column], errors="coerce")
    frame["scenario_label"] = (
        frame["scenario"].map(SCENARIO_LABELS).fillna(frame["scenario"])
    )
    return Dataset(key, path, frame)


def _load_all(root: Path) -> dict[str, Dataset]:
    return {key: _read_csv(root, key) for key in CSV_PATHS}


def _save(fig: plt.Figure, out_dir: Path, filename: str, generated: list[Path]) -> None:
    out_dir.mkdir(parents=True, exist_ok=True)
    out_path = out_dir / filename
    fig.tight_layout()
    fig.savefig(out_path, dpi=220)
    plt.close(fig)
    generated.append(out_path)


def _grouped_bars(
    ax: plt.Axes,
    data: pd.DataFrame,
    *,
    group_col: str,
    series_col: str,
    value_col: str,
    ylabel: str,
    title: str,
) -> None:
    table = data.pivot(index=group_col, columns=series_col, values=value_col)
    table.plot(kind="bar", ax=ax, width=0.78)
    ax.set_xlabel("")
    ax.set_ylabel(ylabel)
    ax.set_title(title)
    ax.grid(axis="y", alpha=0.25)
    ax.legend(title="")
    ax.tick_params(axis="x", rotation=25)


def _boxplot_by_category(
    ax: plt.Axes,
    groups: Iterable[tuple[str, pd.Series]],
    *,
    ylabel: str,
    title: str,
) -> None:
    labels: list[str] = []
    values: list[pd.Series] = []
    for label, series in groups:
        clean = series.dropna()
        if clean.empty:
            continue
        labels.append(label)
        values.append(clean)
    ax.boxplot(values, tick_labels=labels, showfliers=True)
    ax.set_ylabel(ylabel)
    ax.set_title(title)
    ax.grid(axis="y", alpha=0.25)
    ax.tick_params(axis="x", rotation=25)


def plot_protocol_comparison(datasets: dict[str, Dataset], out_dir: Path) -> Path:
    full_state = datasets["p2_full_state"].frame.copy()
    true_delta = datasets["p2_true_delta"].frame.copy()
    full_state["Protocol"] = "full_state"
    true_delta["Protocol"] = "true_delta"
    data = pd.concat([full_state, true_delta], ignore_index=True)
    data = data[data["node_count"] == 10]
    medians = (
        data.groupby(["scenario_label", "Protocol"], observed=True)["sync_bytes_total"]
        .median()
        .reset_index()
    )

    fig, ax = plt.subplots(figsize=(9, 5))
    _grouped_bars(
        ax,
        medians,
        group_col="scenario_label",
        series_col="Protocol",
        value_col="sync_bytes_total",
        ylabel="Median sync bytes",
        title="Protocol comparison at 10 nodes",
    )
    generated: list[Path] = []
    _save(fig, out_dir, "01_protocol_sync_bytes_10n.png", generated)
    return generated[0]


def plot_retention_sensitivity(datasets: dict[str, Dataset], out_dir: Path) -> Path:
    r10 = datasets["p3_retention_10"].frame.copy()
    r100 = datasets["p3_retention_100"].frame.copy()
    data = pd.concat([r10, r100], ignore_index=True)
    data["Retention"] = "retention=" + data["event_id_retention_limit"].astype(
        "Int64"
    ).astype(str)
    medians = (
        data.groupby(["scenario_label", "Retention"], observed=True)["sync_bytes_total"]
        .median()
        .reset_index()
    )
    gaps = (
        data.groupby("Retention", observed=True)["delta_pulls_gap"].max().reset_index()
    )

    fig, axes = plt.subplots(1, 2, figsize=(12, 5))
    _grouped_bars(
        axes[0],
        medians,
        group_col="scenario_label",
        series_col="Retention",
        value_col="sync_bytes_total",
        ylabel="Median sync bytes",
        title="Retention sensitivity: bandwidth",
    )
    axes[1].bar(gaps["Retention"], gaps["delta_pulls_gap"])
    axes[1].set_title("Retention sensitivity: observed delta gaps")
    axes[1].set_ylabel("Max delta_pulls_gap")
    axes[1].set_xlabel("")
    axes[1].grid(axis="y", alpha=0.25)

    generated: list[Path] = []
    _save(fig, out_dir, "02_retention_bytes_and_gaps.png", generated)
    return generated[0]


def plot_degraded_timeout(datasets: dict[str, Dataset], out_dir: Path) -> Path:
    d30 = datasets["p4_degraded_30s"].frame.copy()
    d120 = datasets["p4_degraded_120s"].frame.copy()
    d30 = d30[d30["node_count"] == 10]
    d30["Window"] = "30s budget"
    d120["Window"] = "120s budget"
    data = pd.concat([d30, d120], ignore_index=True)
    data["clean"] = (
        (data["converged_within_timeout"].astype(str).str.lower() == "true")
        & (data["is_converged"].astype(str).str.lower() == "true")
        & (data["missing_at_cutoff_count"] == 0)
    )
    clean = (
        data.groupby(["scenario_label", "Window"], observed=True)["clean"]
        .sum()
        .reset_index(name="clean_rows")
    )
    max_conv = (
        data.groupby(["scenario_label", "Window"], observed=True)["convergence_time_s"]
        .max()
        .reset_index()
    )

    fig, axes = plt.subplots(1, 2, figsize=(12, 5))
    _grouped_bars(
        axes[0],
        clean,
        group_col="scenario_label",
        series_col="Window",
        value_col="clean_rows",
        ylabel="Clean rows out of 5",
        title="Degraded network: budget sensitivity",
    )
    _grouped_bars(
        axes[1],
        max_conv,
        group_col="scenario_label",
        series_col="Window",
        value_col="convergence_time_s",
        ylabel="Max observer convergence (s)",
        title="Degraded network: eventual convergence cost",
    )

    generated: list[Path] = []
    _save(fig, out_dir, "03_degraded_budget_vs_eventual.png", generated)
    return generated[0]


def plot_conditions(datasets: dict[str, Dataset], out_dir: Path) -> Path:
    normal = datasets["p2_true_delta"].frame.copy()
    degraded = datasets["p4_degraded_120s"].frame.copy()
    constrained = datasets["p5_constrained"].frame.copy()
    normal["Condition"] = "Normal"
    degraded["Condition"] = "Degraded network"
    constrained["Condition"] = "0.2 CPU / 128m"
    data = pd.concat([normal, degraded, constrained], ignore_index=True)
    data = data[data["node_count"] == 10]

    fig, axes = plt.subplots(1, 2, figsize=(13, 5))
    time_groups = [
        (f"{condition}\n{scenario}", group["time_to_last_node_s"])
        for (condition, scenario), group in data.groupby(
            ["Condition", "scenario_label"], sort=False, observed=True
        )
    ]
    repair_groups = [
        (f"{condition}\n{scenario}", group["repair_pull_count"])
        for (condition, scenario), group in data.groupby(
            ["Condition", "scenario_label"], sort=False, observed=True
        )
    ]
    _boxplot_by_category(
        axes[0],
        time_groups,
        ylabel="Time to last node (s)",
        title="Tail propagation by condition (10 nodes)",
    )
    _boxplot_by_category(
        axes[1],
        repair_groups,
        ylabel="Repair pulls",
        title="Repair pressure by condition (10 nodes)",
    )
    generated: list[Path] = []
    _save(fig, out_dir, "04_conditions_tail_and_repairs_10n.png", generated)
    return generated[0]


def plot_stress_bytes(datasets: dict[str, Dataset], out_dir: Path) -> Path:
    normal = datasets["p2_true_delta"].frame.copy()
    stress = datasets["p6_stress"].frame.copy()
    normal["Workload"] = "5 events/node"
    stress["Workload"] = "25 events/node"
    data = pd.concat([normal, stress], ignore_index=True)
    data = data[data["node_count"] == 10]
    medians = (
        data.groupby(["scenario_label", "Workload"], observed=True)["sync_bytes_total"]
        .median()
        .reset_index()
    )

    fig, ax = plt.subplots(figsize=(9, 5))
    _grouped_bars(
        ax,
        medians,
        group_col="scenario_label",
        series_col="Workload",
        value_col="sync_bytes_total",
        ylabel="Median sync bytes",
        title="Stress workload increases synchronization cost",
    )
    generated: list[Path] = []
    _save(fig, out_dir, "05_stress_sync_bytes_10n.png", generated)
    return generated[0]


def plot_long_partition(datasets: dict[str, Dataset], out_dir: Path) -> Path:
    normal = datasets["p3_retention_10"].frame.copy()
    long = datasets["p7_long_partition"].frame.copy()
    boundary = datasets["p8_delta_gap_boundary"].frame.copy()
    normal["Partition"] = "8s, retention=10"
    long["Partition"] = "30s, retention=10"
    boundary["Partition"] = "60s, retention=3"
    data = pd.concat([normal, long, boundary], ignore_index=True)
    data = data[(data["node_count"] == 10) & (data["scenario"] == "partition_heal")]
    data["Split"] = data["partition_split"].fillna("—")
    medians = (
        data.groupby(["Split", "Partition"], observed=True)["time_to_last_node_s"]
        .median()
        .reset_index()
    )

    fig, ax = plt.subplots(figsize=(9, 5))
    _grouped_bars(
        ax,
        medians,
        group_col="Split",
        series_col="Partition",
        value_col="time_to_last_node_s",
        ylabel="Median time to last node (s)",
        title="Longer hard partitions extend propagation tail",
    )
    generated: list[Path] = []
    _save(fig, out_dir, "06_long_partition_tail_10n.png", generated)
    return generated[0]


def generate_figures(root: Path, out_dir: Path) -> list[Path]:
    datasets = _load_all(root)
    generated = [
        plot_protocol_comparison(datasets, out_dir),
        plot_retention_sensitivity(datasets, out_dir),
        plot_degraded_timeout(datasets, out_dir),
        plot_conditions(datasets, out_dir),
        plot_stress_bytes(datasets, out_dir),
        plot_long_partition(datasets, out_dir),
    ]
    return generated


def main() -> int:
    parser = argparse.ArgumentParser(
        description="Generate report figures from the Docker evaluation CSV matrix."
    )
    parser.add_argument("--root", type=Path, default=RESULT_ROOT)
    parser.add_argument("--out-dir", type=Path, default=PLOT_ROOT)
    args = parser.parse_args()

    generated = generate_figures(args.root, args.out_dir)
    for path in generated:
        print(path)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
