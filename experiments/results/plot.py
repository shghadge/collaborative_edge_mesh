#!/usr/bin/env python3
"""Draw the experiment figures of one results folder from its summary files only (<folder>/summary/ → figures/).

Every point is a median over runs; error bars show the interquartile range (q1 to q3).

Figures
- exp1_version_comparison.png               earlier vs improved version (dashed = earlier)
- exp1_convergence_overhead.png              convergence time | network overhead (baseline resources)
- exp1_recovery.png                          recovery after 50/50 | 70/30 | 80/20 partitions
- exp2_convergence_overhead_overlay.png      both setups in one panel (dashed = baseline resources)
- exp2_convergence_overhead_side_by_side.png one column per setup, shared axes
- exp2_recovery.png                          one row per setup, one column per split
- exp2_resource_usage.png                    CPU used | CPU throttling | peak memory, per kind of node and mesh size
- exp3_convergence_overhead_overlay.png, exp3_convergence_overhead_side_by_side.png, exp3_recovery.png
- exp3_resource_usage.png                    CPU used | CPU throttling | peak memory, per kind of node and load

Usage (from the repository root):
    uv run --extra report python experiments/results/plot.py previous                              # report runs
    uv run --extra report python experiments/results/plot.py experiments/results/new/<run folder>  # a new run
Figures whose summary file is missing (an experiment not run in that folder) are skipped.
"""

import csv
import sys
from collections import defaultdict
from pathlib import Path

import matplotlib

matplotlib.use("Agg")
import matplotlib.pyplot as plt  # noqa: E402
from matplotlib.lines import Line2D  # noqa: E402
from matplotlib.transforms import blended_transform_factory  # noqa: E402

RESULTS = Path(__file__).resolve().parent
SUMMARY = RESULTS / "previous" / "summary"  # set from the command line in main()
FIGURES = RESULTS / "previous" / "figures"
WIDTH = 7.16  # inches, two-column figure width

METHODS = {  # method -> label, color, marker, x offset
    "full_state": ("Full State", "#d62728", "o", -0.45),
    "optimized_full_state": ("Optimized Full State", "#17becf", "s", -0.15),
    "naive_key_delta": ("Naive Key-Delta", "#ff7f0e", "^", 0.15),
    "true_delta": ("True Delta-State", "#2ca02c", "D", 0.45),
}
SETUPS = {"baseline": "Baseline Resources", "constrained": "Constrained Resources"}
SETUP_LIMITS = {  # what each setup gives the nodes, shown in the legends
    "baseline": "Baseline Resources: every node 0.5 CPU, 512 MiB",
    "constrained": "Constrained Resources: in every 5 nodes, 2 × (0.25 CPU, 256 MiB), "
    "2 × (0.5 CPU, 512 MiB), 1 × (1 CPU, 1 GiB)",
}
SPLITS = ("50/50", "70/30", "80/20")
MESH_MEASURES = {
    "convergence_time_s": ("Convergence Time (No Partition)", "Time (seconds)"),
    "overhead_mb": ("Network Overhead (No Partition)", "Data Exchanged (megabytes)"),
}
DASHED = (
    "--",
    0.5,
)  # style and alpha of the comparison series (baseline resources, earlier version)


def read(name):
    """Rows of a summary file as dictionaries."""
    with open(SUMMARY / name, newline="", encoding="utf-8") as handle:
        return list(csv.DictReader(handle))


def series(rows, group, x_key):
    """(group values..., method) -> sorted [(x, median, q1, q3)]"""
    out = defaultdict(list)
    for r in rows:
        key = tuple(r[g] for g in group) + (r["method"],)
        out[key].append(
            (int(r[x_key]), float(r["median"]), float(r["q1"]), float(r["q3"]))
        )
    return {k: sorted(v) for k, v in out.items()}


class Axis:
    """An x-axis: numeric (nodes) places points at their value, categorical (load) at evenly spaced positions.

    `step` scales the small x offsets that keep the four methods' points from overlapping."""

    def __init__(self, values, label, numeric):
        self.values, self.label, self.numeric = values, label, numeric
        self.pos = {x: (x if numeric else i) for i, x in enumerate(values)}
        self.step = 1.0 if numeric else 0.2

    def finish(self, ax):
        """Set the ticks, labels, limits and grid of a finished panel."""
        ax.set_xticks([self.pos[x] for x in self.values])
        ax.set_xticklabels([str(x) for x in self.values])
        lo, hi = min(self.pos.values()), max(self.pos.values())
        pad = 1.6 if self.numeric else 0.4
        ax.set_xlim(lo - pad, hi + pad)
        ax.set_xlabel(self.label)
        ax.grid(True, linewidth=0.4, alpha=0.5)


NODES = Axis((5, 10, 15), "Number of Nodes", True)
LOAD = Axis((30, 90, 180, 300), "Events per Node (15 Nodes)", False)


def draw(ax, axis, points, method, style="-", alpha=1.0, shift=0.0):
    """Draw one method's medians with interquartile-range error bars; returns the highest q3, for the y limit."""
    _, color, marker, offset = METHODS[method]
    xs = [axis.pos[x] + (offset + shift) * axis.step for x, *_ in points]
    meds = [m for _, m, _, _ in points]
    ax.errorbar(
        xs,
        meds,
        yerr=[[m - lo for _, m, lo, _ in points], [hi - m for _, m, _, hi in points]],
        color=color,
        marker=marker,
        markersize=3.5,
        linewidth=1.1,
        linestyle=style,
        alpha=alpha,
        elinewidth=0.7,
        capsize=2,
    )
    return max(hi for *_, hi in points)


def legend(fig, extra=(), extra_ncol=2, lines=True):
    """Methods on the first row; `extra` (label, style, alpha) entries on the rows below. Returns the top of the plots."""
    row = (
        0.2 / fig.get_figheight()
    )  # one legend row, as a fraction of the figure height
    handles = [
        Line2D([], [], color=c, marker=m, markersize=3.5, linewidth=1.1, label=label)
        for label, c, m, _ in METHODS.values()
    ]
    fig.legend(
        handles=handles,
        loc="upper center",
        ncol=4,
        frameon=False,
        bbox_to_anchor=(0.5, 1.0),
    )
    if not extra:
        return 1 - 1.3 * row
    handles = [
        Line2D(
            [],
            [],
            color="black",
            linestyle=style if lines else "none",
            alpha=alpha,
            linewidth=1.1,
            label=label,
        )
        for label, style, alpha in extra
    ]
    fig.legend(
        handles=handles,
        loc="upper center",
        ncol=extra_ncol,
        frameon=False,
        bbox_to_anchor=(0.5, 1.0 - row),
        **({} if lines else {"handlelength": 0, "handletextpad": 0}),
    )
    rows = -(-len(extra) // extra_ncol)
    return 1 - (1.3 + rows) * row


def setup_notes(setups):
    """Text-only legend entries that say what each setup gives the nodes."""
    return [(SETUP_LIMITS[s], "-", 1.0) for s in setups]


def save(fig, name, top):
    """Lay out the figure below its legend (`top` is the fraction of the height left for the plots) and save it."""
    fig.tight_layout(rect=(0, 0, 1, top))
    fig.savefig(FIGURES / name, dpi=300, bbox_inches="tight")
    plt.close(fig)
    print(f"figures/{name}")


def version_comparison():
    """Figure: earlier vs improved version, time after the partition and overhead (dashed = earlier)."""
    data = series(
        read("experiment1_version_comparison.csv"), ("measure", "version"), "nodes"
    )
    fig, axes = plt.subplots(1, 2, figsize=(WIDTH, 3.1))
    panels = (
        ("time_after_heal_s", "Time to Converge After the Partition", "Time (seconds)"),
        ("overhead_mb", "Network Overhead", "Data Exchanged (megabytes)"),
    )
    for ax, (measure, title, ylabel) in zip(axes, panels):
        top = 0
        for version, (style, alpha), shift in (
            ("earlier", DASHED, -0.12),
            ("improved", ("-", 1.0), 0.12),
        ):
            for method in METHODS:
                top = max(
                    top,
                    draw(
                        ax,
                        NODES,
                        data[(measure, version, method)],
                        method,
                        style,
                        alpha,
                        shift,
                    ),
                )
        ax.set_title(title, fontsize=8, fontweight="bold")
        ax.set_ylabel(ylabel)
        ax.set_ylim(0, top * 1.08)
        NODES.finish(ax)
    top = legend(fig, (("Improved version", "-", 1.0), ("Earlier version", "--", 0.5)))
    save(fig, "exp1_version_comparison.png", top)


def convergence_overhead(name, axis, x_key, setups, layout, out):
    """Figure: convergence time and overhead without a partition, in one of three layouts.

    "single" (one setup), "overlay" (both setups in each panel, baseline dashed) or "side_by_side" (one column per
    setup with shared y limits)."""
    data = series(read(name), ("measure", "partition", "resources"), x_key)
    if layout == "side_by_side":
        fig, axes = plt.subplots(2, 2, figsize=(WIDTH, 5.2))
        grid = [
            (axes[r][c], measure, [setup])
            for r, measure in enumerate(MESH_MEASURES)
            for c, setup in enumerate(setups)
        ]
    else:
        fig, axes = plt.subplots(
            1, 2, figsize=(WIDTH, 3.1 if layout == "overlay" else 2.9)
        )
        grid = [
            (axes[i], measure, list(setups)) for i, measure in enumerate(MESH_MEASURES)
        ]
    tops = defaultdict(float)
    for ax, measure, panel_setups in grid:
        title, ylabel = MESH_MEASURES[measure]
        for setup in panel_setups:
            style, alpha, shift = (
                (DASHED + (-0.12,) if setup == "baseline" else ("-", 1.0, 0.12))
                if layout == "overlay"
                else ("-", 1.0, 0.0)
            )
            for method in METHODS:
                tops[measure] = max(
                    tops[measure],
                    draw(
                        ax,
                        axis,
                        data[(measure, "none", setup, method)],
                        method,
                        style,
                        alpha,
                        shift,
                    ),
                )
        if layout == "side_by_side":
            title = f"{title.split(' (')[0]}: {SETUPS[panel_setups[0]]}"
        ax.set_title(title, fontsize=8, fontweight="bold")
        ax.set_ylabel(ylabel)
        axis.finish(ax)
    for ax, measure, _ in grid:
        ax.set_ylim(0, tops[measure] * 1.08)
    if "constrained" not in setups:  # Experiment 1: baseline resources only
        top = legend(fig)
    elif layout == "overlay" and len(setups) > 1:
        styles = {"constrained": ("-", 1.0), "baseline": DASHED}
        top = legend(
            fig, [(SETUP_LIMITS[s], *styles[s]) for s in ("constrained", "baseline")], 1
        )
    else:
        top = legend(fig, setup_notes(setups), 1, lines=False)
    save(fig, out, top)


def recovery(name, axis, x_key, setups, out):
    """Figure: recovery time after each partition split, one row per resource setup."""
    rows = read(name)
    data = series(rows, ("measure", "partition", "resources"), x_key)
    notes = {r["partition"] for r in rows if r["note"]}
    fig, axes = plt.subplots(
        len(setups), 3, figsize=(WIDTH, 2.6 * len(setups) + 0.3), squeeze=False
    )
    top = 0
    for r, setup in enumerate(setups):
        for c, split in enumerate(SPLITS):
            ax = axes[r][c]
            for method in METHODS:
                top = max(
                    top,
                    draw(
                        ax,
                        axis,
                        data[("recovery_time_s", split, setup, method)],
                        method,
                    ),
                )
            title = f"Recovery After {split} Partition" + (
                f"\n{SETUPS[setup]}" if len(setups) > 1 else ""
            )
            ax.set_title(title, fontsize=8, fontweight="bold")
            ax.set_ylabel("Time (seconds)")
            axis.finish(ax)
    for ax in axes.flat:
        ax.set_ylim(0, top * 1.08)
    if "70/30" in notes:
        for r in range(len(setups)):
            axes[r][1].text(
                3.7,
                top * 0.97,
                "5 nodes: same 4/1 groups\nas 80/20 (80/20 runs shown)",
                ha="left",
                va="top",
                fontsize=6,
                color="grey",
            )
    top = (
        legend(fig, setup_notes(setups), 1, lines=False)
        if "constrained" in setups
        else legend(fig)
    )
    save(fig, out, top)


def resource_usage(name, axis, x_key, tops, out):
    """Figure: CPU used, CPU throttling and peak memory per kind of node; `tops` are the three y limits."""
    rows = read(name)
    kinds = {  # node kind -> label, color, marker, style, x offset (in steps of the axis)
        "baseline": (
            "Baseline resources (limit 0.5 CPU, 512 MiB)",
            "#444444",
            "o",
            "--",
            -0.45,
        ),
        "low": (
            "Constrained: low (limit 0.25 CPU, 256 MiB)",
            "#4b1f6f",
            "v",
            "-",
            -0.15,
        ),
        "medium": (
            "Constrained: medium (limit 0.5 CPU, 512 MiB)",
            "#8e5bbf",
            "s",
            "-",
            0.15,
        ),
        "high": (
            "Constrained: high (limit 1 CPU, 1,024 MiB)",
            "#c7a9e6",
            "^",
            "-",
            0.45,
        ),
    }
    panels = (  # measure, title, y label, low-resource limit line
        (
            "cpu_cores",
            "CPU Used",
            "Average CPU used (cores)",
            (0.25, "low-node limit: 0.25"),
        ),
        ("cpu_throttled_percent", "CPU Throttling", "Throttled (% of the run)", None),
        (
            "memory_peak_mib",
            "Peak Memory Used",
            "Peak memory used (MiB)",
            (256, "low-node limit: 256"),
        ),
    )
    fig, axes = plt.subplots(1, 3, figsize=(WIDTH, 3.0))
    for ax, (measure, title, ylabel, limit), top in zip(axes, panels, tops):
        for kind, (label, color, marker, style, offset) in kinds.items():
            points = sorted(
                (int(r[x_key]), float(r["median"]), float(r["q1"]), float(r["q3"]))
                for r in rows
                if r["measure"] == measure and r["node_kind"] == kind
            )
            if not points:
                continue
            ax.errorbar(
                [axis.pos[x] + offset * axis.step for x, *_ in points],
                [m for _, m, _, _ in points],
                yerr=[
                    [m - lo for _, m, lo, _ in points],
                    [hi - m for _, m, _, hi in points],
                ],
                color=color,
                marker=marker,
                markersize=3.5,
                linewidth=1.1,
                linestyle=style,
                elinewidth=0.7,
                capsize=2,
                label=label,
            )
        if limit:
            value, text = limit
            ax.axhline(value, color=kinds["low"][1], linewidth=0.8, linestyle=":")
            ax.text(
                0.02,
                value * 0.985,
                text,
                color=kinds["low"][1],
                fontsize=6,
                va="top",
                transform=blended_transform_factory(ax.transAxes, ax.transData),
            )
        ax.set_title(title, fontsize=8, fontweight="bold")
        ax.set_ylabel(ylabel)
        ax.set_ylim(0, top)
        axis.finish(ax)
    handles, labels = axes[0].get_legend_handles_labels()
    fig.legend(
        handles,
        labels,
        loc="upper center",
        ncol=2,
        frameon=False,
        bbox_to_anchor=(0.5, 1.0),
    )
    save(fig, out, 0.86)


def setups_in(name):
    """Resource setups present in a summary file, in the order of SETUPS."""
    rows = read(name)
    return tuple(s for s in SETUPS if any(r["resources"] == s for r in rows))


def main() -> int:
    """Draw every figure whose summary file exists in `previous` or the folder given on the command line."""
    global SUMMARY, FIGURES
    if len(sys.argv) != 2:
        print("usage: plot.py previous | <results folder of a run>", file=sys.stderr)
        return 2
    folder = (
        RESULTS / "previous"
        if sys.argv[1] == "previous"
        else Path(sys.argv[1]).resolve()
    )
    SUMMARY, FIGURES = folder / "summary", folder / "figures"
    if not SUMMARY.exists():
        print(f"no summary in {folder}; run summarize.py first", file=sys.stderr)
        return 1
    plt.rcParams.update({"font.family": "serif", "font.size": 8})
    FIGURES.mkdir(exist_ok=True)
    have = {path.name for path in SUMMARY.glob("*.csv")}
    if "experiment1_version_comparison.csv" in have:
        version_comparison()
    if "experiment1_baseline.csv" in have:
        convergence_overhead(
            "experiment1_baseline.csv",
            NODES,
            "nodes",
            ("baseline",),
            "single",
            "exp1_convergence_overhead.png",
        )
        recovery(
            "experiment1_baseline.csv",
            NODES,
            "nodes",
            ("baseline",),
            "exp1_recovery.png",
        )
    for number, name, axis, x_key in (
        (2, "experiment2_constrained_resources.csv", NODES, "nodes"),
        (3, "experiment3_increased_load.csv", LOAD, "events_per_node"),
    ):
        if name not in have:
            continue
        setups = setups_in(name)
        convergence_overhead(
            name,
            axis,
            x_key,
            setups,
            "overlay",
            f"exp{number}_convergence_overhead_overlay.png",
        )
        if (
            len(setups) > 1
        ):  # side by side needs both setups (Experiment 2 needs Experiment 1 in the same folder)
            convergence_overhead(
                name,
                axis,
                x_key,
                setups,
                "side_by_side",
                f"exp{number}_convergence_overhead_side_by_side.png",
            )
        recovery(name, axis, x_key, setups, f"exp{number}_recovery.png")
    if "experiment2_resource_usage.csv" in have:
        resource_usage(
            "experiment2_resource_usage.csv",
            NODES,
            "nodes",
            (0.3, 25, 300),
            "exp2_resource_usage.png",
        )
    if "experiment3_resource_usage.csv" in have:
        resource_usage(
            "experiment3_resource_usage.csv",
            LOAD,
            "events_per_node",
            (0.3, 60, 300),
            "exp3_resource_usage.png",
        )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
