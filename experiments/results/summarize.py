#!/usr/bin/env python3
"""Summarize the runs of one results folder into <folder>/summary/: one row per plotted point.

Usage (from the repository root):
    python3 experiments/results/summarize.py previous                              # the runs used in the report
    python3 experiments/results/summarize.py experiments/results/new/<run folder>  # runs from run_experiments.sh

Input
    previous/        reads previous/raw/*.csv (one row per run, merged from the runner output in previous/runs/)
    a new folder     reads <folder>/runs/, the runner's output (one folder per configuration). Only experiments whose
                     configurations are all complete are summarized; incomplete ones are reported and skipped.
    The earlier version of the version comparison always comes from previous/raw/ (it needs the earlier code).
    previous/shared_item_test/runs/, if present: the earlier shared-item test (Table 7 of the report), summarized as
                     shared_item_test.csv. It used a development version of the system and is not rerun.

Output: one summary file per experiment with data, each row giving the point's setup, n_runs, median, q1 and
q3 (the interquartile range: the middle half of the runs) and a note where needed.

Measures
    convergence_time_s  runs without a partition: for each observation, the time from sending it until the last
                        node has it; a run's value is the median over its observations (time_to_last_node_p50_s)
    recovery_time_s     partitioned runs: time from removing the partition until every node has every
                        observation (recovery_after_heal_s)
    overhead_mb         runs without a partition: synchronization data exchanged by all nodes, gossip summaries
                        plus full-state or delta downloads and requests, without measurement-only timing data
                        (protocol_sync_bytes_total / 10^6)
    cpu_cores, cpu_throttled_percent, memory_peak_mib   per node (per_node_resource_metrics), Experiments 2 and 3
                        without a partition: average CPU used, how often the node was throttled by its CPU limit (% of
                        the run), and peak memory used
    Version comparison (paper test, three partition splits combined per point):
    time_after_heal_s   earlier version: convergence_time_s, checked every 2 s after the partition is removed;
                        improved version: recovery_after_heal_s, from node timestamps
    overhead_mb         earlier version: sync_bytes_total; improved version: gossip_summary_sent_bytes +
                        full_state_repair_bytes + delta_repair_bytes (the same formula), / 10^6

A 5-node mesh cannot form a 70/30 split: groups are sized round(nodes x split), so 70/30 gives the same 4/1
groups as 80/20. The 5-node 70/30 points therefore use the 80/20 runs (see the note column).
Only the Python standard library is needed.
"""

import csv
import json
import re
import statistics as st
import sys
from pathlib import Path

RESULTS = Path(__file__).resolve().parent
CONFIGS = RESULTS.parent / "configs"
PREVIOUS = RESULTS / "previous"

MATRIX_EXPERIMENTS = (
    "experiment1_baseline",
    "experiment2_constrained_resources",
    "experiment3_increased_load",
)
VERSION_COMPARISON = "experiment1_version_comparison"
METHODS = ("full_state", "optimized_full_state", "naive_key_delta", "true_delta")
SPLITS = {"p50": "50/50", "p70": "70/30", "p80": "80/20"}
RESOURCES = {"uniform": "baseline", "mixed": "constrained"}
CASE_RE = re.compile(
    r"^combined-n(\d+)-(uniform|mixed)-(none|p50|p70|p80)-e(\d+)-(\w+)$"
)
csv.field_size_limit(sys.maxsize)


def read(path):
    """Rows of a CSV file as dictionaries."""
    with open(path, newline="", encoding="utf-8") as handle:
        return list(csv.DictReader(handle))


def write(path, rows):
    """Write dictionaries to a CSV file, with the keys of the first row as the header."""
    with open(path, "w", newline="", encoding="utf-8") as handle:
        writer = csv.DictWriter(handle, fieldnames=list(rows[0]))
        writer.writeheader()
        writer.writerows(rows)


# ------------------------------------------------------------------------------------------------ input


def from_runs(runs_dir, name):
    """Rows of every configuration of experiment `name` in the runner output, or None if any is missing."""
    configs = read(CONFIGS / f"{name}.csv")
    rows = []
    for config in configs:
        files = sorted(runs_dir.glob(f"{config['case_id']}/*/eval-*.csv"))
        found = [row for path in files for row in read(path)]
        if len(found) != int(config["repetitions"]):
            return None
        rows.extend(sorted(found, key=lambda row: int(row["repetition"])))
    return rows


def load(folder):
    """Rows per experiment found in the folder (raw/ files if present, else the runner output in runs/)."""
    raw, runs = folder / "raw", folder / "runs"
    files = {name: f"{name}.csv" for name in MATRIX_EXPERIMENTS}
    files[VERSION_COMPARISON] = f"{VERSION_COMPARISON}_improved.csv"
    data = {}
    for name, file in files.items():
        if (raw / file).exists():
            data[name] = read(raw / file)
        elif runs.exists():
            source = runs / "version_comparison" if name == VERSION_COMPARISON else runs
            rows = from_runs(source, name)
            if rows is None:
                print(f"skipped: {name} (not run, or not complete, in {runs})")
                continue
            data[name] = rows
    return data


# --------------------------------------------------------------------------------------------- summary


def stats(values):
    """Median and interquartile range (inclusive quartiles) of the values, with their count."""
    q1, _, q3 = st.quantiles(values, n=4, method="inclusive")
    return {
        "n_runs": len(values),
        "median": f"{st.median(values):.4f}",
        "q1": f"{q1:.4f}",
        "q3": f"{q3:.4f}",
    }


def tag(rows):
    """Attach nodes, resources, partition split, events per node and method, parsed from each case_id."""
    tagged = []
    for row in rows:
        match = CASE_RE.match(row["case_id"])
        if match is None:
            raise ValueError(f"unexpected case_id: {row['case_id']}")
        n, devices, split, events, method = match.groups()
        tagged.append(
            {
                "row": row,
                "nodes": int(n),
                "resources": RESOURCES[devices],
                "split": split,
                "events": int(events),
                "method": method,
            }
        )
    return tagged


def mesh_points(runs, resources_list, x_name, x_values, fixed):
    """Convergence time, overhead and recovery per split, per method, setup and x value."""
    points = []
    measures = (
        ("convergence_time_s", "none"),
        ("overhead_mb", "none"),
        ("recovery_time_s", "p50"),
        ("recovery_time_s", "p70"),
        ("recovery_time_s", "p80"),
    )
    for measure, split in measures:
        for resources in resources_list:
            for method in METHODS:
                for x in x_values:
                    where = {
                        "resources": resources,
                        "method": method,
                        x_name: x,
                        **fixed,
                    }
                    source, note = split, ""
                    if split == "p70" and where["nodes"] == 5:
                        source, note = (
                            "p80",
                            "5 nodes: 80/20 runs (same 4/1 groups as 70/30)",
                        )
                    sel = [
                        r["row"]
                        for r in runs
                        if r["split"] == source
                        and all(r[k] == v for k, v in where.items())
                    ]
                    if measure == "convergence_time_s":
                        values = [float(row["time_to_last_node_p50_s"]) for row in sel]
                    elif measure == "overhead_mb":
                        values = [
                            float(row["protocol_sync_bytes_total"]) / 1e6 for row in sel
                        ]
                    else:
                        values = [float(row["recovery_after_heal_s"]) for row in sel]
                    assert values, (measure, split, where)
                    points.append(
                        {
                            "measure": measure,
                            "resources": resources,
                            "partition": SPLITS.get(split, "none"),
                            "method": method,
                            "nodes": where["nodes"],
                            "events_per_node": where["events"],
                            **stats(values),
                            "note": note,
                        }
                    )
    return points


def version_comparison(improved):
    """Time after the partition and overhead per method and mesh size, earlier version vs improved version."""
    earlier = read(PREVIOUS / "raw" / f"{VERSION_COMPARISON}_earlier.csv")
    points = []
    for version, rows, time_key, traffic in (
        (
            "earlier",
            earlier,
            "convergence_time_s",
            lambda r: float(r["sync_bytes_total"]),
        ),
        (
            "improved",
            improved,
            "recovery_after_heal_s",
            lambda r: (
                float(r["gossip_summary_sent_bytes"])
                + float(r["full_state_repair_bytes"])
                + float(r["delta_repair_bytes"])
            ),
        ),
    ):
        for measure in ("time_after_heal_s", "overhead_mb"):
            for method in METHODS:
                for n in (5, 10, 15):
                    sel = [
                        r
                        for r in rows
                        if r["sync_mode"] == method and int(r["node_count"]) == n
                    ]
                    assert len(sel) == 30, (version, method, n, len(sel))
                    values = (
                        [float(r[time_key]) for r in sel]
                        if measure == "time_after_heal_s"
                        else [traffic(r) / 1e6 for r in sel]
                    )
                    note = (
                        "previous runs (earlier code)" if version == "earlier" else ""
                    )
                    points.append(
                        {
                            "measure": measure,
                            "version": version,
                            "partition": "50/50, 70/30, 80/20",
                            "method": method,
                            "nodes": n,
                            **stats(values),
                            "note": note,
                        }
                    )
    return points


def resource_usage(runs, x_name, x_values, fixed):
    """Per kind of node and x value, runs without a partition: CPU used, CPU throttling and peak memory."""
    points = []
    for measure in ("cpu_cores", "cpu_throttled_percent", "memory_peak_mib"):
        for kind in ("uniform", "low", "medium", "high"):
            for x in x_values:
                where = {x_name: x, **fixed}
                values = []
                for r in runs:
                    if r["split"] != "none" or any(r[k] != v for k, v in where.items()):
                        continue
                    for node in json.loads(
                        r["row"]["per_node_resource_metrics"]
                    ).values():
                        if node["tier"] != kind:
                            continue
                        if measure == "cpu_cores":
                            values.append(
                                node["cpu_limit_mean_percent"] / 100 * node["cpu_limit"]
                            )
                        elif measure == "cpu_throttled_percent":
                            values.append(node["cpu_throttled_period_percent"])
                        else:
                            values.append(
                                node["memory_limit_peak_percent"]
                                / 100
                                * node["memory_limit_mib"]
                            )
                if not values:  # e.g. baseline nodes when Experiment 1 was not run in the same folder
                    continue
                s = stats(values)
                s["n_node_samples"] = s.pop("n_runs")
                points.append(
                    {
                        "measure": measure,
                        "node_kind": "baseline" if kind == "uniform" else kind,
                        "nodes": where["nodes"],
                        "events_per_node": where["events"],
                        **s,
                    }
                )
    return points


SHARED_ITEM_RE = re.compile(
    r"^(?:delta-benefit-n(\d+)-shared_(counter|set)|shared-key-n(\d+))-(\w+)$"
)
SHARED_ITEMS = {"set": "or_set", "counter": "pn_counter", None: "lww_register"}


def shared_item_test(runs_dir):
    """Synchronization data per accepted observation (KiB) when every node updates one shared item."""
    groups = {}
    for path in sorted(runs_dir.glob("*/*/eval-*.csv")):
        match = SHARED_ITEM_RE.match(path.parent.parent.name)
        if match is None:
            raise ValueError(
                f"unexpected configuration folder: {path.parent.parent.name}"
            )
        n_delta, item, n_key, method = match.groups()
        key = (SHARED_ITEMS[item], method, int(n_delta or n_key))
        groups.setdefault(key, []).extend(
            float(r["bytes_per_accepted_event"]) / 1024 for r in read(path)
        )
    return [
        {
            "measure": "kib_per_observation",
            "shared_item": item,
            "method": method,
            "nodes": n,
            **stats(values),
        }
        for (item, method, n), values in sorted(
            groups.items(), key=lambda kv: (kv[0][0], kv[0][2], kv[0][1])
        )
    ]


def summarize(folder):
    """Summarize every complete experiment in a results folder into <folder>/summary/; 1 if there is nothing to do."""
    data = load(folder)
    outputs = {}
    if VERSION_COMPARISON in data:
        outputs[f"{VERSION_COMPARISON}.csv"] = version_comparison(
            data[VERSION_COMPARISON]
        )
    exp1 = tag(data["experiment1_baseline"]) if "experiment1_baseline" in data else None
    if exp1:
        outputs["experiment1_baseline.csv"] = mesh_points(
            exp1, ("baseline",), "nodes", (5, 10, 15), {"events": 90}
        )
    if "experiment2_constrained_resources" in data:
        # The baseline side of the comparison comes from Experiment 1 of the same folder, when it was run there.
        runs = (exp1 or []) + tag(data["experiment2_constrained_resources"])
        setups = ("baseline", "constrained") if exp1 else ("constrained",)
        outputs["experiment2_constrained_resources.csv"] = mesh_points(
            runs, setups, "nodes", (5, 10, 15), {"events": 90}
        )
        outputs["experiment2_resource_usage.csv"] = resource_usage(
            runs, "nodes", (5, 10, 15), {"events": 90}
        )
    if "experiment3_increased_load" in data:
        exp3 = tag(data["experiment3_increased_load"])
        outputs["experiment3_increased_load.csv"] = mesh_points(
            exp3,
            ("baseline", "constrained"),
            "events",
            (30, 90, 180, 300),
            {"nodes": 15},
        )
        outputs["experiment3_resource_usage.csv"] = resource_usage(
            exp3, "events", (30, 90, 180, 300), {"nodes": 15}
        )
    if (folder / "shared_item_test" / "runs").exists():
        outputs["shared_item_test.csv"] = shared_item_test(
            folder / "shared_item_test" / "runs"
        )
    if not outputs:
        print(f"nothing to summarize in {folder}")
        return 1
    summary = folder / "summary"
    summary.mkdir(exist_ok=True)
    for name, rows in outputs.items():
        write(summary / name, rows)
        print(f"{summary / name} ({len(rows)} points)")
    return 0


def main() -> int:
    """Summarize `previous` or the results folder given on the command line."""
    if len(sys.argv) != 2:
        print(
            "usage: summarize.py previous | <results folder of a run>", file=sys.stderr
        )
        return 2
    folder = PREVIOUS if sys.argv[1] == "previous" else Path(sys.argv[1]).resolve()
    return summarize(folder)


if __name__ == "__main__":
    raise SystemExit(main())
