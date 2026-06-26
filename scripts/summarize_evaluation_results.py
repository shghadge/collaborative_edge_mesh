#!/usr/bin/env python3
"""Historical report helper for the old implementation-comparison CSVs.

Core evaluation runs do not depend on this script. It is kept for regenerating
the project report tables that compare historical labels such as
`current_full_state_repair` and `growth_fixed_true_delta_repair`.
"""

import argparse
import csv
import statistics
from collections import defaultdict
from pathlib import Path

HISTORICAL_IMPLEMENTATIONS = {
    "current_full_state_repair",
    "growth_fixed_full_state_repair",
    "growth_fixed_delta_repair",
    "growth_fixed_true_delta_repair",
}


COLUMNS = [
    "Scenario",
    "Nodes",
    "Split",
    "Implementation",
    "Write Success",
    "Converged",
    "Missing @ Cutoff",
    "Convergence Time",
    "Staleness",
    "Avg State Size",
    "Sync Bytes",
    "Bytes/Event",
    "Repair Pulls",
    "Delta Pulls",
    "Delta Success",
    "Delta Gaps",
    "Deltas Applied",
    "Delta Bytes",
    "Delta Bytes/Delta",
]


def _float(row: dict, key: str, default: float = 0.0) -> float:
    value = row.get(key, "")
    if value in {"", "None", None}:
        return default
    return float(value)


def _display(value: str | None) -> str:
    return "—" if value in {None, "", "None"} else str(value)


def _row_key(row: dict) -> tuple[str, str, str]:
    return (
        row.get("scenario", ""),
        row.get("node_count", ""),
        row.get("partition_split", ""),
    )


def _pair_changes(rows: list[dict], before: str, after: str, field: str) -> list[float]:
    by_key = defaultdict(dict)
    for row in rows:
        by_key[_row_key(row)][row.get("implementation", "")] = row

    changes = []
    for implementations in by_key.values():
        before_row = implementations.get(before)
        after_row = implementations.get(after)
        if not before_row or not after_row:
            continue
        before_value = _float(before_row, field)
        after_value = _float(after_row, field)
        if before_value == 0:
            continue
        changes.append(((after_value - before_value) / before_value) * 100.0)
    return changes


def _summarize_change(changes: list[float]) -> str:
    if not changes:
        return "insufficient data"
    avg_change = sum(changes) / len(changes)
    return f"{avg_change:.2f}% average change across {len(changes)} matching rows"


def _ratio_display(row: dict, numerator: str, denominator: str) -> str:
    denominator_value = _float(row, denominator)
    if denominator_value == 0:
        return "—"
    return f"{_float(row, numerator) / denominator_value:.2f}"


def _total(rows: list[dict], implementation: str, field: str) -> float:
    return sum(
        _float(row, field)
        for row in rows
        if row.get("implementation", "") == implementation
    )


def _format_total(value: float) -> str:
    return f"{int(value):,}"


def _percentile(values: list[float], percentile: float) -> float:
    if not values:
        return 0.0
    ordered = sorted(values)
    index = min(
        len(ordered) - 1,
        max(0, round((percentile / 100.0) * (len(ordered) - 1))),
    )
    return ordered[index]


def _numeric_values(rows: list[dict], field: str) -> list[float]:
    values = []
    for row in rows:
        raw = row.get(field, "")
        if raw in {"", "None", None}:
            continue
        values.append(float(raw))
    return values


def _has_repetitions(rows: list[dict]) -> bool:
    return any(row.get("repetition") not in {"", None} for row in rows)


def _has_historical_implementations(rows: list[dict]) -> bool:
    return any(
        row.get("implementation", "") in HISTORICAL_IMPLEMENTATIONS for row in rows
    )


def _repetition_key(row: dict) -> tuple[str, str, str, str]:
    return (
        row.get("implementation", ""),
        row.get("scenario", ""),
        row.get("node_count", ""),
        row.get("partition_split", ""),
    )


def _append_repetition_summary(lines: list[str], rows: list[dict]) -> None:
    if not _has_repetitions(rows):
        return

    grouped = defaultdict(list)
    for row in rows:
        grouped[_repetition_key(row)].append(row)

    columns = [
        "Implementation",
        "Scenario",
        "Nodes",
        "Split",
        "Reps",
        "Metric",
        "Median",
        "Max",
        "P90/P95",
    ]
    metric_fields = [
        ("time_to_last_node_s", "Time to Last Node"),
        ("convergence_time_s", "Observer Convergence"),
        ("sync_bytes_total", "Sync Bytes"),
        ("bytes_per_accepted_event", "Bytes/Event"),
        ("missing_at_cutoff_count", "Missing @ Cutoff"),
        ("tracking_coverage_percent", "Tracking Coverage %"),
    ]

    lines.append("")
    lines.append("## Repeated-run Statistics")
    lines.append("")
    lines.append(
        "Tail metrics are shown only as rough descriptive statistics; prefer max/worst-observed for very small repetition counts."
    )
    lines.append("")
    lines.append("| " + " | ".join(columns) + " |")
    lines.append("| " + " | ".join(["---"] * len(columns)) + " |")

    for key, group in sorted(grouped.items()):
        implementation, scenario, node_count, split = key
        repetitions = sorted({row.get("repetition", "") for row in group})
        rep_count = len([rep for rep in repetitions if rep not in {"", None}])
        for field, label in metric_fields:
            values = _numeric_values(group, field)
            if not values:
                continue
            tail = "—"
            if len(values) >= 20:
                tail = f"p95={_percentile(values, 95):.3f}"
            elif len(values) >= 10:
                tail = f"p90={_percentile(values, 90):.3f}"
            lines.append(
                "| "
                + " | ".join(
                    [
                        _display(implementation),
                        _display(scenario),
                        _display(node_count),
                        _display(split),
                        str(rep_count),
                        label,
                        f"{statistics.median(values):.3f}",
                        f"{max(values):.3f}",
                        tail,
                    ]
                )
                + " |"
            )


def main() -> int:
    parser = argparse.ArgumentParser(description="Summarize evaluation CSV files.")
    parser.add_argument("--inputs", required=True, help="Comma-separated CSV paths")
    parser.add_argument("--out", default="results/evaluation/summary.md")
    args = parser.parse_args()

    rows = []
    for raw_path in args.inputs.split(","):
        path = Path(raw_path.strip())
        if not path:
            continue
        with path.open(newline="", encoding="utf-8") as handle:
            rows.extend(csv.DictReader(handle))

    grouped = sorted(
        rows,
        key=lambda row: (
            row.get("scenario", ""),
            int(row.get("node_count", "0") or 0),
            row.get("partition_split", ""),
            row.get("implementation", ""),
        ),
    )

    lines = []
    lines.append("| " + " | ".join(COLUMNS) + " |")
    lines.append("| " + " | ".join(["---"] * len(COLUMNS)) + " |")
    for row in grouped:
        converged = "Yes" if row.get("converged_within_timeout") == "True" else "No"
        values = [
            _display(row.get("scenario")),
            _display(row.get("node_count")),
            _display(row.get("partition_split")),
            _display(row.get("implementation")),
            f"{_float(row, 'write_success_rate'):.4f}",
            converged,
            f"{_display(row.get('missing_at_cutoff_count'))} ({_display(row.get('missing_at_cutoff_pct'))}%)",
            _display(row.get("convergence_time_s")),
            _display(row.get("gateway_staleness_seconds")),
            _display(row.get("avg_state_size_bytes")),
            _display(row.get("sync_bytes_total")),
            _display(row.get("bytes_per_accepted_event")),
            _display(row.get("repair_pull_count")),
            _display(row.get("delta_pulls_started")),
            _display(row.get("delta_pulls_success")),
            _display(row.get("delta_pulls_gap")),
            _display(row.get("delta_deltas_applied")),
            _display(row.get("delta_bytes_received")),
            _ratio_display(row, "delta_bytes_received", "delta_deltas_applied"),
        ]
        lines.append("| " + " | ".join(values) + " |")

    availability = min((_float(row, "write_success_rate") for row in rows), default=0.0)

    if _has_historical_implementations(rows):
        growth_state_changes = _pair_changes(
            rows,
            "current_full_state_repair",
            "growth_fixed_full_state_repair",
            "avg_state_size_bytes",
        )
        key_delta_sync_changes = _pair_changes(
            rows,
            "growth_fixed_full_state_repair",
            "growth_fixed_delta_repair",
            "sync_bytes_total",
        )
        key_delta_bytes_event_changes = _pair_changes(
            rows,
            "growth_fixed_full_state_repair",
            "growth_fixed_delta_repair",
            "bytes_per_accepted_event",
        )
        true_delta_sync_changes = _pair_changes(
            rows,
            "growth_fixed_full_state_repair",
            "growth_fixed_true_delta_repair",
            "sync_bytes_total",
        )
        true_delta_bytes_event_changes = _pair_changes(
            rows,
            "growth_fixed_full_state_repair",
            "growth_fixed_true_delta_repair",
            "bytes_per_accepted_event",
        )
        true_vs_key_delta_sync_changes = _pair_changes(
            rows,
            "growth_fixed_delta_repair",
            "growth_fixed_true_delta_repair",
            "sync_bytes_total",
        )

        growth_state = _summarize_change(growth_state_changes)
        key_delta_sync = _summarize_change(key_delta_sync_changes)
        key_delta_bytes_event = _summarize_change(key_delta_bytes_event_changes)
        true_delta_sync = _summarize_change(true_delta_sync_changes)
        true_delta_bytes_event = _summarize_change(true_delta_bytes_event_changes)
        true_vs_key_delta_sync = _summarize_change(true_vs_key_delta_sync_changes)

        key_delta_bytes = _total(
            rows, "growth_fixed_delta_repair", "delta_bytes_received"
        )
        key_delta_applied = _total(
            rows, "growth_fixed_delta_repair", "delta_deltas_applied"
        )
        true_delta_bytes = _total(
            rows, "growth_fixed_true_delta_repair", "delta_bytes_received"
        )
        true_delta_applied = _total(
            rows, "growth_fixed_true_delta_repair", "delta_deltas_applied"
        )
        key_delta_bytes_per_delta = (
            key_delta_bytes / key_delta_applied if key_delta_applied else 0.0
        )
        true_delta_bytes_per_delta = (
            true_delta_bytes / true_delta_applied if true_delta_applied else 0.0
        )

        lines.append("")
        lines.append(f"- Growth fix state-size change: {growth_state}.")
        if (
            key_delta_sync == "insufficient data"
            or key_delta_bytes_event == "insufficient data"
        ):
            lines.append(
                "- Historical key-delta bandwidth change: insufficient data for comparison."
            )
        else:
            lines.append(
                "- Historical key-delta bandwidth change: "
                f"sync_bytes_total {key_delta_sync}; "
                f"bytes_per_accepted_event {key_delta_bytes_event}."
            )
        if (
            true_delta_sync == "insufficient data"
            or true_delta_bytes_event == "insufficient data"
        ):
            lines.append(
                "- True-delta bandwidth change: insufficient data for comparison."
            )
        else:
            lines.append(
                "- True-delta bandwidth change: "
                f"sync_bytes_total {true_delta_sync}; "
                f"bytes_per_accepted_event {true_delta_bytes_event}."
            )
        lines.append(f"- Availability: minimum write_success_rate {availability:.4f}.")
        lines.append("")
        lines.append("## Observations")
        lines.append("")
        lines.append(
            f"- Growth-fixed full-state reduces serialized state size modestly "
            f"({growth_state}), confirming event ID retention was only one growth source."
        )
        lines.append(
            f"- Historical key-level deltas are much worse than full-state repair "
            f"({key_delta_sync}), because each log entry carries full CRDT object slices."
        )
        lines.append(
            f"- True deltas improve over historical key-level deltas "
            f"({true_vs_key_delta_sync}) but still exceed growth-fixed full-state repair "
            f"({true_delta_sync})."
        )
        lines.append(
            "- Delta diagnostics point to replay overhead: historical key-delta replayed "
            f"{_format_total(key_delta_applied)} deltas / "
            f"{_format_total(key_delta_bytes)} bytes "
            f"({key_delta_bytes_per_delta:.2f} bytes per delta), while true-delta replayed "
            f"{_format_total(true_delta_applied)} deltas / "
            f"{_format_total(true_delta_bytes)} bytes "
            f"({true_delta_bytes_per_delta:.2f} bytes per delta)."
        )
        lines.append(
            "- Practical next optimization is hybrid repair: use true deltas only when a "
            "peer is close to the sender's `delta_seq`; otherwise pull one full state."
        )
    else:
        lines.append("")
        lines.append(f"- Availability: minimum write_success_rate {availability:.4f}.")
        lines.append("")
        lines.append("## Observations")
        lines.append("")
        lines.append(
            "- No historical implementation labels were present, so historical full-state/key-delta comparison notes were suppressed."
        )
        lines.append(
            "- Use the repeated-run statistics below for current Docker-system claims."
        )
        lines.append(
            "- Treat `convergence_time_s` as observer timing and `time_to_last_node_s` as the lower-skew mesh propagation metric."
        )

    _append_repetition_summary(lines, rows)

    out_path = Path(args.out)
    out_path.parent.mkdir(parents=True, exist_ok=True)
    out_path.write_text("\n".join(lines) + "\n", encoding="utf-8")
    print(out_path)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
