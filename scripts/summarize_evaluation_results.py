#!/usr/bin/env python3
import argparse
import csv
from collections import defaultdict
from pathlib import Path


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
        ]
        lines.append("| " + " | ".join(values) + " |")

    growth_state = _summarize_change(
        _pair_changes(
            rows,
            "current_full_state_repair",
            "growth_fixed_full_state_repair",
            "avg_state_size_bytes",
        )
    )
    delta_sync = _summarize_change(
        _pair_changes(
            rows,
            "growth_fixed_full_state_repair",
            "growth_fixed_delta_repair",
            "sync_bytes_total",
        )
    )
    delta_bytes_event = _summarize_change(
        _pair_changes(
            rows,
            "growth_fixed_full_state_repair",
            "growth_fixed_delta_repair",
            "bytes_per_accepted_event",
        )
    )
    availability = min((_float(row, "write_success_rate") for row in rows), default=0.0)

    lines.append("")
    lines.append(f"- Growth fix state-size change: {growth_state}.")
    if delta_sync == "insufficient data" or delta_bytes_event == "insufficient data":
        lines.append("- Delta bandwidth change: insufficient data for comparison.")
    else:
        lines.append(
            "- Delta bandwidth change: "
            f"sync_bytes_total {delta_sync}; bytes_per_accepted_event {delta_bytes_event}."
        )
    lines.append(f"- Availability: minimum write_success_rate {availability:.4f}.")

    out_path = Path(args.out)
    out_path.parent.mkdir(parents=True, exist_ok=True)
    out_path.write_text("\n".join(lines) + "\n", encoding="utf-8")
    print(out_path)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
