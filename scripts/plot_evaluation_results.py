#!/usr/bin/env python3
import argparse
import csv
import html
import statistics
from collections import defaultdict
from pathlib import Path


CHARTS = [
    (
        "avg_state_size_bytes",
        "Average State Size",
        "bytes",
        "avg_state_size_by_nodes.svg",
    ),
    (
        "avg_state_bytes_per_known_event",
        "State Bytes per Known Event",
        "bytes/event",
        "state_bytes_per_event_by_nodes.svg",
    ),
    ("sync_bytes_total", "Synchronization Bytes", "bytes", "sync_bytes_by_nodes.svg"),
    (
        "bytes_per_accepted_event",
        "Sync Bytes per Accepted Event",
        "bytes/event",
        "sync_bytes_per_event_by_nodes.svg",
    ),
    (
        "convergence_time_s",
        "Convergence Time",
        "seconds",
        "convergence_time_by_nodes.svg",
    ),
]

COLORS = {
    "current_full_state_repair": "#2563eb",
    "growth_fixed_full_state_repair": "#16a34a",
    "growth_fixed_delta_repair": "#dc2626",
}


def _float(row: dict, key: str) -> float | None:
    value = row.get(key, "")
    if value in {"", "None", None}:
        return None
    return float(value)


def _load_rows(paths: list[str]) -> list[dict]:
    rows = []
    for raw_path in paths:
        path = Path(raw_path.strip())
        if not raw_path.strip():
            continue
        with path.open(newline="", encoding="utf-8") as handle:
            rows.extend(csv.DictReader(handle))
    return rows


def _aggregate(rows: list[dict], metric: str) -> list[dict]:
    grouped = defaultdict(list)
    for row in rows:
        value = _float(row, metric)
        if value is None:
            continue
        implementation = row.get("implementation", "unknown")
        node_count = int(row.get("node_count", "0") or 0)
        grouped[(implementation, node_count)].append(value)

    points = []
    for (implementation, node_count), values in sorted(grouped.items()):
        points.append(
            {
                "implementation": implementation,
                "node_count": node_count,
                "metric": metric,
                "mean": statistics.mean(values),
                "min": min(values),
                "max": max(values),
                "samples": len(values),
            }
        )
    return points


def _write_datapoints(rows: list[dict], out_dir: Path) -> Path:
    path = out_dir / "comparison_datapoints.csv"
    fields = [
        "implementation",
        "scenario",
        "node_count",
        "partition_split",
        "avg_state_size_bytes",
        "avg_state_bytes_per_known_event",
        "sync_bytes_total",
        "bytes_per_accepted_event",
        "convergence_time_s",
        "converged_within_timeout",
        "missing_at_cutoff_count",
    ]
    with path.open("w", newline="", encoding="utf-8") as handle:
        writer = csv.DictWriter(handle, fieldnames=fields)
        writer.writeheader()
        for row in rows:
            writer.writerow({field: row.get(field, "") for field in fields})
    return path


def _write_aggregates(rows: list[dict], out_dir: Path) -> Path:
    path = out_dir / "comparison_aggregates.csv"
    fields = ["implementation", "node_count", "metric", "mean", "min", "max", "samples"]
    all_points = []
    for metric, _, _, _ in CHARTS:
        all_points.extend(_aggregate(rows, metric))
    with path.open("w", newline="", encoding="utf-8") as handle:
        writer = csv.DictWriter(handle, fieldnames=fields)
        writer.writeheader()
        writer.writerows(all_points)
    return path


def _write_comparison_changes(rows: list[dict], out_dir: Path) -> Path:
    path = out_dir / "comparison_changes.csv"
    by_key = defaultdict(dict)
    for row in rows:
        key = (
            row.get("scenario", ""),
            row.get("node_count", ""),
            row.get("partition_split", ""),
        )
        by_key[key][row.get("implementation", "")] = row

    fields = [
        "scenario",
        "node_count",
        "partition_split",
        "current_avg_state_size_bytes",
        "growth_avg_state_size_bytes",
        "growth_state_size_change_pct",
        "growth_sync_bytes_total",
        "delta_sync_bytes_total",
        "delta_sync_bytes_change_pct",
        "growth_bytes_per_accepted_event",
        "delta_bytes_per_accepted_event",
        "delta_bytes_per_event_change_pct",
    ]

    def pct(before: float | None, after: float | None) -> str:
        if before in {None, 0} or after is None:
            return ""
        return f"{((after - before) / before) * 100:.2f}"

    with path.open("w", newline="", encoding="utf-8") as handle:
        writer = csv.DictWriter(handle, fieldnames=fields)
        writer.writeheader()
        for key, implementations in sorted(by_key.items()):
            current = implementations.get("current_full_state_repair", {})
            growth = implementations.get("growth_fixed_full_state_repair", {})
            delta = implementations.get("growth_fixed_delta_repair", {})
            current_state = _float(current, "avg_state_size_bytes")
            growth_state = _float(growth, "avg_state_size_bytes")
            growth_sync = _float(growth, "sync_bytes_total")
            delta_sync = _float(delta, "sync_bytes_total")
            growth_bpe = _float(growth, "bytes_per_accepted_event")
            delta_bpe = _float(delta, "bytes_per_accepted_event")
            writer.writerow(
                {
                    "scenario": key[0],
                    "node_count": key[1],
                    "partition_split": key[2],
                    "current_avg_state_size_bytes": current_state or "",
                    "growth_avg_state_size_bytes": growth_state or "",
                    "growth_state_size_change_pct": pct(current_state, growth_state),
                    "growth_sync_bytes_total": growth_sync or "",
                    "delta_sync_bytes_total": delta_sync or "",
                    "delta_sync_bytes_change_pct": pct(growth_sync, delta_sync),
                    "growth_bytes_per_accepted_event": growth_bpe or "",
                    "delta_bytes_per_accepted_event": delta_bpe or "",
                    "delta_bytes_per_event_change_pct": pct(growth_bpe, delta_bpe),
                }
            )
    return path


def _scale(value: float, min_value: float, max_value: float, size: float) -> float:
    if max_value == min_value:
        return size / 2
    return (value - min_value) / (max_value - min_value) * size


def _render_svg(points: list[dict], metric: str, title: str, y_label: str) -> str:
    width = 920
    height = 560
    left = 90
    right = 30
    top = 70
    bottom = 90
    plot_w = width - left - right
    plot_h = height - top - bottom

    node_counts = sorted({point["node_count"] for point in points})
    implementations = sorted({point["implementation"] for point in points})
    values = [point["mean"] for point in points]
    y_min = 0.0
    y_max = max(values) * 1.1 if values else 1.0
    if y_max <= 0:
        y_max = 1.0

    by_impl = defaultdict(list)
    for point in points:
        by_impl[point["implementation"]].append(point)
    for impl_points in by_impl.values():
        impl_points.sort(key=lambda point: point["node_count"])

    def x_for(node_count: int) -> float:
        if len(node_counts) == 1:
            return left + plot_w / 2
        idx = node_counts.index(node_count)
        return left + _scale(idx, 0, len(node_counts) - 1, plot_w)

    def y_for(value: float) -> float:
        return top + plot_h - _scale(value, y_min, y_max, plot_h)

    svg = [
        f'<svg xmlns="http://www.w3.org/2000/svg" width="{width}" height="{height}" viewBox="0 0 {width} {height}">',
        '<rect width="100%" height="100%" fill="#0f172a"/>',
        f'<text x="{width / 2}" y="32" text-anchor="middle" font-family="sans-serif" font-size="22" fill="#e2e8f0">{html.escape(title)}</text>',
        f'<text x="{width / 2}" y="535" text-anchor="middle" font-family="sans-serif" font-size="14" fill="#94a3b8">Node count</text>',
        f'<text x="20" y="{height / 2}" transform="rotate(-90 20 {height / 2})" text-anchor="middle" font-family="sans-serif" font-size="14" fill="#94a3b8">{html.escape(y_label)}</text>',
        f'<line x1="{left}" y1="{top + plot_h}" x2="{left + plot_w}" y2="{top + plot_h}" stroke="#475569"/>',
        f'<line x1="{left}" y1="{top}" x2="{left}" y2="{top + plot_h}" stroke="#475569"/>',
    ]

    for tick in range(6):
        value = y_max * tick / 5
        y = y_for(value)
        svg.append(
            f'<line x1="{left}" y1="{y:.2f}" x2="{left + plot_w}" y2="{y:.2f}" stroke="#1e293b"/>'
        )
        svg.append(
            f'<text x="{left - 10}" y="{y + 4:.2f}" text-anchor="end" font-family="monospace" font-size="11" fill="#94a3b8">{value:,.0f}</text>'
        )

    for node_count in node_counts:
        x = x_for(node_count)
        svg.append(
            f'<line x1="{x:.2f}" y1="{top + plot_h}" x2="{x:.2f}" y2="{top + plot_h + 6}" stroke="#475569"/>'
        )
        svg.append(
            f'<text x="{x:.2f}" y="{top + plot_h + 24}" text-anchor="middle" font-family="monospace" font-size="12" fill="#cbd5e1">{node_count}</text>'
        )

    legend_x = left + 10
    legend_y = top + 10
    for index, implementation in enumerate(implementations):
        color = COLORS.get(implementation, "#f59e0b")
        y = legend_y + index * 22
        svg.append(
            f'<rect x="{legend_x}" y="{y - 10}" width="14" height="14" fill="{color}"/>'
        )
        svg.append(
            f'<text x="{legend_x + 22}" y="{y + 1}" font-family="sans-serif" font-size="12" fill="#cbd5e1">{html.escape(implementation)}</text>'
        )

    for implementation, impl_points in by_impl.items():
        color = COLORS.get(implementation, "#f59e0b")
        coords = [
            (x_for(point["node_count"]), y_for(point["mean"]), point)
            for point in impl_points
        ]
        polyline = " ".join(f"{x:.2f},{y:.2f}" for x, y, _ in coords)
        svg.append(
            f'<polyline points="{polyline}" fill="none" stroke="{color}" stroke-width="3"/>'
        )
        for x, y, point in coords:
            label = f"{point['mean']:,.0f}"
            svg.append(f'<circle cx="{x:.2f}" cy="{y:.2f}" r="5" fill="{color}"/>')
            svg.append(
                f'<text x="{x:.2f}" y="{y - 10:.2f}" text-anchor="middle" font-family="monospace" font-size="10" fill="#e2e8f0">{label}</text>'
            )

    svg.append("</svg>")
    return "\n".join(svg) + "\n"


def _write_charts(rows: list[dict], out_dir: Path) -> list[Path]:
    paths = []
    for metric, title, y_label, filename in CHARTS:
        points = _aggregate(rows, metric)
        svg = _render_svg(points, metric, title, y_label)
        path = out_dir / filename
        path.write_text(svg, encoding="utf-8")
        paths.append(path)
    return paths


def main() -> int:
    parser = argparse.ArgumentParser(
        description="Create simple SVG plots from evaluation CSV files."
    )
    parser.add_argument("--inputs", required=True, help="Comma-separated CSV paths")
    parser.add_argument("--out-dir", default="results/evaluation/plots")
    args = parser.parse_args()

    rows = _load_rows(args.inputs.split(","))
    out_dir = Path(args.out_dir)
    out_dir.mkdir(parents=True, exist_ok=True)

    datapoints = _write_datapoints(rows, out_dir)
    aggregates = _write_aggregates(rows, out_dir)
    changes = _write_comparison_changes(rows, out_dir)
    charts = _write_charts(rows, out_dir)

    print(datapoints)
    print(aggregates)
    print(changes)
    for chart in charts:
        print(chart)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
