#!/usr/bin/env python3
"""Report-only plotting for Monte Carlo gossip CSVs.

This script intentionally lives outside `run_monte_carlo.py` so the generator
stays dependency-light. Install report dependencies with:

    uv sync --extra report
"""

import argparse
from pathlib import Path

import matplotlib.pyplot as plt
import pandas as pd


def _parse_curve(raw: object) -> list[float]:
    if pd.isna(raw):
        return []
    return [float(value) for value in str(raw).split(";") if value]


def _curve_frame(csv_path: Path) -> tuple[pd.DataFrame, str]:
    df = pd.read_csv(csv_path)
    if df.empty:
        raise ValueError(f"{csv_path} has no rows")
    if "infection_curve" not in df.columns:
        raise ValueError(f"{csv_path} has no infection_curve column")

    curves = [_parse_curve(value) for value in df["infection_curve"]]
    max_len = max((len(curve) for curve in curves), default=0)
    if max_len == 0:
        raise ValueError(f"{csv_path} has no infection curve samples")

    expanded_rows = []
    for trial_index, curve in enumerate(curves):
        for round_index in range(max_len):
            expanded_rows.append(
                {
                    "trial": trial_index,
                    "round": round_index + 1,
                    "infection": curve[round_index]
                    if round_index < len(curve)
                    else 1.0,
                }
            )

    first = df.iloc[0]
    label = (
        f"{csv_path.stem} "
        f"(N={first.get('node_count', '?')}, "
        f"fanout={first.get('fanout', '?')}, "
        f"drop={first.get('drop_rate', '?')})"
    )
    return pd.DataFrame(expanded_rows), label


def _summary_frame(curve_df: pd.DataFrame) -> pd.DataFrame:
    grouped = curve_df.groupby("round")["infection"]
    return pd.DataFrame(
        {
            "round": grouped.mean().index,
            "mean": grouped.mean().values,
            "p10": grouped.quantile(0.10).values,
            "p50": grouped.quantile(0.50).values,
            "p90": grouped.quantile(0.90).values,
        }
    )


def plot_infection_curves(
    csv_paths: list[Path], out_path: Path, title: str | None
) -> None:
    fig, ax = plt.subplots(figsize=(10, 6))
    color_cycle = plt.rcParams["axes.prop_cycle"].by_key()["color"]

    for index, csv_path in enumerate(csv_paths):
        curve_df, label = _curve_frame(csv_path)
        summary = _summary_frame(curve_df)
        color = color_cycle[index % len(color_cycle)]

        ax.plot(
            summary["round"],
            summary["mean"],
            color=color,
            linewidth=2.5,
            label=f"Mean — {label}",
        )
        ax.plot(
            summary["round"],
            summary["p50"],
            color=color,
            linewidth=1.5,
            linestyle="--",
            alpha=0.9,
            label=f"Median — {label}",
        )
        ax.fill_between(
            summary["round"].to_numpy(dtype=float),
            summary["p10"].to_numpy(dtype=float),
            summary["p90"].to_numpy(dtype=float),
            color=color,
            alpha=0.15,
        )

    ax.set_title(title or "Monte Carlo Gossip Infection Curve")
    ax.set_xlabel("Gossip round")
    ax.set_ylabel("Largest converged group / total nodes")
    ax.set_ylim(0.0, 1.05)
    ax.grid(True, alpha=0.25)
    ax.legend(fontsize=8)
    fig.tight_layout()
    out_path.parent.mkdir(parents=True, exist_ok=True)
    fig.savefig(out_path, dpi=160)
    plt.close(fig)


def main() -> int:
    parser = argparse.ArgumentParser(
        description="Plot infection curves from Monte Carlo gossip CSV outputs."
    )
    parser.add_argument("csv", nargs="+", help="Monte Carlo CSV file(s)")
    parser.add_argument(
        "--out",
        default="results/monte_carlo_infection.png",
        help="Output figure path: .png, .svg, .pdf, etc.",
    )
    parser.add_argument("--title")
    args = parser.parse_args()

    csv_paths = [Path(value) for value in args.csv]
    plot_infection_curves(csv_paths, Path(args.out), args.title)
    print(args.out)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
