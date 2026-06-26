#!/usr/bin/env python3
import argparse
import csv
import random
import statistics
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

from src.crdt.state import NodeState

EVENT_TYPES = (
    "water_level",
    "temperature",
    "shelter_occupancy",
    "road_status",
    "wind_speed",
    "air_quality",
)


def _percentile(values: list[float], percentile: float) -> float:
    if not values:
        return 0.0
    ordered = sorted(values)
    index = min(
        len(ordered) - 1, max(0, round((percentile / 100.0) * (len(ordered) - 1)))
    )
    return float(ordered[index])


def _seed_initial_events(
    nodes: list[NodeState], events_per_node: int, rng: random.Random
) -> int:
    event_index = 0
    for node in nodes:
        for _ in range(events_per_node):
            event_type = EVENT_TYPES[event_index % len(EVENT_TYPES)]
            category = "resource" if event_type == "shelter_occupancy" else "sensor"
            node.record_event(
                f"event-{event_index}",
                event_type,
                {
                    "value": rng.randint(1, 100),
                    "location": f"loc-{rng.randint(1, max(1, len(nodes) // 2))}",
                },
                category=category,
            )
            event_index += 1
    return event_index


def _converged(nodes: list[NodeState]) -> bool:
    roots = {node.merkle_root() for node in nodes}
    return len(roots) == 1


def run_trial(
    *,
    node_count: int,
    events_per_node: int,
    fanout: int,
    drop_rate: float,
    max_rounds: int,
    rng: random.Random,
) -> dict:
    nodes = [NodeState(f"edge-{index}") for index in range(node_count)]
    event_count = _seed_initial_events(nodes, events_per_node, rng)
    messages = 0
    delivered = 0
    infection_curve = []

    if _converged(nodes):
        return {
            "rounds": 0,
            "converged": True,
            "messages": messages,
            "delivered": delivered,
            "event_count": event_count,
            "final_unique_roots": 1,
            "infection_curve": "1.0",
        }

    for round_number in range(1, max_rounds + 1):
        order = list(range(node_count))
        rng.shuffle(order)
        for source_index in order:
            source = nodes[source_index]
            candidates = [index for index in range(node_count) if index != source_index]
            rng.shuffle(candidates)
            for target_index in candidates[:fanout]:
                messages += 1
                if rng.random() < drop_rate:
                    continue
                nodes[target_index].merge(source)
                delivered += 1

        root_counts = {}
        for node in nodes:
            root = node.merkle_root()
            root_counts[root] = root_counts.get(root, 0) + 1
        infection_curve.append(max(root_counts.values()) / node_count)
        if len(root_counts) == 1:
            return {
                "rounds": round_number,
                "converged": True,
                "messages": messages,
                "delivered": delivered,
                "event_count": event_count,
                "final_unique_roots": 1,
                "infection_curve": ";".join(
                    f"{value:.3f}" for value in infection_curve
                ),
            }

    return {
        "rounds": max_rounds,
        "converged": False,
        "messages": messages,
        "delivered": delivered,
        "event_count": event_count,
        "final_unique_roots": len({node.merkle_root() for node in nodes}),
        "infection_curve": ";".join(f"{value:.3f}" for value in infection_curve),
    }


def summarize(rows: list[dict]) -> dict:
    rounds = [row["rounds"] for row in rows if row["converged"]]
    return {
        "trials": len(rows),
        "converged_trials": sum(1 for row in rows if row["converged"]),
        "median_rounds": statistics.median(rounds) if rounds else None,
        "p95_rounds": _percentile(rounds, 95) if rounds else None,
        "p99_rounds": _percentile(rounds, 99) if rounds else None,
        "max_rounds": max(rounds) if rounds else None,
        "mean_messages": round(statistics.mean(row["messages"] for row in rows), 2),
        "mean_delivered": round(statistics.mean(row["delivered"] for row in rows), 2),
    }


def main() -> int:
    parser = argparse.ArgumentParser(
        description="Run an in-memory Monte Carlo gossip convergence simulation."
    )
    parser.add_argument("--node-count", type=int, default=15)
    parser.add_argument("--events-per-node", type=int, default=3)
    parser.add_argument("--fanout", type=int, default=2)
    parser.add_argument("--drop-rate", type=float, default=0.0)
    parser.add_argument("--trials", type=int, default=1000)
    parser.add_argument("--max-rounds", type=int, default=100)
    parser.add_argument("--seed", type=int, default=290)
    parser.add_argument("--out", default="results/monte_carlo.csv")
    args = parser.parse_args()

    if args.node_count < 2:
        print("--node-count must be at least 2", file=sys.stderr)
        return 2
    if args.fanout < 1:
        print("--fanout must be at least 1", file=sys.stderr)
        return 2
    if not 0.0 <= args.drop_rate < 1.0:
        print("--drop-rate must be in [0.0, 1.0)", file=sys.stderr)
        return 2

    rng = random.Random(args.seed)
    rows = []
    for trial in range(args.trials):
        trial_seed = rng.randrange(2**63)
        trial_rng = random.Random(trial_seed)
        row = run_trial(
            node_count=args.node_count,
            events_per_node=args.events_per_node,
            fanout=args.fanout,
            drop_rate=args.drop_rate,
            max_rounds=args.max_rounds,
            rng=trial_rng,
        )
        row.update(
            {
                "trial": trial,
                "seed": trial_seed,
                "node_count": args.node_count,
                "events_per_node": args.events_per_node,
                "fanout": args.fanout,
                "drop_rate": args.drop_rate,
            }
        )
        rows.append(row)

    out_path = Path(args.out)
    out_path.parent.mkdir(parents=True, exist_ok=True)
    fieldnames = [
        "trial",
        "seed",
        "node_count",
        "events_per_node",
        "fanout",
        "drop_rate",
        "rounds",
        "converged",
        "messages",
        "delivered",
        "event_count",
        "final_unique_roots",
        "infection_curve",
    ]
    with out_path.open("w", newline="", encoding="utf-8") as handle:
        writer = csv.DictWriter(handle, fieldnames=fieldnames)
        writer.writeheader()
        writer.writerows(rows)

    summary = summarize(rows)
    for key, value in summary.items():
        print(f"{key}: {value}")
    print(out_path)

    return 0


if __name__ == "__main__":
    raise SystemExit(main())
