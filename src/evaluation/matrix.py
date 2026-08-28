import csv
from collections import Counter, defaultdict
from pathlib import Path
from typing import Any

METHODS = (
    "full_state",
    "optimized_full_state",
    "naive_key_delta",
    "true_delta",
)
METHOD_RETENTION = {
    "full_state": 0,
    "optimized_full_state": 100,
    "naive_key_delta": 100,
    "true_delta": 100,
}
NODE_COUNTS = (3, 5, 7, 10, 12, 15, 20, 25, 30, 40, 50)
NETWORK_PROFILES = ("clean_lan", "constrained_wifi", "degraded", "severe")
CPU_LIMITS = (0.25, 0.5, 1.0)
MEMORY_LIMITS_MIB = (100, 256, 512)
EXPECTED_SWEEP_COUNTS = {
    "scale": 44,
    "load": 60,
    "network": 48,
    "partition_shape": 24,
    "resource_interaction": 1584,
}
EXPECTED_CONFIGURATION_COUNT = sum(EXPECTED_SWEEP_COUNTS.values())
EXPECTED_CONDITION_COUNT = EXPECTED_CONFIGURATION_COUNT // len(METHODS)
MATRIX_COLUMNS = (
    "case_id",
    "condition_id",
    "enabled",
    "sweep",
    "sync_mode",
    "event_id_retention_limit",
    "delta_log_limit",
    "node_count",
    "scenario",
    "partition_split",
    "network_profile",
    "events_per_node",
    "partition_events_per_node",
    "event_concurrency",
    "offered_rate_per_second",
    "partition_wait_seconds",
    "convergence_wait_seconds",
    "poll_interval_seconds",
    "repetitions",
    "seed",
    "docker_cpus",
    "docker_memory",
    "observability",
)


def _slug_number(value: float) -> str:
    return f"{value:g}".replace(".", "p")


def _base_case(
    *,
    condition_id: str,
    sweep: str,
    node_count: int,
    scenario: str,
    network_profile: str,
    events_per_node: int,
    partition_events_per_node: int,
    offered_rate_per_second: float | str,
    partition_split: float | str,
    partition_wait_seconds: float,
    convergence_wait_seconds: float,
    repetitions: int,
    seed: int,
    docker_cpus: float,
    docker_memory: str,
) -> dict[str, Any]:
    return {
        "condition_id": condition_id,
        "enabled": 1,
        "sweep": sweep,
        "node_count": node_count,
        "scenario": scenario,
        "partition_split": partition_split,
        "network_profile": network_profile,
        "events_per_node": events_per_node,
        "partition_events_per_node": partition_events_per_node,
        "event_concurrency": 25,
        "offered_rate_per_second": offered_rate_per_second,
        "partition_wait_seconds": partition_wait_seconds,
        "convergence_wait_seconds": convergence_wait_seconds,
        "poll_interval_seconds": 2,
        "repetitions": repetitions,
        "seed": seed,
        "docker_cpus": docker_cpus,
        "docker_memory": docker_memory,
        "observability": "off",
    }


def generate_experiment_cases(
    *, repetitions: int = 30, base_seed: int = 7301
) -> list[dict[str, Any]]:
    if repetitions < 1:
        raise ValueError("repetitions must be at least 1")

    cases: list[dict[str, Any]] = []
    condition_index = 0

    def add_condition(**settings: Any) -> None:
        nonlocal condition_index
        condition_seed = base_seed + condition_index * repetitions
        condition_index += 1
        base = _base_case(repetitions=repetitions, seed=condition_seed, **settings)
        for method in METHODS:
            cases.append(
                {
                    "case_id": f"{base['condition_id']}-{method}",
                    **base,
                    "sync_mode": method,
                    "event_id_retention_limit": METHOD_RETENTION[method],
                    "delta_log_limit": 1000,
                }
            )

    for node_count in NODE_COUNTS:
        add_condition(
            condition_id=f"scale-n{node_count:02d}",
            sweep="scale",
            node_count=node_count,
            scenario="baseline",
            network_profile="clean_lan",
            events_per_node=15,
            partition_events_per_node=0,
            offered_rate_per_second="",
            partition_split="",
            partition_wait_seconds=0,
            convergence_wait_seconds=45,
            docker_cpus=1.0,
            docker_memory="512m",
        )

    for node_count in (5, 15, 30):
        for offered_rate in (5, 10, 25, 50, 100):
            add_condition(
                condition_id=f"load-n{node_count:02d}-r{offered_rate:03d}",
                sweep="load",
                node_count=node_count,
                scenario="baseline",
                network_profile="clean_lan",
                events_per_node=90,
                partition_events_per_node=0,
                offered_rate_per_second=offered_rate,
                partition_split="",
                partition_wait_seconds=0,
                convergence_wait_seconds=60,
                docker_cpus=1.0,
                docker_memory="512m",
            )

    for node_count in (10, 25, 50):
        for network_profile in NETWORK_PROFILES:
            add_condition(
                condition_id=f"network-n{node_count:02d}-{network_profile}",
                sweep="network",
                node_count=node_count,
                scenario="baseline",
                network_profile=network_profile,
                events_per_node=15,
                partition_events_per_node=0,
                offered_rate_per_second="",
                partition_split="",
                partition_wait_seconds=0,
                convergence_wait_seconds=45,
                docker_cpus=1.0,
                docker_memory="512m",
            )

    for node_count in (15, 30):
        for partition_split in (0.5, 0.7, 0.8):
            split_label = round(partition_split * 100)
            add_condition(
                condition_id=f"partition-n{node_count:02d}-p{split_label:02d}",
                sweep="partition_shape",
                node_count=node_count,
                scenario="partition_heal",
                network_profile="hard_partition",
                events_per_node=45,
                partition_events_per_node=45,
                offered_rate_per_second="",
                partition_split=partition_split,
                partition_wait_seconds=30,
                convergence_wait_seconds=45,
                docker_cpus=1.0,
                docker_memory="512m",
            )

    for node_count in NODE_COUNTS:
        for network_profile in NETWORK_PROFILES:
            for cpu_limit in CPU_LIMITS:
                for memory_limit in MEMORY_LIMITS_MIB:
                    add_condition(
                        condition_id=(
                            f"resource-n{node_count:02d}-{network_profile}-"
                            f"cpu{_slug_number(cpu_limit)}-mem{memory_limit}"
                        ),
                        sweep="resource_interaction",
                        node_count=node_count,
                        scenario="baseline",
                        network_profile=network_profile,
                        events_per_node=15,
                        partition_events_per_node=0,
                        offered_rate_per_second=25,
                        partition_split="",
                        partition_wait_seconds=0,
                        convergence_wait_seconds=45,
                        docker_cpus=cpu_limit,
                        docker_memory=f"{memory_limit}m",
                    )

    validate_experiment_cases(cases, require_complete=True)
    return cases


def validate_experiment_cases(
    cases: list[dict[str, Any]], *, require_complete: bool = False
) -> None:
    if not cases:
        raise ValueError("experiment matrix contains no enabled configurations")

    case_ids = [str(case.get("case_id", "")).strip() for case in cases]
    if any(not case_id for case_id in case_ids):
        raise ValueError("every configuration requires a case_id")
    duplicate_case_ids = sorted(
        case_id for case_id, count in Counter(case_ids).items() if count > 1
    )
    if duplicate_case_ids:
        raise ValueError(f"duplicate case_id values: {', '.join(duplicate_case_ids)}")

    condition_methods: dict[str, set[str]] = defaultdict(set)
    condition_seeds: dict[str, set[int]] = defaultdict(set)
    for case in cases:
        condition_id = str(case.get("condition_id", "")).strip()
        if not condition_id:
            raise ValueError(f"case {case['case_id']} is missing condition_id")
        method = str(case.get("sync_mode", "")).strip()
        condition_methods[condition_id].add(method)
        condition_seeds[condition_id].add(int(case["seed"]))
        if int(case["repetitions"]) < 1:
            raise ValueError(f"case {case['case_id']} has invalid repetitions")

    if not require_complete:
        return

    for condition_id, methods in condition_methods.items():
        if methods != set(METHODS):
            raise ValueError(
                f"condition {condition_id} does not contain all four methods"
            )
        if len(condition_seeds[condition_id]) != 1:
            raise ValueError(f"condition {condition_id} does not use one paired seed")

    counts = Counter(str(case["sweep"]) for case in cases)
    if dict(counts) != EXPECTED_SWEEP_COUNTS:
        raise ValueError(f"unexpected sweep counts: {dict(sorted(counts.items()))}")
    if len(cases) != EXPECTED_CONFIGURATION_COUNT:
        raise ValueError(
            f"expected {EXPECTED_CONFIGURATION_COUNT} configurations, found {len(cases)}"
        )
    if len(condition_methods) != EXPECTED_CONDITION_COUNT:
        raise ValueError(
            f"expected {EXPECTED_CONDITION_COUNT} conditions, "
            f"found {len(condition_methods)}"
        )


def write_experiment_matrix(path: Path, cases: list[dict[str, Any]]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("w", newline="", encoding="utf-8") as handle:
        writer = csv.DictWriter(handle, fieldnames=MATRIX_COLUMNS)
        writer.writeheader()
        writer.writerows(cases)


def load_experiment_matrix(path: Path) -> list[dict[str, str]]:
    with path.open(newline="", encoding="utf-8") as handle:
        reader = csv.DictReader(handle)
        missing = set(MATRIX_COLUMNS) - set(reader.fieldnames or [])
        if missing:
            raise ValueError(
                f"matrix is missing required columns: {', '.join(sorted(missing))}"
            )
        cases = [
            row
            for row in reader
            if row["enabled"].strip().lower() in {"1", "true", "yes", "on"}
        ]
    validate_experiment_cases(cases)
    return cases
