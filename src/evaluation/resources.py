import re
import random
from typing import Any, cast

RESOURCE_MODES = {"uniform", "heterogeneous"}
DEFAULT_RESOURCE_PROFILE = "balanced_edge_v1"
RESOURCE_PROFILES: dict[str, dict[str, Any]] = {
    DEFAULT_RESOURCE_PROFILE: {
        "uniform_control": {
            "cpus": 0.5,
            "memory": "512m",
            "memory_mib": 512,
        },
        "tiers": (
            {
                "tier": "low",
                "weight": 2,
                "cpus": 0.25,
                "memory": "256m",
                "memory_mib": 256,
            },
            {
                "tier": "medium",
                "weight": 2,
                "cpus": 0.5,
                "memory": "512m",
                "memory_mib": 512,
            },
            {
                "tier": "high",
                "weight": 1,
                "cpus": 1.0,
                "memory": "1g",
                "memory_mib": 1024,
            },
        ),
    }
}


def resource_profile_block_size(profile_name: str) -> int:
    profile = RESOURCE_PROFILES.get(profile_name)
    if profile is None:
        raise ValueError(f"unknown resource profile: {profile_name}")
    return sum(int(tier["weight"]) for tier in profile["tiers"])


def memory_limit_bytes(value: str | None) -> int | None:
    if not value:
        return None
    match = re.fullmatch(r"(\d+(?:\.\d+)?|\.\d+)([kmgt]?i?b?)", value.lower())
    if match is None:
        raise ValueError(f"invalid Docker memory limit: {value}")
    amount = float(match.group(1))
    unit = match.group(2)
    power = {
        "": 0,
        "b": 0,
        "k": 1,
        "kb": 1,
        "ki": 1,
        "kib": 1,
        "m": 2,
        "mb": 2,
        "mi": 2,
        "mib": 2,
        "g": 3,
        "gb": 3,
        "gi": 3,
        "gib": 3,
        "t": 4,
        "tb": 4,
        "ti": 4,
        "tib": 4,
    }[unit]
    return int(amount * (1024**power))


def build_resource_assignments(
    *,
    node_count: int,
    resource_mode: str,
    resource_profile: str | None,
    uniform_cpus: float | None,
    uniform_memory: str | None,
    repetition_seed: int | None,
) -> list[dict[str, Any]]:
    if resource_mode not in RESOURCE_MODES:
        raise ValueError("resource_mode must be uniform or heterogeneous")

    if resource_mode == "uniform":
        memory_bytes = memory_limit_bytes(uniform_memory)
        return [
            {
                "node_id": f"node-{index}",
                "container": f"edge-node-{index}",
                "tier": "uniform",
                "cpus": uniform_cpus,
                "memory": uniform_memory,
                "memory_mib": (
                    memory_bytes / (1024 * 1024) if memory_bytes is not None else None
                ),
            }
            for index in range(1, node_count + 1)
        ]

    profile_name = resource_profile or DEFAULT_RESOURCE_PROFILE
    profile = RESOURCE_PROFILES.get(profile_name)
    if profile is None:
        raise ValueError(f"unknown resource profile: {profile_name}")

    block_size = resource_profile_block_size(profile_name)
    if node_count % block_size != 0:
        raise ValueError(
            f"{profile_name} requires a node count divisible by {block_size}"
        )

    tiers: list[dict[str, Any]] = []
    for _ in range(node_count // block_size):
        for tier in profile["tiers"]:
            tiers.extend([tier] * int(tier["weight"]))
    random.Random(repetition_seed if repetition_seed is not None else 0).shuffle(tiers)

    return [
        {
            "node_id": f"node-{index}",
            "container": f"edge-node-{index}",
            "tier": tier["tier"],
            "cpus": tier["cpus"],
            "memory": tier["memory"],
            "memory_mib": tier["memory_mib"],
        }
        for index, tier in enumerate(tiers, start=1)
    ]


def resource_totals(assignments: list[dict[str, Any]]) -> dict[str, float | int | None]:
    cpu_values = [assignment.get("cpus") for assignment in assignments]
    memory_values = [assignment.get("memory_mib") for assignment in assignments]
    cpu_total = (
        round(sum(float(cast(float, value)) for value in cpu_values), 3)
        if all(value is not None for value in cpu_values)
        else None
    )
    memory_total = (
        round(sum(float(cast(float, value)) for value in memory_values), 3)
        if all(value is not None for value in memory_values)
        else None
    )
    return {
        "total_cpus": cpu_total,
        "total_memory_mib": memory_total,
    }
