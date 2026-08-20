#!/usr/bin/env python3
import argparse
import hashlib
import json
import os
import platform
import subprocess
import sys
from datetime import datetime, timezone
from pathlib import Path

SOURCE_PATHS = (
    "Dockerfile",
    "docker-compose.yml",
    "pyproject.toml",
    "uv.lock",
    "src",
    "scripts/run_evaluation_matrix.py",
    "scripts/run_reviewer_matrix.py",
    "scripts/reviewer_experiment_matrix.csv",
    "scripts/reviewer_capture_matrix.csv",
    "scripts/summarize_reviewer_results.py",
    "scripts/prepare_paper_evidence.py",
    "scripts/generate_reviewer_documents.py",
    "scripts/capture_evaluation_environment.py",
    "scripts/plot_reviewer_results.py",
    "scripts/analyze_packet_capture.py",
    "tests",
)


def _command(arguments: list[str]) -> str | None:
    try:
        completed = subprocess.run(
            arguments,
            check=True,
            capture_output=True,
            text=True,
            timeout=30,
        )
    except (OSError, subprocess.CalledProcessError, subprocess.TimeoutExpired):
        return None
    return completed.stdout.strip()


def _cpu_model() -> str | None:
    try:
        for line in Path("/proc/cpuinfo").read_text(encoding="utf-8").splitlines():
            if line.startswith("model name"):
                return line.split(":", 1)[1].strip()
    except OSError:
        pass
    return None


def _memory_bytes() -> int | None:
    try:
        for line in Path("/proc/meminfo").read_text(encoding="utf-8").splitlines():
            if line.startswith("MemTotal:"):
                return int(line.split()[1]) * 1024
    except OSError:
        pass
    return None


def _source_files(root: Path) -> list[Path]:
    files = []
    for relative in SOURCE_PATHS:
        path = root / relative
        if path.is_file():
            files.append(path)
        elif path.is_dir():
            files.extend(
                candidate
                for candidate in path.rglob("*")
                if candidate.is_file()
                and "__pycache__" not in candidate.parts
                and candidate.suffix not in {".pyc", ".pyo"}
            )
    return sorted(set(files))


def _checksums(root: Path) -> dict[str, str]:
    return {
        str(path.relative_to(root)): hashlib.sha256(path.read_bytes()).hexdigest()
        for path in _source_files(root)
    }


def _docker_images() -> dict[str, dict[str, str | None]]:
    images = {}
    for name in (
        "collaborative_edge_mesh-edge-node",
        "collaborative_edge_mesh-gateway",
        "collaborative_edge_mesh-simulator",
    ):
        image_id = _command(["docker", "image", "inspect", name, "--format", "{{.Id}}"])
        if image_id:
            images[name] = {
                "id": image_id,
                "repo_digest": _command(
                    [
                        "docker",
                        "image",
                        "inspect",
                        name,
                        "--format",
                        '{{join .RepoDigests ","}}',
                    ]
                ),
            }
    return images


def main() -> int:
    parser = argparse.ArgumentParser(
        description="Capture evaluation environment metadata."
    )
    parser.add_argument("--out", type=Path, required=True)
    args = parser.parse_args()
    root = Path(__file__).resolve().parents[1]
    payload = {
        "captured_at": datetime.now(timezone.utc).isoformat(),
        "hostname": platform.node(),
        "platform": platform.platform(),
        "kernel": platform.release(),
        "machine": platform.machine(),
        "cpu_model": _cpu_model(),
        "logical_cpu_count": os.cpu_count(),
        "memory_total_bytes": _memory_bytes(),
        "python_version": sys.version,
        "docker_image_note": (
            "Dynamic edge containers use collaborative_edge_mesh-simulator:latest "
            "with the node entrypoint override."
        ),
        "docker_server_version": _command(
            ["docker", "version", "--format", "{{.Server.Version}}"]
        ),
        "docker_images": _docker_images(),
        "container_default_limits": {"cpus": 1.0, "memory": "512m"},
        "source_revision": None,
        "source_revision_note": "Repository has no committed HEAD; use source_sha256.",
        "source_sha256": _checksums(root),
    }
    args.out.parent.mkdir(parents=True, exist_ok=True)
    args.out.write_text(
        json.dumps(payload, indent=2, sort_keys=True) + "\n", encoding="utf-8"
    )
    print(args.out)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
