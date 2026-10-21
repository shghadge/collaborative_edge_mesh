#!/usr/bin/env python3
"""Run fast local checks: unit tests, compileall, and ruff check.

Usage: python3 scripts/run_quick_checks.py [--skip-ruff] [--skip-compileall].
Does not use Docker or run the evaluation matrix.
"""

import argparse
import subprocess
import sys
import time
from collections.abc import Sequence


DEFAULT_PATHS = ("src", "tests", "scripts")


def _run(label: str, command: Sequence[str]) -> None:
    """Run command and print its label and elapsed time; raises on nonzero exit."""
    print(f"==> {label}: {' '.join(command)}", flush=True)
    started = time.monotonic()
    subprocess.run(command, check=True)
    elapsed = time.monotonic() - started
    print(f"<== {label}: {elapsed:.2f}s", flush=True)


def main() -> int:
    """Run the selected checks in order and stop at the first failure.

    Returns 0 if all pass, otherwise the failing command's exit code (or 1).
    """
    parser = argparse.ArgumentParser(
        description="Run fast local checks without Docker or the evaluation matrix."
    )
    parser.add_argument(
        "--skip-ruff",
        action="store_true",
        help="Skip ruff check when iterating on non-Python artifacts.",
    )
    parser.add_argument(
        "--skip-compileall",
        action="store_true",
        help="Skip bytecode compilation.",
    )
    args = parser.parse_args()

    checks: list[tuple[str, list[str]]] = [
        ("unit tests", [sys.executable, "-m", "unittest", "discover", "tests"]),
    ]
    if not args.skip_compileall:
        checks.append(
            ("compileall", [sys.executable, "-m", "compileall", *DEFAULT_PATHS])
        )
    if not args.skip_ruff:
        checks.append(("ruff", [sys.executable, "-m", "ruff", "check", *DEFAULT_PATHS]))

    started = time.monotonic()
    try:
        for label, command in checks:
            _run(label, command)
    except subprocess.CalledProcessError as error:
        print(f"quick checks failed during {error.cmd}", file=sys.stderr)
        return error.returncode or 1

    elapsed = time.monotonic() - started
    print(f"quick checks passed in {elapsed:.2f}s")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
