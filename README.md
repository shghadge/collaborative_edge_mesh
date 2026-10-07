# Collaborative Edge Mesh

CRDT-based edge mesh for disaster analytics. Nodes sync data using gossip protocol and merge state using conflict-free data types.

## Runtime Topology

- **Simulator service**: hosts dashboard + simulation control APIs
- **Gateway service**: polling/merge/metrics backend
- **Edge nodes**: created dynamically from simulator (no default nodes at startup)

After `make docker-rebuild`:
- Dashboard + simulator API: `http://localhost:8000`
- Gateway API: `http://localhost:7000`

## Setup

```bash
cd collaborative_edge_mesh
uv sync
```

## Build

```bash
make build   # build docker images
make docker-rebuild  # clean + rebuild + start simulator/gateway
```

## Create Nodes (On-Demand)

- Start with zero edge nodes
- Use dashboard batch create or API: `POST /nodes/batch?count=<n>` on simulator
- First created node starts as `node-1`, then increments


## Run an Individual Scenario

Start the simulator and gateway first:

```bash
make docker-rebuild
```

The evaluation runner creates the edge-node containers, applies the selected synchronization mode and resource limits, submits events, collects metrics, and writes CSV/JSON results under `results/`.

### Baseline scenario

Runs connected nodes on the clean-LAN profile:

```bash
uv run python scripts/run_evaluation_matrix.py \
  --case-id demo-baseline \
  --sync-mode true_delta \
  --network-profile clean_lan \
  --node-counts 5 \
  --events-per-node 15 \
  --scenarios baseline \
  --repetitions 3 \
  --scenario-seed 7301 \
  --convergence-wait-seconds 45 \
  --cold-topology \
  --out results/demo
```

### Partition and healing scenario

Creates a hard partition, writes events on both sides, heals the partition, and measures recovery:

```bash
uv run python scripts/run_evaluation_matrix.py \
  --case-id demo-partition \
  --sync-mode true_delta \
  --network-profile hard_partition \
  --node-counts 5 \
  --events-per-node 15 \
  --partition-events-per-node 15 \
  --partition-splits 0.5 \
  --partition-wait-seconds 30 \
  --scenarios partition_heal \
  --repetitions 3 \
  --scenario-seed 7301 \
  --convergence-wait-seconds 45 \
  --cold-topology \
  --out results/demo
```

Use `--partition-splits 0.5,0.7,0.8` to test 50/50, 70/30, and 80/20 partitions.

### High-load scenario

Sends more events to stress throughput, latency, and synchronization:

```bash
uv run python scripts/run_evaluation_matrix.py \
  --case-id demo-high-load \
  --sync-mode optimized_full_state \
  --network-profile clean_lan \
  --node-counts 15 \
  --events-per-node 90 \
  --offered-rate-per-second 25 \
  --scenarios high_load \
  --repetitions 3 \
  --scenario-seed 7301 \
  --convergence-wait-seconds 60 \
  --cold-topology \
  --out results/demo
```

### Degraded-network scenario

Applies the selected delay, jitter, packet-loss, and bandwidth profile before submitting events:

```bash
uv run python scripts/run_evaluation_matrix.py \
  --case-id demo-degraded \
  --sync-mode full_state \
  --network-profile degraded \
  --node-counts 10 \
  --events-per-node 15 \
  --scenarios baseline \
  --repetitions 3 \
  --scenario-seed 7301 \
  --docker-cpus 0.5 \
  --docker-memory 256m \
  --convergence-wait-seconds 120 \
  --cold-topology \
  --out results/demo
```

Supported synchronization modes are `full_state`, `optimized_full_state`, `naive_key_delta`, and `true_delta`. Supported network profiles are `clean_lan`, `constrained_wifi`, `degraded`, `severe`, and `hard_partition`.

## AIBThings Fig. 3 Rerun (Partition-Heal, 10 Repetitions)

This branch contains the experiment behind the revised Fig. 3 and Table II of the AIBThings 2026 paper (#297): 4 synchronization methods × 5/10/15 nodes × 50/50, 70/30, 80/20 partitions × 10 repetitions = 360 runs.

Each run uses freshly created containers, each limited to 1 CPU and 500 MiB of memory (`DOCKER_CPUS` and `DOCKER_MEMORY` in `scripts/run_paper_rerun.py`). Every node receives 15 events, then an iptables partition is applied and every node receives 3 more events. The partition is held for 8 seconds and then removed. Convergence is checked every 2 seconds from the heal, with a 25-second budget. Repetition `r` uses seed `9100 + r` for all four methods.

Run it in resumable batches:

```bash
uv run python scripts/run_paper_rerun.py up                    # build images from this tree and start simulator + gateway
uv run python scripts/run_paper_rerun.py run --max-minutes 60  # stop starting new runs after 60 minutes; rerun to resume
uv run python scripts/run_paper_rerun.py status                # completed runs per configuration
uv run python scripts/run_paper_rerun.py summarize             # mean/SD tables
uv run python scripts/plot_paper_rerun_figure.py               # Fig. 3 with standard-deviation error bars
uv run python scripts/run_paper_rerun.py down                  # stop all containers
```

Results are in `results/paper_rerun_10x/`:

| File | Contents |
|---|---|
| `<method>-n<nodes>-s<split>-r<rep>/` | Raw output of one run: result CSV, run manifest, and full results JSON |
| `all_runs.csv` | All 360 runs, one row each: method, nodes, split, convergence time, sync bytes, missing events, seed |
| `summary.csv`, `summary.md` | Mean, sample standard deviation, and median for each of the 36 configurations |
| `fig3_rerun.png` | Revised Fig. 3 |

## Inspect a Running Experiment

In another terminal:

```bash
curl -s http://localhost:8000/evaluation/status | python -m json.tool
docker stats --no-stream
docker ps --format "table {{.Names}}\t{{.Status}}\t{{.Ports}}"
```

For a running node, inspect the active network controls:

```bash
docker exec edge-node-1 tc qdisc show dev eth0
docker exec edge-node-1 tc class show dev eth0
docker exec edge-node-1 iptables -S
```

The evaluator records the exact network settings, synchronization mode, resource limits, repetition seed, convergence results, event coverage, latency, throughput, communication bytes, CPU, memory, throttling, and OOM status in each result CSV.

## Clean Up

Stop and remove the simulator, gateway, and dynamically created edge nodes:

```bash
make docker-clean
```
## Notes

The project follows a minimal command policy:
- Keep Makefile targets limited to essential build and runtime commands
- Use `uv` for all Python-related workflows

Core components:
- **FastAPI** HTTP server for event ingestion and state queries
- **UDP Gossip** service sending Merkle-root summaries to up to 3 peers every 5 seconds; differing nodes pull state or deltas over HTTP
- **Hash-chain log** for tamper-evident auditing
- **Merkle root** computation for quick divergence detection