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
cd /home/shubham/290_copy/collaborative_edge_mesh
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

## Run the Complete Experiment Matrix

Generate the 1,760 selected configurations. Each configuration contains 30 repetitions, producing 52,800 expected result rows:

```bash
uv run python scripts/generate_experiment_matrix.py \
  --repetitions 30 \
  --base-seed 7301 \
  --out scripts/final_experiment_matrix.csv
```

Validate the matrix without starting experiments:

```bash
uv run python scripts/run_reviewer_matrix.py \
  scripts/final_experiment_matrix.csv \
  --dry-run \
  --require-complete
```

Run the complete matrix:

```bash
uv run python scripts/run_reviewer_matrix.py \
  scripts/final_experiment_matrix.csv \
  --out results/final_experiment \
  --require-complete
```

The runner executes configurations one at a time and supports resuming completed configurations. Use `--no-resume` to force a configuration to run again.

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
- **UDP Gossip** service broadcasting state to peers every 5 seconds
- **Hash-chain log** for tamper-evident auditing
- **Merkle root** computation for quick divergence detection