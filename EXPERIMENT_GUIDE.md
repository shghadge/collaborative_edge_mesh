# Run One Experiment Configuration

This guide runs one selected row from `scripts/final_experiment_matrix.csv`. It does not run the complete 1,760-configuration matrix.

## 1. Prepare the project

Run all commands from the implementation directory:

```bash
cd /home/shubham/290_copy/collaborative_edge_mesh
uv sync
make docker-rebuild
```

Wait until the simulator evaluation API responds:

```bash
curl -s http://localhost:8000/evaluation/status | uv run python -m json.tool
```

The simulator and gateway start first. The experiment runner creates and removes the required edge-node containers.

## 2. Select one configuration

Open `scripts/final_experiment_matrix.csv`. Copy its header and exactly one row into a new file named `scripts/selected_experiment.csv`.

Example: the following selected configuration runs a five-node, clean-LAN, baseline experiment with true-delta synchronization and 30 repetitions:

```csv
case_id,condition_id,enabled,sweep,sync_mode,event_id_retention_limit,delta_log_limit,node_count,scenario,partition_split,network_profile,events_per_node,partition_events_per_node,event_concurrency,offered_rate_per_second,partition_wait_seconds,convergence_wait_seconds,poll_interval_seconds,repetitions,seed,docker_cpus,docker_memory,resource_mode,resource_profile
scale-n05-true_delta,scale-n05,1,scale,true_delta,100,1000,5,baseline,,clean_lan,15,0,25,,0,45,2,30,7331,1.0,512m,uniform,
```

For any other configuration, keep the same header and replace the example row with the required row from the full CSV. Keep `enabled` set to `1`.

For a quick non-publication smoke run, change only `repetitions` in the copied row from `30` to `1`. Keep the original value for formal experiment results.

For the equal-budget mixed-device experiment, select a row from `../masters_report/experiment_3_resource_heterogeneity.csv`. Its uniform rows use 0.50 CPU and 512 MiB per node. Its heterogeneous rows use `resource_mode=heterogeneous` with `resource_profile=balanced_edge_v1`; node counts must be divisible by five. Heterogeneous rows run on a warm topology: each repetition's tier assignment is applied to the running containers in place, without recreating them.

For the repeated LWW-register comparison, use `../masters_report/experiment_2_shared_key_comparison.csv`. The `shared_key` scenario first synchronizes one update per node, resets measurement counters without clearing CRDT state, and then measures 15 updates per node to `sensor:shared_site:water_level`.

For the CRDT-specific delta-benefit comparison, use `../masters_report/experiment_2_delta_benefit_comparison.csv`. The `shared_counter` and `shared_set` scenarios first synchronize 60 updates per node, reset measurement counters without clearing CRDT state, and then measure 15 updates per node. The accumulated PN-counter slots and OR-set tags give true delta a smaller mathematical payload than naive key delta; warmup traffic is excluded.

For the focused resource-pressure comparison, use `../masters_report/experiment_3_focused_true_delta_mixed_resources.csv`. It holds synchronization constant at `true_delta`, sends 300 events per node at 10 events/s per node, and compares the equal-budget uniform and `balanced_edge_v1` layouts. The larger delta log avoids workload-induced forced gaps. Treat it as a CPU-allocation experiment; calibration did not make memory binding.

For the focused partition-recovery comparison, use `../masters_report/experiment_4_partition_recovery_comparison.csv`. It holds synchronization at `true_delta`, resources at 1 CPU and 512 MiB per node, and total workload at 90 observations per node. Partitioned rows send 45 observations per node before isolation and 45 during isolation, wait 30 additional seconds, heal the mesh, and allow up to 120 seconds for recovery. The matrix tests a near-even split at 5, 10, and 15 nodes and adds 70/30 and 80/20 shapes at 15 nodes. This file is kept as the record of completed runs: it still uses the removed `hard_partition` profile and no longer validates under the current runner.

A partition is the `partition_heal` scenario with a `partition_split`; it runs on top of any network profile (`clean_lan`, `constrained_wifi`, `degraded`, `severe`). Partition cells apply netem like every other cell, and both event phases are sent at `offered_rate_per_second`.

### Combined partition, resource, and load experiment

The final-report experiment (ADR-021) varies nodes (5, 10, 15), devices (uniform or `balanced_edge_v1`), partition (none, 50/50, 80/20), total events per node (30, 90, 180, 300), and sync protocol together in 144 configurations on `clean_lan`. It supersedes Experiments 3 and 4.

Generate the matrix:

```bash
uv run python scripts/generate_experiment_matrix.py --design combined
```

For a pilot, generate one repetition per configuration into a separate file (add `--repetitions 1` and point the output at, for example, `../masters_report/experiment_6_combined_pilot.csv`) and run that file into a separate results directory.

Dry-run, then run the full matrix:

```bash
uv run python scripts/run_reviewer_matrix.py ../masters_report/experiment_6_combined.csv \
  --out ../masters_report/results/combined --dry-run
uv run python scripts/run_reviewer_matrix.py ../masters_report/experiment_6_combined.csv \
  --out ../masters_report/results/combined
```

Report per configuration: `converged_within_timeout`, `time_to_last_node_s`, `recovery_after_heal_s` (post-heal recovery in partition cells), `sync_bytes_total`/`bytes_per_accepted_event` and protocol-only `protocol_sync_bytes_total`/`protocol_bytes_per_accepted_event`, and for mixed-device cells `last_node_resource_tier` with `partition_group_a`/`partition_group_b`.

## 3. Validate the selected configuration

Run a dry run before starting containers:

```bash
uv run python scripts/run_reviewer_matrix.py \
  scripts/selected_experiment.csv \
  --out results/single_configuration \
  --dry-run
```

For the example above, the expected output is:

```text
validated_configurations=1
expected_result_rows=30
scale=1
```

Do not add `--require-complete`; that option intentionally requires the full 1,760-configuration matrix.

## 4. Run the experiment

```bash
uv run python scripts/run_reviewer_matrix.py \
  scripts/selected_experiment.csv \
  --out results/single_configuration
```

The runner performs the following work automatically for each repetition:

1. Creates the configured number of edge-node containers.
2. Applies the selected synchronization mode, network profile, and uniform or per-node resource limits.
3. Reads Docker's applied CPU and memory limits and fails the run if they do not match the requested assignment.
4. Injects the configured events and, when selected, partition/heal or repeated-key behavior.
5. Waits up to the configured convergence cutoff. Repeated-key scenarios complete an unmeasured warmup convergence before starting their measured workload.
6. Collects correctness, latency, bandwidth, state-size, per-node CPU, and per-node memory measurements.
7. Saves the result artifacts and exact resource assignments.

Impaired network profiles shape mesh traffic only; the evaluator's own traffic to nodes bypasses the emulated link, so `write_latency_*` measures node-side processing and excludes the emulated link.

The command resumes an already completed matching configuration by default. Add `--no-resume` only when the same configuration must be run again from the beginning.

## 5. Monitor a running experiment

In another terminal:

```bash
curl -s http://localhost:8000/evaluation/status | uv run python -m json.tool
docker stats --no-stream
```

The first command shows the current case, scenario, repetition, and progress. The second shows live container resource use.

## 6. Find and check the results

At completion, the runner prints the result CSV path and manifest JSON path. For the example, files are stored below:

```text
results/single_configuration/
└── scale-n05-true_delta/
    └── true_delta_retention_100_clean_lan/
        ├── <run-id>.csv
        ├── <run-id>.json
        └── <run-id>-results.json
```

- `<run-id>.csv`: one measurement row per repetition; use this for analysis.
- `<run-id>.json`: exact configuration, resource assignments, verification results, and artifact paths.
- `<run-id>-results.json`: raw results returned by the simulator.

Check these fields in every CSV row:

| Field | Expected value for a successful converged run |
|---|---|
| `accepted_events` | Equal to `attempted_events` |
| `missing_at_cutoff_count` | `0` |
| `tracking_coverage_percent` | `100.0` |
| `is_converged` | `true` |
| `unique_merkle_roots` | `1` |
| `network_rules_verified` | `true` |
| `resource_limit_verified` | `true` |
| `container_failure_count` | `0` |
| `container_oom_count` | `0` |

Use `time_to_last_node_s` as the primary end-to-end mesh propagation time. Use `sync_bytes_total` or `bytes_per_accepted_event` for communication cost. A cutoff or failed-convergence row is still an experiment result and should not be silently deleted.

For partition runs, `recovery_after_heal_s` measures recovery after healing, while `time_to_last_node_s` (and `convergence_time_s`, which equals it when events are tracked) preserves the end-to-end delay experienced by observations. `actual_fault_duration_s` includes both fault-period event submission and the configured post-submission wait. Use `partition_group_a` and `partition_group_b` rather than the requested ratio when reporting discrete group sizes.

For heterogeneous runs, `node_resource_assignments` records the exact node-to-tier mapping. `per_node_resource_metrics` preserves each node's quota-relative CPU and memory measurements; `resource_tier_metrics` summarizes low, medium, and high tiers. Use `last_node_resource_tier` and the tier arrival-latency fields to test whether constrained nodes determine the convergence tail. `cpu_throttled_period_percent` and `cpu_throttled_time_s` report Docker CPU-quota pressure accumulated during the measured run; compare these with accepted throughput and p95 write latency rather than interpreting peak CPU alone.

## 7. Clean up

After collecting the artifacts:

```bash
make docker-clean
```

This removes the simulator, gateway, edge-node containers, and experiment Docker resources. Result files under `results/` remain on disk.
