#!/usr/bin/env bash
# Runs the experiments one stage at a time (or all in order) into a new results folder, then summarizes the runs
# and draws the figures for that folder.
#
# Usage (from the repository root, with the stack running: make docker-rebuild):
#   experiments/run_experiments.sh 1      # Experiment 1: improved-version runs of the version comparison + baseline
#   experiments/run_experiments.sh 2      # Experiment 2: constrained resources
#   experiments/run_experiments.sh 3      # Experiment 3: increased load
#   experiments/run_experiments.sh all    # 1, then 2, then 3
#   experiments/run_experiments.sh 2 experiments/results/new/<run folder>   # continue an interrupted run
#
# Every call creates experiments/results/new/run_<date>_<time>_exp<stage>/ with runs/ (the runner's output, one
# folder per configuration), summary/ and figures/, so running an experiment again never overwrites earlier
# results. Pass an existing run folder as the second argument to continue it: configurations already completed
# there are skipped. Within one folder, the 32 configurations that Experiment 3 shares with Experiments 1 and 2
# (15 nodes, 90 events per node) run only once. The results used in the report are in experiments/results/previous/.
set -euo pipefail

EXPERIMENTS=$(cd "$(dirname "$0")" && pwd)
REPO=$(dirname "$EXPERIMENTS")
CONFIGS=$EXPERIMENTS/configs

stage=${1:-}
case $stage in
  1) stages=(version_comparison experiment1_baseline) ;;
  2) stages=(experiment2_constrained_resources) ;;
  3) stages=(experiment3_increased_load) ;;
  all) stages=(version_comparison experiment1_baseline experiment2_constrained_resources experiment3_increased_load) ;;
  *) echo "usage: $0 <1|2|3|all> [existing run folder to continue]" >&2; exit 2 ;;
esac
curl -sf -o /dev/null http://localhost:8000/ || { echo "simulator not reachable on http://localhost:8000; run 'make docker-rebuild'" >&2; exit 2; }

if [ -n "${2:-}" ]; then
  [ -d "$2/runs" ] || { echo "not a run folder (no runs/ inside): $2" >&2; exit 2; }
  FOLDER=$(cd "$2" && pwd)
else
  FOLDER=$EXPERIMENTS/results/new/run_$(date '+%Y%m%d_%H%M%S')_exp$stage
fi
RUNS=$FOLDER/runs
mkdir -p "$RUNS"
cd "$REPO"
log() { echo "[$(date '+%F %T')] $*" | tee -a "$FOLDER/run.log"; }
log "results folder: $FOLDER (git $(git rev-parse --short HEAD))"

# The version-comparison runs use the conference paper's test (cold topology, 8 s partition), which the matrix
# runner does not support, so each row of the configuration file is passed to the evaluation runner directly.
run_version_comparison() {
  tail -n +2 "$CONFIGS/experiment1_version_comparison.csv" | while IFS=, read -r case_id condition_id sweep \
      sync_mode retention delta_log nodes scenario split events partition_events partition_wait convergence_wait \
      poll concurrency network repetitions seed warm_topology _cpus _memory; do
    log "$case_id"
    topology=(); [ "$warm_topology" = "False" ] && topology=(--cold-topology)
    uv run python scripts/run_evaluation_matrix.py \
      --sync-mode "$sync_mode" --event-id-retention-limit "$retention" --delta-log-limit "$delta_log" \
      --node-counts "$nodes" --events-per-node "$events" --partition-events-per-node "$partition_events" \
      --scenarios "$scenario" --partition-splits "$split" --partition-wait-seconds "$partition_wait" \
      --convergence-wait-seconds "$convergence_wait" --poll-interval-seconds "$poll" \
      --event-concurrency "$concurrency" --network-profile "$network" --repetitions "$repetitions" \
      --scenario-seed "$seed" "${topology[@]}" --case-id "$case_id" --condition-id "$condition_id" \
      --sweep "$sweep" --resume --out "$RUNS/version_comparison/$case_id" 2>&1 | tail -1
  done
}

for name in "${stages[@]}"; do
  log "$name start"
  if [ "$name" = version_comparison ]; then
    run_version_comparison
  else
    uv run python scripts/run_reviewer_matrix.py "$CONFIGS/$name.csv" --out "$RUNS" 2>&1 | tee -a "$FOLDER/run.log"
  fi
  log "$name done"
done

python3 "$EXPERIMENTS/results/summarize.py" "$FOLDER" | tee -a "$FOLDER/run.log"
uv run --extra report python "$EXPERIMENTS/results/plot.py" "$FOLDER" | tee -a "$FOLDER/run.log"
