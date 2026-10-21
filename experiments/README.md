# Experiments

The three experiments from the master's project report, their results, and the scripts to run and plot them.

| Experiment | Question | What changes |
|---|---|---|
| 1. Baseline | How does the mesh behave, and how much did the new version improve? | Mesh size (5, 10, 15 nodes) and partitions; earlier vs improved version |
| 2. Constrained resources | What happens when some nodes have less CPU and memory? | Device resources only |
| 3. Increased load | What happens with more events per node? | Events per node (30, 90, 180, 300) at 15 nodes |

Each experiment is defined by a file in `configs/`. Every configuration is run 10 times with all four
synchronization methods.

## Folders

- `configs/`: one configuration file per experiment
- `results/previous/`: the runs used in the report
  - `runs/`: original runner output
  - `raw/`: the same runs, one file per experiment
  - `summary/`: the numbers behind each graph
  - `figures/`: the graphs
  - `shared_item_test/`: an earlier test behind Table 7 of the report (kept as a record, not rerun)
- `results/new/`: a new folder for every run of `run_experiments.sh`

## Run an experiment

```bash
make docker-rebuild                   # start the system
experiments/run_experiments.sh 2      # 1, 2, 3 or all
```

Results, summary and graphs go into a new folder in `results/new/`, so earlier results are never overwritten.
To continue an interrupted run, add its folder: `experiments/run_experiments.sh 2 experiments/results/new/<folder>`.

## Summarize and plot

```bash
python3 experiments/results/summarize.py previous
uv run --extra report python experiments/results/plot.py previous
```

Replace `previous` with a folder from `results/new/` to summarize or plot a new run.

## Notes

- Comparing constrained with baseline resources needs Experiment 1 in the same folder, so run `all` for full results.
- The earlier version of the system can't be rerun here; its results come from branch `paper-rerun-10x`.
- A 5-node mesh can't be split 70/30, so the 5-node 70/30 points use the 80/20 runs (same groups).
