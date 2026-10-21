# Shared-item test

An earlier test behind Table 7 of the report: naive key delta and true delta when every node updates the same item.
It is kept as a record and is not part of the three experiments; `run_experiments.sh` does not run it.

- Every node updates one shared item: an OR-Set (`shared_set`), a PN-Counter (`shared_counter`), or an
  LWW-Register (`shared_key`).
- 5, 10, and 15 nodes; 15 observations per node; no partition; clean network; 1 CPU per node; 10 repetitions.
- Run on 30 Sep 2026 with a development version of the system, before the final change to how traffic is measured,
  so its values compare the two methods but are not comparable in size with the three experiments.

`configs/` holds the configurations as run, `runs/` the runner's output. `summarize.py previous` writes
`previous/summary/shared_item_test.csv`: synchronization data per accepted observation in KiB, per item, method, and
mesh size. The 15-node rows are Table 7.
