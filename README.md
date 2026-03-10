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

## Testing

```bash
make test            # run all unit tests
make test-crdt       # run CRDT-focused unit tests
make test-services   # run service-focused unit tests

# equivalent direct command
uv run pytest -q
```

## Notes

The project follows a minimal command policy:
- Keep Makefile targets limited to essential build and test commands
- Use `uv` for all Python-related workflows
- Add/update unit tests for every feature change and run them after implementation

Core components:
- **FastAPI** HTTP server for event ingestion and state queries
- **UDP Gossip** service broadcasting state to peers every 5 seconds
- **Hash-chain log** for tamper-evident auditing
- **Merkle root** computation for quick divergence detection