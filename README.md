# Collaborative Edge Mesh

CRDT-based edge mesh for disaster analytics. Nodes sync data using gossip protocol and merge state using conflict-free data types.

## Setup

```bash
cd /home/shubham/290_copy/collaborative_edge_mesh
uv sync
```

## Build

```bash
make build   # build docker images
```

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