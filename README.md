# Collaborative Edge Mesh

CRDT-based edge mesh for disaster analytics. Nodes sync data using gossip protocol and merge state using conflict-free data types.

## Quick Start

```bash
make build    # build docker images
make up       # start 3 edge nodes
make logs     # watch logs
```

## Send Events

```bash
make send-event NODE=1 TYPE=sensor VALUE=42 LOCATION=zone_a
make send-event NODE=2 TYPE=alert VALUE=100 LOCATION=zone_b
```

## Check State

```bash
make status   # show all node statuses
make merkle   # compare merkle roots (should match after gossip)
```

## Network Partition Testing

```bash
make isolate NODE=edge-node-1   # cut node 1 from gossip
make send-event NODE=1 TYPE=sensor VALUE=99 LOCATION=zone_c  # event only on node 1
make merkle                     # roots will differ
make heal NODE=edge-node-1      # reconnect
# wait ~10 seconds for gossip
make merkle                     # roots should match again
```

Each node runs:
- **FastAPI** HTTP server for event ingestion and state queries
- **UDP Gossip** service broadcasting state to peers every 5 seconds
- **Hash-chain log** for tamper-evident auditing
- **Merkle root** computation for quick divergence detection