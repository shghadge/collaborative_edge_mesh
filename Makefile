.PHONY: build up down logs status send-event isolate heal heal-all

build:
	docker compose build

up:
	docker compose up -d

down:
	docker compose down

logs:
	docker compose logs -f

status:
	@echo "--- Node 1 ---"
	@curl -s http://localhost:8001/status | python3 -m json.tool
	@echo "\n--- Node 2 ---"
	@curl -s http://localhost:8002/status | python3 -m json.tool
	@echo "\n--- Node 3 ---"
	@curl -s http://localhost:8003/status | python3 -m json.tool

merkle:
	@echo "Node 1: $$(curl -s http://localhost:8001/state/merkle | python3 -c 'import sys,json; print(json.load(sys.stdin)["merkle_root"][:16])')"
	@echo "Node 2: $$(curl -s http://localhost:8002/state/merkle | python3 -c 'import sys,json; print(json.load(sys.stdin)["merkle_root"][:16])')"
	@echo "Node 3: $$(curl -s http://localhost:8003/state/merkle | python3 -c 'import sys,json; print(json.load(sys.stdin)["merkle_root"][:16])')"

# usage: make send-event NODE=1 TYPE=sensor VALUE=42 LOCATION=zone_a
send-event:
	curl -s -X POST http://localhost:800$(NODE)/event \
		-H "Content-Type: application/json" \
		-d '{"type":"$(TYPE)","value":$(VALUE),"location":"$(LOCATION)"}' | python3 -m json.tool

# usage: make isolate NODE=edge-node-1
isolate:
	docker exec $(NODE) iptables -A INPUT -p udp -s 172.28.0.0/24 -j DROP
	docker exec $(NODE) iptables -A OUTPUT -p udp -d 172.28.0.0/24 -j DROP
	@echo "$(NODE) isolated from gossip"

# usage: make heal NODE=edge-node-1
heal:
	docker exec $(NODE) iptables -F INPUT
	docker exec $(NODE) iptables -F OUTPUT
	@echo "$(NODE) healed"

heal-all:
	@for c in edge-node-1 edge-node-2 edge-node-3; do \
		docker exec $$c iptables -F INPUT 2>/dev/null || true; \
		docker exec $$c iptables -F OUTPUT 2>/dev/null || true; \
	done
	@echo "all partitions healed"

# send realistic disaster events across the mesh
demo:
	@echo "=== Sending disaster events ==="
	@echo ">> Node 1: water level at bridge_north"
	@curl -s -X POST http://localhost:8001/event -H "Content-Type: application/json" \
		-d '{"type":"water_level","value":3.2,"location":"bridge_north","metadata":{"unit":"meters","severity":"warning"}}' | python3 -m json.tool
	@echo ">> Node 2: injured count at shelter_east"
	@curl -s -X POST http://localhost:8002/event -H "Content-Type: application/json" \
		-d '{"type":"injured_count","value":14,"location":"shelter_east","metadata":{"medics_needed":3}}' | python3 -m json.tool
	@echo ">> Node 3: road blocked on highway_101"
	@curl -s -X POST http://localhost:8003/event -H "Content-Type: application/json" \
		-d '{"type":"road_status","value":"blocked","location":"highway_101","metadata":{"cause":"flooding","detour":"route_9"}}' | python3 -m json.tool
	@echo ">> Node 1: shelter capacity"
	@curl -s -X POST http://localhost:8001/event -H "Content-Type: application/json" \
		-d '{"type":"shelter_capacity","value":85,"location":"shelter_east","metadata":{"max_capacity":200,"supplies":"low"}}' | python3 -m json.tool
	@echo ">> Node 2: power outage"
	@curl -s -X POST http://localhost:8002/event -H "Content-Type: application/json" \
		-d '{"type":"power_status","value":"offline","location":"district_5","metadata":{"estimated_restore":"6h","affected_homes":1200}}' | python3 -m json.tool
	@echo ">> Node 3: water level rising"
	@curl -s -X POST http://localhost:8003/event -H "Content-Type: application/json" \
		-d '{"type":"water_level","value":4.1,"location":"bridge_north","metadata":{"unit":"meters","severity":"critical","trend":"rising"}}' | python3 -m json.tool
	@echo "\n=== Waiting for gossip convergence (10s) ==="
	@sleep 10
	@echo "\n=== Merkle roots ==="
	@$(Make) merkle
	@echo "\n=== Node states ==="
	@$(Make) status

# simulate a partition, send events to both sides, then heal and watch convergence
demo-partition:
	@echo "=== Isolating node-1 ==="
	@$(Make) isolate NODE=edge-node-1
	@sleep 2
	@echo "\n>> Node 1 (isolated): emergency water reading"
	@curl -s -X POST http://localhost:8001/event -H "Content-Type: application/json" \
		-d '{"type":"water_level","value":5.8,"location":"bridge_north","metadata":{"unit":"meters","severity":"critical","trend":"rising fast"}}' | python3 -m json.tool
	@echo ">> Node 2 (connected): new injured arrivals"
	@curl -s -X POST http://localhost:8002/event -H "Content-Type: application/json" \
		-d '{"type":"injured_count","value":27,"location":"shelter_east","metadata":{"medics_needed":6,"ambulances_dispatched":2}}' | python3 -m json.tool
	@sleep 5
	@echo "\n=== Merkle roots (should DIFFER) ==="
	@$(Make) merkle
	@echo "\n=== Healing partition ==="
	@$(Make) heal NODE=edge-node-1
	@echo "Waiting for convergence (15s)..."
	@sleep 15
	@echo "\n=== Merkle roots (should MATCH) ==="
	@$(Make) merkle

# run full demo: events then partition test
demo-full:
	@$(MAKE) demo
	@echo "\n\n========================================="
	@echo "=== PARTITION TEST ==="
	@echo "=========================================\n"
	@$(MAKE) demo-partition
