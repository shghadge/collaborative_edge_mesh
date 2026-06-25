.PHONY: quick smoke build format format-check docker-clean docker-rebuild

UV ?= uv
RUFF ?= $(UV) run ruff

quick:
	@$(UV) run python scripts/run_quick_checks.py

smoke:
	@$(UV) run python scripts/run_evaluation_smoke.py --sync-mode true_delta

build:
	@docker compose build

format:
	@$(RUFF) format .

format-check:
	@$(RUFF) format --check .

docker-clean:
	@docker ps -aq --filter "name=edge-node-" | xargs -r docker rm -f
	@docker ps -aq --filter "name=gateway" | xargs -r docker rm -f
	@docker ps -aq --filter "name=simulator" | xargs -r docker rm -f
	@docker compose down -v --remove-orphans

docker-rebuild:
	@$(MAKE) docker-clean
	@docker compose up -d --build