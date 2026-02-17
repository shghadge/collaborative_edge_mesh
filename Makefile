.PHONY: build test test-unit test-integration test-crdt test-services

UV ?= uv
PYTEST ?= PYTHONPATH=. $(UV) run pytest

build:
	@docker compose build

test:
	@$(MAKE) test-unit
	@$(MAKE) test-integration

test-unit:
	@$(PYTEST) tests --ignore=tests/integration -q

test-integration:
	@$(PYTEST) tests/integration -q

test-crdt:
	@$(PYTEST) tests/test_crdt_payloads.py -q

test-services:
	@$(PYTEST) tests/test_intake_service.py tests/test_gossip_service.py tests/test_gateway_service.py -q
