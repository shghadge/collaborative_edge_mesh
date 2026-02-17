.PHONY: build test test-crdt test-services

UV ?= uv
PYTEST ?= $(UV) run pytest

build:
	@docker compose build

test:
	@$(PYTEST) -q

test-crdt:
	@$(PYTEST) tests/test_crdt_payloads.py -q

test-services:
	@$(PYTEST) tests/test_intake_service.py tests/test_gossip_service.py -q
