import asyncio
import json
import unittest
from unittest.mock import AsyncMock, patch

from src.config import Config
from src.crdt import NodeState
from src.services.gossip import GossipService


class _FakeResponse:
    def __init__(self, payload: dict):
        self._text = json.dumps(payload)

    async def __aenter__(self):
        return self

    async def __aexit__(self, exc_type, exc, traceback):
        return False

    async def text(self) -> str:
        return self._text


class _BlockingResponse(_FakeResponse):
    def __init__(self, payload: dict, started: asyncio.Event, release: asyncio.Event):
        super().__init__(payload)
        self._started = started
        self._release = release

    async def text(self) -> str:
        self._started.set()
        await self._release.wait()
        return await super().text()


class _FakeSession:
    def __init__(self, payload: dict):
        self._response = _FakeResponse(payload)

    async def __aenter__(self):
        return self

    async def __aexit__(self, exc_type, exc, traceback):
        return False

    def get(self, url: str) -> _FakeResponse:
        return self._response


class _BlockingSession(_FakeSession):
    def __init__(self, payload: dict, started: asyncio.Event, release: asyncio.Event):
        self._response = _BlockingResponse(payload, started, release)


class GossipDeltaRepairTests(unittest.IsolatedAsyncioTestCase):
    def setUp(self):
        config = Config()
        config.node_id = "edge-1"
        config.http_port = 8000
        self.service = GossipService(config, NodeState("edge-1"))
        self.addr = ("10.0.0.2", 9000)

    @staticmethod
    def _message(delta_seq: int) -> dict:
        return {
            "sender": "edge-2",
            "source_http_port": 8000,
            "delta_seq": delta_seq,
            "merkle_root": "remote-root",
            "state_version": delta_seq,
        }

    async def test_skips_an_already_processed_sequence(self):
        self.service.peer_delta_seq["edge-2"] = 7

        with patch(
            "src.services.gossip.aiohttp.ClientSession",
            side_effect=AssertionError("stale summary started an HTTP request"),
        ):
            await self.service._maybe_pull_delta_from_sender(
                self.addr, self._message(7)
            )

        self.assertEqual(self.service.stats["delta_pulls_started"], 0)
        self.assertEqual(self.service.stats["delta_pulls_skipped_stale"], 1)

    async def test_skips_a_second_pull_while_peer_pull_is_inflight(self):
        delta_payload = {
            "node_id": "edge-2",
            "from_seq": 0,
            "to_seq": 8,
            "has_gap": False,
            "deltas": [],
        }
        started = asyncio.Event()
        release = asyncio.Event()

        with patch(
            "src.services.gossip.aiohttp.ClientSession",
            return_value=_BlockingSession(delta_payload, started, release),
        ):
            first_pull = asyncio.create_task(
                self.service._maybe_pull_delta_from_sender(self.addr, self._message(8))
            )
            await started.wait()
            try:
                await self.service._maybe_pull_delta_from_sender(
                    self.addr, self._message(8)
                )
            finally:
                release.set()
            await first_pull

        self.assertEqual(self.service.stats["delta_pulls_started"], 1)
        self.assertEqual(self.service.stats["delta_pulls_skipped_inflight"], 1)
        self.assertEqual(self.service.peer_delta_seq["edge-2"], 8)

    async def test_successful_gap_fallback_advances_peer_frontier(self):
        gap_payload = {
            "node_id": "edge-2",
            "from_seq": 0,
            "to_seq": 9,
            "has_gap": True,
            "deltas": [],
        }
        fallback = AsyncMock(return_value=True)

        with (
            patch(
                "src.services.gossip.aiohttp.ClientSession",
                return_value=_FakeSession(gap_payload),
            ),
            patch.object(
                self.service,
                "_maybe_pull_state_from_sender",
                fallback,
            ),
        ):
            await self.service._maybe_pull_delta_from_sender(
                self.addr, self._message(9)
            )
            await self.service._maybe_pull_delta_from_sender(
                self.addr, self._message(9)
            )

        self.assertEqual(self.service.peer_delta_seq["edge-2"], 9)
        self.assertEqual(self.service.stats["delta_pulls_started"], 1)
        self.assertEqual(self.service.stats["delta_pulls_gap"], 1)
        self.assertEqual(self.service.stats["delta_pulls_skipped_stale"], 1)
        fallback.assert_awaited_once()

    async def test_failed_gap_fallback_does_not_advance_peer_frontier(self):
        gap_payload = {
            "node_id": "edge-2",
            "from_seq": 0,
            "to_seq": 9,
            "has_gap": True,
            "deltas": [],
        }

        with (
            patch(
                "src.services.gossip.aiohttp.ClientSession",
                return_value=_FakeSession(gap_payload),
            ),
            patch.object(
                self.service,
                "_maybe_pull_state_from_sender",
                AsyncMock(return_value=False),
            ),
        ):
            await self.service._maybe_pull_delta_from_sender(
                self.addr, self._message(9)
            )

        self.assertNotIn("edge-2", self.service.peer_delta_seq)

    async def test_naive_key_delta_requests_key_level_payload(self):
        self.service.config.sync_mode = "naive_key_delta"
        payload = {
            "node_id": "edge-2",
            "from_seq": 0,
            "to_seq": 1,
            "has_gap": False,
            "deltas": [],
        }
        session = _FakeSession(payload)

        with (
            patch(
                "src.services.gossip.aiohttp.ClientSession",
                return_value=session,
            ),
            patch.object(session, "get", wraps=session.get) as get,
        ):
            await self.service._maybe_pull_delta_from_sender(
                self.addr, self._message(1)
            )

        request_url = get.call_args.args[0]
        self.assertIn("mode=naive_key_delta", request_url)

    async def test_naive_key_delta_summary_uses_delta_repair(self):
        self.service.config.sync_mode = "naive_key_delta"
        delta_pull = AsyncMock()
        state_pull = AsyncMock()

        with (
            patch.object(
                self.service,
                "_maybe_pull_delta_from_sender",
                delta_pull,
            ),
            patch.object(
                self.service,
                "_maybe_pull_state_from_sender",
                state_pull,
            ),
        ):
            await self.service._handle(
                {
                    "type": "state_summary",
                    **self._message(1),
                    "peer_observations": [],
                },
                self.addr,
            )

        delta_pull.assert_awaited_once()
        state_pull.assert_not_awaited()

    async def test_optimized_full_state_summary_uses_full_state_repair(self):
        self.service.config.sync_mode = "optimized_full_state"
        delta_pull = AsyncMock()
        state_pull = AsyncMock()

        with (
            patch.object(
                self.service,
                "_maybe_pull_delta_from_sender",
                delta_pull,
            ),
            patch.object(
                self.service,
                "_maybe_pull_state_from_sender",
                state_pull,
            ),
        ):
            await self.service._handle(
                {
                    "type": "state_summary",
                    **self._message(1),
                    "peer_observations": [],
                },
                self.addr,
            )

        state_pull.assert_awaited_once()
        delta_pull.assert_not_awaited()


if __name__ == "__main__":
    unittest.main()
