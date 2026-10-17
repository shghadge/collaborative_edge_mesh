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
        self.last_body = None

    async def __aenter__(self):
        return self

    async def __aexit__(self, exc_type, exc, traceback):
        return False

    def post(self, url: str, data=None, headers=None) -> _FakeResponse:
        self.last_body = data
        return self._response


class _BlockingSession(_FakeSession):
    def __init__(self, payload: dict, started: asyncio.Event, release: asyncio.Event):
        super().__init__(payload)
        self._response = _BlockingResponse(payload, started, release)


class GossipDeltaRepairTests(unittest.IsolatedAsyncioTestCase):
    def setUp(self):
        config = Config()
        config.node_id = "edge-1"
        config.http_port = 8000
        self.service = GossipService(config, NodeState("edge-1"))
        self.addr = ("10.0.0.2", 9000)

    @staticmethod
    def _message(delta_vv: dict) -> dict:
        return {
            "sender": "edge-2",
            "source_http_port": 8000,
            "delta_vv": delta_vv,
            "merkle_root": "remote-root",
            "state_version": 1,
        }

    async def test_skips_summary_whose_mutations_are_already_held(self):
        self.service.state.delta_vv.update({"edge-2": 7, "edge-3": 2})

        with patch(
            "src.services.gossip.aiohttp.ClientSession",
            side_effect=AssertionError("stale summary started an HTTP request"),
        ):
            await self.service._maybe_pull_delta_from_sender(
                self.addr, self._message({"edge-2": 7, "edge-3": 1})
            )

        self.assertEqual(self.service.stats["delta_pulls_started"], 0)
        self.assertEqual(self.service.stats["delta_pulls_skipped_stale"], 1)

    async def test_skips_a_second_pull_while_peer_pull_is_inflight(self):
        delta_payload = {"node_id": "edge-2", "delta_vv": {"edge-2": 8}, "state": None}
        started = asyncio.Event()
        release = asyncio.Event()

        with patch(
            "src.services.gossip.aiohttp.ClientSession",
            return_value=_BlockingSession(delta_payload, started, release),
        ):
            first_pull = asyncio.create_task(
                self.service._maybe_pull_delta_from_sender(
                    self.addr, self._message({"edge-2": 8})
                )
            )
            await started.wait()
            try:
                await self.service._maybe_pull_delta_from_sender(
                    self.addr, self._message({"edge-2": 8})
                )
            finally:
                release.set()
            await first_pull

        self.assertEqual(self.service.stats["delta_pulls_started"], 1)
        self.assertEqual(self.service.stats["delta_pulls_skipped_inflight"], 1)

    async def test_gap_response_repairs_from_inline_full_state(self):
        source = NodeState("edge-2", delta_log_limit=1)
        for index in range(3):
            source.record_event(
                f"event-{index}",
                "water_level",
                {"location": f"bridge-{index}", "value": index},
                category="sensor",
            )
        gap_payload = source.get_deltas_for({})
        self.assertEqual(gap_payload["fallback"], "gap")

        with patch(
            "src.services.gossip.aiohttp.ClientSession",
            return_value=_FakeSession(gap_payload),
        ):
            for _ in range(2):
                await self.service._maybe_pull_delta_from_sender(
                    self.addr, self._message(source.delta_vv)
                )

        self.assertEqual(self.service.state.merkle_root(), source.merkle_root())
        self.assertEqual(self.service.stats["delta_pulls_started"], 1)
        self.assertEqual(self.service.stats["delta_pulls_gap"], 1)
        self.assertEqual(self.service.stats["delta_pulls_skipped_stale"], 1)
        self.assertEqual(self.service.stats["repair_pulls_started"], 0)

    async def test_delta_pull_counts_tracking_metadata_bytes_separately(self):
        source = NodeState("edge-2")
        for index in range(3):
            source.record_event(
                f"event-{index}",
                "water_level",
                {
                    "location": f"bridge-{index}",
                    "value": index,
                    "tracking_id": f"t-{index}",
                },
                category="sensor",
            )
        payload = source.get_deltas_for({})
        tracking = payload["state"]["first_seen_tracking"]
        self.assertEqual(len(tracking), 3)

        with patch(
            "src.services.gossip.aiohttp.ClientSession",
            return_value=_FakeSession(payload),
        ):
            await self.service._maybe_pull_delta_from_sender(
                self.addr, self._message(source.delta_vv)
            )

        expected = len(json.dumps(tracking, separators=(",", ":")).encode())
        self.assertEqual(self.service.stats["tracking_bytes_received"], expected)
        self.assertLess(expected, self.service.stats["delta_bytes_received"])

    async def test_failed_pull_is_retried_by_the_next_summary(self):
        with patch(
            "src.services.gossip.aiohttp.ClientSession",
            side_effect=OSError("connection refused"),
        ):
            for _ in range(2):
                await self.service._maybe_pull_delta_from_sender(
                    self.addr, self._message({"edge-2": 1})
                )

        self.assertEqual(self.service.stats["delta_pulls_started"], 2)
        self.assertEqual(self.service.stats["repair_pulls_failed"], 2)
        self.assertEqual(self.service.state.delta_vv, {})

    async def test_pull_request_sends_local_version_vector_and_mode(self):
        self.service.config.sync_mode = "naive_key_delta"
        self.service.state.delta_vv["edge-1"] = 4
        session = _FakeSession(
            {"node_id": "edge-2", "delta_vv": {"edge-2": 1}, "state": None}
        )

        with patch(
            "src.services.gossip.aiohttp.ClientSession",
            return_value=session,
        ):
            await self.service._maybe_pull_delta_from_sender(
                self.addr, self._message({"edge-2": 1})
            )

        self.assertEqual(
            json.loads(session.last_body),
            {"vv": {"edge-1": 4}, "mode": "naive_key_delta"},
        )

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
                    **self._message({"edge-2": 1}),
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
                    **self._message({"edge-2": 1}),
                    "peer_observations": [],
                },
                self.addr,
            )

        state_pull.assert_awaited_once()
        delta_pull.assert_not_awaited()


if __name__ == "__main__":
    unittest.main()
