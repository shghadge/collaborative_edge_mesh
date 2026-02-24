import asyncio
from datetime import datetime
import uuid
import aiohttp


async def run_split_brain_then_heal(
    docker_manager,
    gateway_service,
    isolate_seconds: float = 8.0,
    verify_polls: int = 2,
):
    action_id = str(uuid.uuid4())
    started_at = datetime.utcnow().isoformat()

    split_result = await asyncio.to_thread(docker_manager.create_split_brain)
    if split_result.get("status") != "split_brain":
        return {
            "action_id": action_id,
            "action": "split_brain_then_heal",
            "status": "failed",
            "started_at": started_at,
            "finished_at": datetime.utcnow().isoformat(),
            "message": split_result.get("message", "Split-brain setup failed"),
            "split_result": split_result,
        }

    await gateway_service.poll_once()
    divergent_after_split = gateway_service.is_divergent

    await asyncio.sleep(max(isolate_seconds, 0.0))

    heal_result = await asyncio.to_thread(docker_manager.heal_all)

    polls = max(int(verify_polls), 1)
    verification_states = []
    for _ in range(polls):
        await gateway_service.poll_once()
        verification_states.append(
            {
                "is_divergent": gateway_service.is_divergent,
                "poll_count": gateway_service.poll_count,
            }
        )

    converged = not gateway_service.is_divergent

    return {
        "action_id": action_id,
        "action": "split_brain_then_heal",
        "status": "completed" if converged else "partial",
        "started_at": started_at,
        "finished_at": datetime.utcnow().isoformat(),
        "split_result": split_result,
        "heal_result": heal_result,
        "isolate_seconds": isolate_seconds,
        "verify_polls": polls,
        "divergent_after_split": divergent_after_split,
        "converged": converged,
        "verification_states": verification_states,
    }


async def run_bootstrap_events_convergence(
    docker_manager,
    gateway_service,
    create_nodes: int = 0,
    events_per_node: int = 1,
    verify_polls: int = 3,
    event_sender=None,
):
    action_id = str(uuid.uuid4())
    started_at = datetime.utcnow().isoformat()

    created = []
    create_failures = []
    for _ in range(max(int(create_nodes), 0)):
        try:
            created.append(await asyncio.to_thread(docker_manager.create_node))
        except Exception as error:
            create_failures.append(str(error))

    nodes = await asyncio.to_thread(docker_manager.list_nodes)
    targets = [
        node for node in nodes if node.get("internal_url") or node.get("url")
    ]

    if not targets:
        return {
            "action_id": action_id,
            "action": "bootstrap_events_convergence",
            "status": "failed",
            "started_at": started_at,
            "finished_at": datetime.utcnow().isoformat(),
            "message": "No reachable node URLs available for event submission",
            "created": created,
            "create_failures": create_failures,
        }

    async def _default_sender(node_url, payload):
        async with aiohttp.ClientSession(
            timeout=aiohttp.ClientTimeout(total=5)
        ) as session:
            async with session.post(f"{node_url}/event", json=payload) as response:
                body = await response.json()
                return {
                    "ok": response.status < 400,
                    "status_code": response.status,
                    "body": body,
                }

    async def _wait_for_node_ready(node_url, attempts=10, delay_seconds=0.4):
        timeout = aiohttp.ClientTimeout(total=3)
        for attempt in range(max(int(attempts), 1)):
            try:
                async with aiohttp.ClientSession(timeout=timeout) as session:
                    async with session.get(f"{node_url}/state/merkle") as response:
                        if response.status < 400:
                            return True
            except Exception:
                pass

            if attempt < attempts - 1:
                await asyncio.sleep(max(delay_seconds, 0.0))

        return False

    sender = event_sender or _default_sender

    if event_sender is None:
        readiness = []
        for node in targets:
            node_url = node.get("internal_url") or node.get("url")
            node_id = node.get("node_id", node.get("name", "unknown"))
            is_ready = await _wait_for_node_ready(node_url)
            readiness.append({"node_id": node_id, "node_url": node_url, "ready": is_ready})

        not_ready = [entry for entry in readiness if not entry["ready"]]
        if not_ready:
            await asyncio.sleep(0.5)

    send_results = []
    sample_types = [
        ("water_level", "sensor", "bridge_north", 3.2),
        ("shelter_occupancy", "resource", "shelter_east", 12),
        ("road_status", "infrastructure", "highway_101", "blocked"),
    ]

    for idx, node in enumerate(targets):
        node_url = node.get("internal_url") or node.get("url")
        node_id = node.get("node_id", node.get("name", "unknown"))
        for event_idx in range(max(int(events_per_node), 1)):
            event_type, category, location, value = sample_types[
                (idx + event_idx) % len(sample_types)
            ]
            payload = {
                "type": event_type,
                "value": value,
                "location": location,
                "category": category,
                "metadata": {
                    "source": "scenario_bootstrap",
                    "scenario_action_id": action_id,
                },
            }

            if category == "resource":
                payload["operation"] = "increment"
            if category == "infrastructure":
                payload["operation"] = "add"

            try:
                response = await sender(node_url, payload)
                send_results.append(
                    {
                        "node_id": node_id,
                        "node_url": node_url,
                        "event_type": event_type,
                        "category": category,
                        "ok": bool(response.get("ok")),
                        "status_code": response.get("status_code"),
                    }
                )
            except Exception as error:
                send_results.append(
                    {
                        "node_id": node_id,
                        "node_url": node_url,
                        "event_type": event_type,
                        "category": category,
                        "ok": False,
                        "status_code": None,
                        "error": str(error),
                    }
                )

    polls = max(int(verify_polls), 1)
    verification_states = []
    for _ in range(polls):
        await gateway_service.poll_once()
        verification_states.append(
            {
                "is_divergent": gateway_service.is_divergent,
                "poll_count": gateway_service.poll_count,
            }
        )

    successful_events = sum(1 for entry in send_results if entry.get("ok"))
    failed_events = len(send_results) - successful_events
    converged = not gateway_service.is_divergent

    return {
        "action_id": action_id,
        "action": "bootstrap_events_convergence",
        "status": "completed" if converged else "partial",
        "started_at": started_at,
        "finished_at": datetime.utcnow().isoformat(),
        "created_count": len(created),
        "create_failures": create_failures,
        "target_node_count": len(targets),
        "events_per_node": events_per_node,
        "successful_events": successful_events,
        "failed_events": failed_events,
        "converged": converged,
        "verification_states": verification_states,
        "send_results": send_results,
    }
