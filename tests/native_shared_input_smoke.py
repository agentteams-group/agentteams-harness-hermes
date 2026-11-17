"""Actual pinned mautrix/Hermes callback boundary; authority/transport mocked.

No model, shared native-fact or task-continuation acceptance is claimed here.
"""
import asyncio
import os
from pathlib import Path
import tempfile
import time
from types import SimpleNamespace
from unittest.mock import AsyncMock, patch

from gateway.config import PlatformConfig
from mautrix.types import MessageEvent
from hermes_matrix.overlay_adapter import MatrixAdapter
from hermes_matrix.policies import DualAllowList
from task_recovery import RecoveryUnavailable
from worker_matrix_state import WorkerMatrix


async def check(workspace):
    os.environ.update(AGENTTEAMS_WORKER_HOME=str(workspace), AGENTTEAMS_PROCESS_ID="actual-start",
        AGENTTEAMS_CONTROLLER_URL="http://mock-authority.invalid", AGENTTEAMS_AGENT_ROLE="worker")
    adapter = MatrixAdapter(PlatformConfig(enabled=True))
    adapter._user_id = "@worker:test"
    adapter._dual_allow = DualAllowList(group_policy="open")
    adapter._is_allowed_matrix_room_event = AsyncMock(return_value=True)
    adapter._is_dm_room = AsyncMock(return_value=False)
    adapter._resolve_room_identity = AsyncMock(return_value=SimpleNamespace(
        display_name="Room", room_topic="Topic", server_name="test"))
    adapter._get_display_name = AsyncMock(return_value="Human")
    adapter._require_mention = True
    adapter._auto_thread = False
    adapter._matrix_session_scope = "room"
    adapter._text_batch_delay_seconds = 0
    adapter.handle_message = AsyncMock()
    adapter.send_read_receipt = AsyncMock()
    adapter._startup_ts = time.time() - 10
    order = []
    for name, mentioned, fail in [("message", True, False), ("context", False, False), ("failed", True, True)]:
        content = {"msgtype": "m.text", "body": "Continue work",
            "m.mentions": {"user_ids": [adapter._user_id] if mentioned else []}}
        timestamp = int(time.time() * 1000)
        event = MessageEvent.deserialize({"type": "m.room.message", "room_id": "!room:test",
            "sender": "@human:test", "event_id": "$" + name, "origin_server_ts": timestamp, "content": content})
        def persist(room, event_id, decoded, intent, **kwargs):
            assert room == "!room:test" and event_id == "$" + name
            assert decoded == {"sender": "@human:test", "type": "m.room.message",
                "origin_server_ts": timestamp, "content": event.content.serialize()}
            assert intent == ("message" if mentioned else "context")
            order.append("persist:" + name)
            if fail:
                raise RecoveryUnavailable("input storage unavailable")
        adapter.handle_message.side_effect = lambda event: order.append("admit:" + name)
        with patch.object(WorkerMatrix, "accept", side_effect=persist):
            try:
                await adapter._on_room_message(event)
                assert not fail
            except RecoveryUnavailable:
                assert fail
        await asyncio.sleep(0)
    assert order == ["persist:message", "admit:message", "persist:context", "persist:failed"]
    adapter.send_read_receipt.assert_not_called()
    assert adapter.handle_message.call_count == 1
    assert adapter._history.event_ids("!room:test") == ["$context"]
    print("Hermes pinned SDK decoded input, context ordering, persistence failure and early-read suppression passed (mock authority; no model)")


if __name__ == "__main__":
    with tempfile.TemporaryDirectory() as directory:
        asyncio.run(check(Path(directory)))
