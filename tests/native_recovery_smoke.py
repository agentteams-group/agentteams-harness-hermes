"""Installed SDK adapter contracts. No model/tool continuation is claimed."""
import asyncio
import json
import os
from pathlib import Path
import tempfile
from types import SimpleNamespace
from unittest.mock import AsyncMock, Mock, patch

from gateway.config import PlatformConfig, GatewayConfig
from gateway.run import GatewayRunner
from gateway.session import SessionStore
from hermes_matrix.overlay_adapter import MatrixAdapter


async def check(workspace):
    # Real Controller-generated OpenClaw config has no team_id. Assignment
    # scope comes from the authenticated self-discovery API, not this cache.
    (workspace / "openclaw.json").write_text(json.dumps({"agents": {"defaults": {}}}))
    os.environ.update(AGENTTEAMS_WORKER_HOME=str(workspace), AGENTTEAMS_PROCESS_ID="actual-start",
                      AGENTTEAMS_CONTROLLER_URL="http://task-client-fixture.invalid")
    task = {"task_id": "stable-task", "status": "in_progress", "room_id": "!task:test", "execution": {
        "generation": 2, "checkpoint": {"version": 3, "completed_steps": ["one"], "remaining_steps": ["two", "three"]}}}
    recovery = Mock()
    recovery.discover.return_value = {"instance_id": "new-start", "tasks": [{"team_id": "team", "task": task, "etag": "read-version"}]}
    recovery.claim.return_value = {"task": task, "spec": "Do one, two, three", "saved_result": "Saved answer"}
    recovery.current.return_value = task
    recovery.resume_text.return_value = "Resume step two from checkpoint three"
    adapter = MatrixAdapter(PlatformConfig(enabled=True))
    adapter._user_id = "@alice:test"
    adapter._client = SimpleNamespace(send_message_event=AsyncMock(return_value="$sent"))
    observed = []
    runner = GatewayRunner.__new__(GatewayRunner)
    runner.config = SimpleNamespace(multiplex_profiles=False)
    runner.session_store = SessionStore(workspace / "sessions", GatewayConfig())
    runner._is_session_running = Mock(return_value=False)
    async def admit(self, event):
        observed.append(event)
        event._gateway_accepted = True
    from types import MethodType
    adapter._message_handler = MethodType(admit, runner)
    adapter.handle_message = adapter._message_handler
    with patch("task_recovery.enabled", return_value=True), patch("task_recovery.Recovery", return_value=recovery):
        await adapter._recover_tasks()
        assert len(observed) == 1
        event = observed[0]
        assert event.text == "Resume step two from checkpoint three"
        assert event._agentteams_recovery["task_id"] == "stable-task"
        assert event.internal is True
        assert event.allow_gateway_control is False
        # Exercise the actual downstream SDK authorization gate. Adapter queue
        # acceptance alone previously hid a rejection of our own Matrix ID.
        runner._is_user_authorized_for_source = Mock(return_value=False)
        runner._scale_to_zero_note_real_inbound = Mock()
        runner._hm_pre_gateway_dispatch_hook = AsyncMock(return_value=event)
        assert await runner._hm_admit_event(event) == (event, event.source, True)
        runner._is_user_authorized_for_source.assert_not_called()
        event.internal = False
        assert await runner._hm_admit_event(event) is None
        runner._is_user_authorized_for_source.assert_called_once()
        event.internal = True
        assert event.source.thread_id is None  # No forged Matrix thread/event.
        key = adapter._event_session_key(event)
        assert key == runner._session_key_for_source(event.source)
        first_session = runner.session_store.lookup_by_session_key(key).session_id
        assert event._agentteams_recovery["session_id"] == first_session
        adapter._client.send_message_event.assert_not_called()
        await adapter._recover_tasks()
        assert len(observed) == 1  # Same generation is admitted once.
        cp = {"version": 4, "delivery": "pending", "reply_txn": "stable-reply"}
        task["execution"]["checkpoint"] = cp
        await adapter._recover_tasks()
        assert len(observed) == 1
        assert adapter._client.send_message_event.call_args.kwargs["txn_id"] == "stable-reply"
        recovery.delivered.assert_called_once_with("stable-task", "team", "$sent")
        adapter._client.send_message_event.reset_mock()
        recovery.prepare_reply.return_value = ({"execution": {"checkpoint": cp}}, "Saved authoritative answer")
        recorded = Mock()
        await adapter._send_final_text(event, adapter._event_session_key(event), "Final answer", {}, False, 0, recorded)
        recovery.prepare_reply.assert_called_once_with("stable-task", "team", "Final answer")
        assert adapter._client.send_message_event.call_args.args[2]["body"] == "Saved authoritative answer"
        assert adapter._client.send_message_event.call_args.kwargs["txn_id"] == "stable-reply"
        assert recorded.call_args.args[0].success is True
        # A delivered progress turn does not complete the authoritative Task.
        cp["delivery"] = "delivered"
        task["execution"]["generation"] = 3
        await adapter._recover_tasks()
        assert len(observed) == 2
        assert runner.session_store.lookup_by_session_key(key).session_id != first_session
        task["status"] = "submitted"
        task["execution"]["generation"] = 4
        await adapter._recover_tasks()
        assert len(observed) == 2
        task["status"] = "in_progress"
        task["execution"]["generation"] = 5
        await adapter._recover_tasks("stable-task", source_event=SimpleNamespace(event_id="$original-assignment", sender="@leader:test"))
        assert len(observed) == 3
        assert observed[-1].message_id == "$original-assignment"
        assert observed[-1].source.user_id == "@leader:test"
    print("Hermes installed SDK recovery event/session shape, single admission and stable saved-reply delivery: passed (mocked Task client)")


if __name__ == "__main__":
    with tempfile.TemporaryDirectory() as directory:
        asyncio.run(check(Path(directory)))
