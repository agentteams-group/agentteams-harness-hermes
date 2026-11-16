"""Installed SDK adapter contracts. No model/tool continuation is claimed."""
import asyncio
import json
import os
from pathlib import Path
import tempfile
from types import SimpleNamespace
from unittest.mock import AsyncMock, Mock, patch

from gateway.config import PlatformConfig
from hermes_matrix.overlay_adapter import MatrixAdapter


async def check(workspace):
    (workspace / "openclaw.json").write_text(json.dumps({"team_id": "team"}))
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
    async def admit(event):
        observed.append(event)
        event._gateway_accepted = True
    adapter.handle_message = admit
    with patch("task_recovery.enabled", return_value=True), patch("task_recovery.Recovery", return_value=recovery):
        await adapter._recover_tasks()
        assert len(observed) == 1
        event = observed[0]
        assert event.text == "Resume step two from checkpoint three"
        assert event._agentteams_recovery["task_id"] == "stable-task"
        assert event.source.thread_id is None  # No forged Matrix thread/event.
        assert "stable-task:2" in adapter._event_session_key(event)
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
        recovery.save_reply.return_value = {"execution": {"checkpoint": cp}}
        recorded = Mock()
        await adapter._send_final_text(event, adapter._event_session_key(event), "Final answer", {}, False, 0, recorded)
        recovery.save_reply.assert_called_once_with("stable-task", "team", "Final answer")
        assert adapter._client.send_message_event.call_args.kwargs["txn_id"] == "stable-reply"
        assert recorded.call_args.args[0].success is True
        # A delivered progress turn does not complete the authoritative Task.
        cp["delivery"] = "delivered"
        task["execution"]["generation"] = 3
        await adapter._recover_tasks()
        assert len(observed) == 2
        task["status"] = "submitted"
        task["execution"]["generation"] = 4
        await adapter._recover_tasks()
        assert len(observed) == 2
    print("Hermes installed SDK recovery event/session shape, single admission and stable saved-reply delivery: passed (mocked Task client)")


if __name__ == "__main__":
    with tempfile.TemporaryDirectory() as directory:
        asyncio.run(check(Path(directory)))
