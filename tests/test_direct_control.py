"""Run the overlay against the pinned native hard-interrupt and shell backend."""
import asyncio
import os
from pathlib import Path
import threading
from types import SimpleNamespace
from unittest.mock import AsyncMock

import pytest

pytest.importorskip("gateway")
from agent.interrupt_control import InterruptControlMixin
from gateway.config import PlatformConfig
from hermes_matrix.overlay_adapter import MatrixAdapter, _NativeMatrixAdapter
from tools.environments.local import LocalEnvironment
from tools.interrupt import set_interrupt


def test_completed_manager_input_is_not_dispatched_after_runtime_switch(tmp_path, monkeypatch):
    from agentteams_receipts import Receipts
    monkeypatch.setenv('AGENTTEAMS_AGENT_ROLE', 'manager')
    adapter = MatrixAdapter.__new__(MatrixAdapter)
    adapter._session_receipts = Receipts(tmp_path)
    adapter._session_receipts.enqueue('!private:test', '$done', 'completed')
    asyncio.run(adapter._on_room_message(SimpleNamespace(room_id='!private:test', event_id='$done')))


def message(event_id, text, action=None, sender="@human:test", run_id="$start"):
    return SimpleNamespace(message_id=event_id, text=text,
        source=SimpleNamespace(chat_id="!dm:test", user_id=sender),
        raw_message={} if action is None else {"com.agentteams.control": {
            "action": action, "agent_id": "@agent:test", "run_id": run_id}})


class NativeAgent(InterruptControlMixin):
    def __init__(self):
        self._execution_thread_id = None
        self._active_children = []
        self._active_children_lock = threading.Lock()
        self.quiet_mode = True


def test_room_identity_reads_members_from_the_actual_mautrix_api(monkeypatch):
    monkeypatch.setenv("MATRIX_ALLOWED_USERS", "@human:test")
    async def check():
        adapter = MatrixAdapter(PlatformConfig(enabled=True))
        members = {"@agent:test": {}, "@human:test": {}}
        adapter._client = SimpleNamespace(state_store=None, get_joined_members=AsyncMock(return_value=members))
        adapter._get_room_state_value = AsyncMock(return_value=None)
        assert await adapter._get_room_member_count("!dm:test") == 2
        assert (await adapter._resolve_room_identity("!dm:test", force_refresh=True)).chat_type == "dm"
        members["@third:test"] = {}
        assert (await adapter._resolve_room_identity("!dm:test", force_refresh=True)).chat_type == "room"
    asyncio.run(check())


def test_execution_notice_uses_the_native_serializable_event_type(monkeypatch):
    monkeypatch.setenv("MATRIX_ALLOWED_USERS", "@human:test")
    async def check():
        adapter = MatrixAdapter(PlatformConfig(enabled=True))
        async def send(room, event_type, content):
            # Native Megolm serialization calls this method; a plain string
            # works in an unencrypted room but fails before encrypted delivery.
            assert event_type.serialize() == "m.room.message"
            assert content["com.agentteams.execution"]["status"] == "running"
        adapter._client = SimpleNamespace(send_message_event=send)
        await adapter._execution_status({"room": "!dm:test", "event": message("$start", "task")}, "running")
    asyncio.run(check())


def test_streaming_edit_marks_its_original_response(monkeypatch):
    monkeypatch.setenv("MATRIX_ALLOWED_USERS", "@human:test")
    async def check():
        adapter = MatrixAdapter(PlatformConfig(enabled=True))
        adapter._user_id = "@agent:test"
        adapter._client = SimpleNamespace(send_message_event=AsyncMock(return_value="$edit"))
        run = adapter._direct_runs["!dm:test"] = {"event": message("$start", "task")}
        adapter._wrap_send_message_event()
        from mautrix.types import EventType
        await adapter._client.send_message_event("!dm:test", EventType.ROOM_MESSAGE, {
            "msgtype": "m.text", "body": "partial",
            "m.relates_to": {"rel_type": "m.replace", "event_id": "$root"},
        })
        assert run["message_id"] == "$root"
    asyncio.run(check())


@pytest.mark.parametrize("action", ["stop"])
def test_native_shell_exits_before_stop_ack_and_supplement(monkeypatch, tmp_path, action):
    monkeypatch.setenv("MATRIX_ALLOWED_USERS", "@human:test")

    async def check():
        adapter = MatrixAdapter(PlatformConfig(enabled=True))
        adapter._user_id = "@agent:test"
        adapter._client = SimpleNamespace(
            get_joined_members=AsyncMock(return_value={"@agent:test": {}, "@human:test": {}}),
            send_message_event=AsyncMock(return_value="$response"))
        agent = NativeAgent()
        marker, pidfile = tmp_path / "must-not-run", tmp_path / "pid"

        class Runner:
            _running_agents = {"session": agent}

            async def handle(self, event):
                def execute():
                    agent._execution_thread_id = threading.get_ident()
                    try:
                        return LocalEnvironment().execute(
                            f"echo $$ > {pidfile}; sleep 30; touch {marker}", timeout=40)
                    finally:
                        set_interrupt(False, thread_id=threading.get_ident())
                result = await asyncio.to_thread(execute)
                assert result["returncode"] != 0

        adapter._message_handler = Runner().handle
        async def native_process(self, event, session_key):
            await self._message_handler(event)
        monkeypatch.setattr(_NativeMatrixAdapter, "_process_message_background", native_process)
        forwarded = AsyncMock()
        monkeypatch.setattr(_NativeMatrixAdapter, "handle_message", forwarded)
        original = asyncio.create_task(adapter._process_message_background(message("$start", "原任务"), "session"))
        for _ in range(200):
            if pidfile.exists():
                break
            await asyncio.sleep(0.025)
        assert pidfile.exists(), "native shell never started"
        await asyncio.wait_for(adapter.handle_message(message("$control", "新增杭州", action)), 10)
        await original
        pid = int(pidfile.read_text())
        with pytest.raises(ProcessLookupError):
            os.kill(pid, 0)
        assert not marker.exists()
        statuses = [call.args[2]["com.agentteams.execution"]["status"]
                    for call in adapter._client.send_message_event.await_args_list]
        assert statuses == ["running", "stopping", "stopped"]
        if action == "steer":
            forwarded.assert_awaited_once()
            assert "原任务" in forwarded.call_args.args[0].text
            assert "新增杭州" in forwarded.call_args.args[0].text
        else:
            forwarded.assert_not_awaited()

    asyncio.run(check())


def test_supplements_use_native_steer_without_stopping_the_active_task(monkeypatch):
    monkeypatch.setenv("MATRIX_ALLOWED_USERS", "@human:test")

    async def check():
        adapter = MatrixAdapter(PlatformConfig(enabled=True))
        adapter._user_id = "@agent:test"
        adapter._client = SimpleNamespace(
            get_joined_members=AsyncMock(return_value={"@agent:test": {}, "@human:test": {}}),
            send_message_event=AsyncMock())
        agent = NativeAgent()
        task = asyncio.create_task(asyncio.Event().wait())
        run = {"event": message("$start", "original"), "task": task}
        adapter._direct_runs["!dm:test"] = run
        forwarded = []

        async def native_handle(self, event):
            forwarded.append(event)
            assert event.text.startswith("/steer ")
            assert agent.steer(event.text.removeprefix("/steer "))

        monkeypatch.setattr(_NativeMatrixAdapter, "handle_message", native_handle)
        try:
            for index in range(2):
                supplement = message(f"$supplement-{index}", f"additional {index}", "steer")
                await adapter.handle_message(supplement)
                assert supplement.text == f"additional {index}"
                assert forwarded[-1].message_id == supplement.message_id
                assert forwarded[-1].source is supplement.source
            assert agent._drain_pending_steer() == "additional 0\nadditional 1"
            assert not task.done() and not task.cancelling()
            assert not run.get("stopped")
            adapter._client.send_message_event.assert_not_awaited()
        finally:
            task.cancel()
            await asyncio.gather(task, return_exceptions=True)

    asyncio.run(check())


def test_controls_reject_other_owner_group_members_and_stale_run(monkeypatch):
    monkeypatch.setenv("MATRIX_ALLOWED_USERS", "@human:test,@other:test")
    async def check():
        adapter = MatrixAdapter(PlatformConfig(enabled=True))
        adapter._user_id = "@agent:test"
        adapter._client = SimpleNamespace(get_joined_members=AsyncMock(return_value={
            "@agent:test": {}, "@human:test": {}, "@other:test": {}}))
        task = asyncio.create_task(asyncio.sleep(20))
        adapter._direct_runs["!dm:test"] = {"event": message("$start", "原任务"), "task": task}
        try:
            await adapter.handle_message(message("$other", "stop", "stop", sender="@other:test"))
            await adapter.handle_message(message("$group", "stop", "stop"))
            await adapter.handle_message(message("$stale", "stop", "stop", run_id="$previous"))
            assert not task.done()
        finally:
            task.cancel()
            await asyncio.gather(task, return_exceptions=True)
    asyncio.run(check())


@pytest.mark.parametrize("recovery", ["resume", "done", "verify"])
def test_task_supplement_is_claimed_before_native_steer(monkeypatch, recovery):
    from unittest.mock import Mock
    from hermes_matrix import task_inputs
    from task_recovery import RecoveryConflict
    monkeypatch.setenv("TEAMHARNESS_RUNTIME_CONFIG", "/unused/runtime.yaml")
    monkeypatch.setattr(task_inputs.Path, "read_text", lambda self: "{}")
    monkeypatch.setattr(task_inputs, "task_supplement", lambda *args: True)
    order = []
    matrix = Mock()
    matrix.message_claim.side_effect = lambda key: (order.append("claim") or {"recovery": recovery})
    monkeypatch.setattr("worker_matrix_state.WorkerMatrix", lambda runtime: matrix)
    agent = SimpleNamespace(steer=lambda *args, **kwargs: (order.append("steer") or True))
    event = SimpleNamespace(_agentteams_recovery={"task_id": "child"},
        _agentteams_task_turn=SimpleNamespace(supplements={}), source=SimpleNamespace(chat_id="!task:test"))
    runner = SimpleNamespace(_running_agents={"session": agent})
    class Handler:
        def handle(self): pass
    handler = Handler()
    # The native adapter stores its bound message handler and active session.
    adapter = SimpleNamespace(_task_runs={"!task:test": {"event": event, "session_key": "session"}},
        _message_handler=handler.handle)
    handler._running_agents = runner._running_agents
    entry = {"room_id": "!task:test", "event_id": "$extra", "input": {
        "sender": "@leader:test", "content": {"body": "new requirement"}}}
    if recovery == "verify":
        with pytest.raises(RecoveryConflict):
            asyncio.run(task_inputs.steer(adapter, entry))
    else:
        assert asyncio.run(task_inputs.steer(adapter, entry)) is True
    assert order == (["claim", "steer"] if recovery == "resume" else ["claim"])
