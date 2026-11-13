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


@pytest.mark.parametrize("action", ["stop", "steer"])
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
