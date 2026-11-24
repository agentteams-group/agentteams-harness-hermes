"""Drive Hermes' real overlay ingress against public Controller receipts."""
import asyncio
import os
from pathlib import Path
import sys
from types import SimpleNamespace
from unittest.mock import AsyncMock, Mock

core = Path(os.environ.get("AT_SOURCE", Path(__file__).resolve().parents[1] / ".deps/AgentTeams"))
sys.path.insert(0, str(core / "tests/lib"))
from assistant_fixture import AssistantFixture
from test_shared_input_boundary import fixture


def test_assistant_wakes_through_teams_and_rejects_replaced_role(monkeypatch):
    _module, _native, adapter = fixture(monkeypatch)
    with AssistantFixture() as f:
        adapter._user_id = f.own
        adapter._is_allowed_matrix_room_event = AsyncMock(return_value=True)
        adapter._is_duplicate_event = lambda _: False
        adapter._dispatch_coordination = AsyncMock(return_value=True)
        adapter._pending_matrix = None
        event = SimpleNamespace(room_id=f.room, event_id="$assistant", sender=f.own, content=f.content())
        asyncio.run(adapter._on_matrix_input(event))
        assert adapter._accept_matrix_input.await_args.args[-1] == "control"
        packet = adapter._dispatch_coordination.await_args.args[1]
        assert packet["text"] == f.text
        assert packet["session_id"] == f.control["session_id"]
        f.context["owner"] = "replaced"
        asyncio.run(adapter._on_matrix_input(event))
        assert adapter._dispatch_coordination.await_count == 1


def test_assistant_native_output_is_silent_but_approvals_remain_visible(monkeypatch):
    module, _native, adapter = fixture(monkeypatch)
    sent = AsyncMock(return_value="$approval")
    adapter._client = SimpleNamespace(send_message_event=sent)
    adapter._user_id = "@leader:test"
    adapter._session_receipts = None
    adapter._direct_runs = {}
    adapter._wrap_send_message_event()

    async def run():
        token = module._coordination_turn.set("assistant_digest")
        try:
            assert await adapter._client.send_message_event("!assistant:test", "m.room.message", {"msgtype":"m.text","body":"No updates"}) is None
            sent.assert_not_awaited()
            await adapter._client.send_message_event("!assistant:test", "m.room.message", {"msgtype":"m.text","body":"Approve?","org.agentteams.approval":{"request_id":"one"}})
            sent.assert_awaited_once()
        finally:
            module._coordination_turn.reset(token)
    asyncio.run(run())


def test_assistant_keeps_native_trace_context_without_loading_task_roster(monkeypatch, tmp_path):
    from hermes_matrix import public_tasks
    _module, _native, adapter = fixture(monkeypatch)
    adapter._user_id = '@lead:test'
    monkeypatch.setenv("AGENTTEAMS_CONTROLLER_URL", "http://controller.test")
    monkeypatch.setenv("AGENTTEAMS_PLUGIN_DIR", str(core / "plugins/teamharness"))
    monkeypatch.setenv("AGENTTEAMS_AGENT_ROLE", "team_leader")
    runtime = tmp_path / "runtime.yaml"
    runtime.write_text('team:\n  name: "Hermes 验收"\n')
    monkeypatch.setenv("TEAMHARNESS_RUNTIME_CONFIG", str(runtime))
    monkeypatch.setattr(public_tasks, "install", lambda: None)
    calls = []
    def invoke(request):
        calls.append(request["operation"])
        assert request["operation"] == "prepare_event"
        return {}
    monkeypatch.setattr(public_tasks, "common", Mock(return_value=SimpleNamespace(invoke=invoke)))
    event = SimpleNamespace(message_id="$assistant", source=SimpleNamespace(chat_id="!assistant:test", user_id="@lead:test"),
        _task_protocol_meta={"teamharness_coordination": {"id":"assistant-notice", "kind":"assistant_digest"}})
    async def native(*args):
        assert public_tasks.current_turn.get().meta["teamharness_coordination"]["kind"] == "assistant_digest"
    adapter._process_native_message_background = AsyncMock(side_effect=native)
    asyncio.run(adapter._process_message_background(event, "assistant-session"))
    assert calls == ["prepare_event"]
    adapter._process_native_message_background.assert_awaited_once_with(event, "assistant-session")
