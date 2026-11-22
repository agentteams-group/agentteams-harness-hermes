"""Hermes native-binding contract fixtures; not model/E2E certification."""
import json
from pathlib import Path
import sys
from types import ModuleType, SimpleNamespace

import pytest

from hermes_matrix import public_tasks


@pytest.fixture
def modules(monkeypatch):
    source = Path(__file__).resolve().parents[2]
    monkeypatch.setenv("AGENTTEAMS_PLUGIN_DIR", str(source / "plugins/teamharness"))
    monkeypatch.setenv("AGENTTEAMS_AGENT_ROLE", "worker")
    public_tasks.common.cache_clear()
    yield public_tasks.common("task_native")
    public_tasks.common.cache_clear()


def event(identity="$source"):
    return SimpleNamespace(message_id=identity, source=SimpleNamespace(chat_id="!room:test", user_id="@human:test"))


def test_native_tool_result_keeps_cli_result_and_adds_verified_fact(modules, monkeypatch):
    monkeypatch.setenv("AGENTTEAMS_AGENT_ROLE", "manager")
    captured = []
    result = {"sourceEventId": "$source", "task": {"id": "main", "kind": "main", "status": "assigned"},
              "notification": {"status": "queued", "recipient": "leader", "eventId": "notice"}}
    def call(arguments):
        captured.append(arguments)
        return result
    monkeypatch.setattr(modules.task_cli, "run", call)
    with public_tasks.task_turn(event()) as turn:
        answer = json.loads(turn.invoke({"action": "create", "leader": "leader", "title": "own request"}))
        assert answer["task"] == result["task"]
        assert answer == result
        assert "queued" in turn.prompt()
        assert captured[0]["_nativeSource"] == {"eventId": "$source", "userId": "@human:test", "roomId": "!room:test"}
        assert public_tasks.current_turn.get() is turn
    assert public_tasks.current_turn.get() is None


def test_verified_handoff_uses_admitted_task_ids_without_a_human_source(modules, monkeypatch):
    monkeypatch.setenv("AGENTTEAMS_AGENT_ROLE", "team_leader")
    notice = event("$handoff")
    notice._task_protocol_meta = {"sender_id": "", "teamharness_coordination": {"kind": "handoff"}}
    with public_tasks.task_turn(notice, {"planning": ["main"]}) as turn:
        assert turn.context["planning"] == ["main"]
        assert turn.context["sources"] == []
    notice._task_protocol_meta.pop("teamharness_coordination")
    with pytest.raises(ValueError, match="complete native source identity"):
        with public_tasks.task_turn(notice):
            pass


def test_model_cannot_supply_source_or_operate_on_another_execution(modules):
    with public_tasks.task_turn(event(), {"execution": "own"}) as turn:
        with pytest.raises(ValueError, match="execution"):
            turn.invoke({"action": "submit", "taskId": "other", "summary": "wrong"})
        with pytest.raises(ValueError, match="_nativeSource"):
            turn.invoke({"action": "get", "taskId": "own", "_nativeSource": {}})


def test_native_cwd_is_task_scoped_and_restored_without_global_chdir(modules, monkeypatch):
    terminal = ModuleType("tools.terminal_tool")
    overrides = {"native-session": {"cwd": "/original", "docker_image": "retained"}}
    terminal.resolve_task_overrides = lambda task: overrides.get(task, {})
    terminal.register_task_env_overrides = lambda task, value: overrides.__setitem__(task, value)
    terminal.clear_task_env_overrides = lambda task: overrides.pop(task, None)
    monkeypatch.setitem(sys.modules, "tools.terminal_tool", terminal)
    original_tools = [{"function": {"name": "terminal"}}, {"function": {"name": "mcp__teamharness__taskflow"}}]
    agent = SimpleNamespace(tools=original_tools, valid_tool_names={"terminal", "mcp__teamharness__taskflow"})
    messages = [{"role": "user", "platform_message_id": "$source", "content": "own task"}]
    with public_tasks.task_turn(event(), {"execution": "t1", "context": {"outputDirectory": "/shared/tasks/t1/workspace"}}) as turn:
        turn.bind(agent, "native-session", messages)
        assert overrides["native-session"] == {"cwd": "/shared/tasks/t1/workspace", "docker_image": "retained"}
        assert agent.valid_tool_names == {"terminal", public_tasks.TASK_TOOL}
        assert messages[0]["content"].startswith("[Message $source]\n")
    assert overrides["native-session"]["cwd"] == "/original"
    assert agent.tools is original_tools


def test_review_exposes_only_common_task_operations(modules):
    agent = SimpleNamespace(tools=[{"function": {"name": "terminal"}}], valid_tool_names={"terminal"})
    with public_tasks.task_turn(event(), {"review": ["execution", "submission"]}) as turn:
        turn.bind(agent, "review-session", [])
        assert agent.valid_tool_names == {public_tasks.TASK_TOOL}


def test_refreshed_input_must_be_saved_and_read_back_before_creation_resumes(modules):
    source = {"eventId": "$late", "body": "new actual Human instruction"}
    persisted = []
    def save(messages):
        persisted[:] = [dict(row) for row in messages]
    agent = SimpleNamespace(_persist_session=save, _persist_disabled=False, session_id="own",
                            _session_db=SimpleNamespace(get_messages=lambda *a, **kw: persisted))
    with public_tasks.task_turn(event()) as turn:
        turn.pending = [source]
        turn.context["inputRefreshPending"] = True
        messages = []
        turn.checkpoint_refreshed(agent, messages)
        assert messages[0]["role"] == "user"
        assert messages[0]["platform_message_id"] == "$late"
        assert "inputRefreshPending" not in turn.context
        agent._session_db.get_messages = lambda *a, **kw: []
        turn.pending = [{"eventId": "$not-saved", "body": "do not lose"}]
        turn.context["inputRefreshPending"] = True
        with pytest.raises(RuntimeError, match="not retained"):
            turn.checkpoint_refreshed(agent, messages)
        assert turn.context["inputRefreshPending"] is True
