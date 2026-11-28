"""Hermes policy admits only durably retained Matrix inputs (mock authority)."""
import asyncio
from types import SimpleNamespace
from unittest.mock import AsyncMock, Mock

import pytest

from hermes_matrix.policies import HistoryBuffer
from test_overlay_image_downgrade import _load_overlay_with_fake_gateway


def fixture(monkeypatch):
    module, native = _load_overlay_with_fake_gateway(monkeypatch)
    adapter = module.MatrixAdapter.__new__(module.MatrixAdapter)
    native.__init__(adapter)
    adapter._dual_allow = SimpleNamespace(permits=lambda *a, **kw: True)
    adapter._dashboard_approvals = SimpleNamespace(answer=AsyncMock(return_value=False))
    adapter._history = HistoryBuffer(limit=10)
    adapter._accept_matrix_input = AsyncMock()
    return module, native, adapter


@pytest.mark.parametrize("accepted", [True, False])
def test_policy_persists_before_admitting_or_buffering_input(monkeypatch, accepted):
    module, native, adapter = fixture(monkeypatch)
    if not accepted:
        async def unmentioned(*args, **kwargs):
            return None
        monkeypatch.setattr(native, "_resolve_message_context", unmentioned)
    asyncio.run(adapter._resolve_message_context("!room", "@alice:test", "$event", "hello",
        {"msgtype": "m.text", "body": "hello"}, {}))
    assert adapter._accept_matrix_input.await_args.args == (
        "!room", "@alice:test", "$event", {"msgtype": "m.text", "body": "hello"},
        "message" if accepted else "context")


def test_storage_failure_prevents_history_admission(monkeypatch):
    module, native, adapter = fixture(monkeypatch)
    async def unmentioned(*args, **kwargs):
        return None
    monkeypatch.setattr(native, "_resolve_message_context", unmentioned)
    adapter._accept_matrix_input.side_effect = OSError("shared storage unavailable")
    adapter._history.record = Mock()
    with pytest.raises(OSError):
        asyncio.run(adapter._resolve_message_context("!room", "@alice:test", "$event", "hello", {}, {}))
    adapter._history.record.assert_not_called()


@pytest.mark.parametrize("failure", [False, True])
def test_verified_assignment_is_durable_before_task_claim(monkeypatch, failure):
    import task_coordination
    import task_recovery
    module, native, adapter = fixture(monkeypatch)
    order = []
    adapter._user_id = "@worker:test"
    adapter._is_allowed_matrix_room_event = AsyncMock(return_value=True)
    adapter._is_duplicate_event = Mock(return_value=False)
    adapter._recover_tasks = AsyncMock(side_effect=lambda *a, **kw: order.append("claim"))
    def persist(*args, **kwargs):
        order.append("persist")
        if failure:
            raise OSError("storage failed")
    adapter._accept_matrix_input.side_effect = persist
    monkeypatch.setattr(task_recovery, "enabled", lambda: True)
    monkeypatch.setattr(task_coordination, "notification", lambda *args, event_id: {
        "meta": {"teamharness_coordination": {"kind": "task_assigned", "task_id": "T"}}})
    event = SimpleNamespace(room_id="!room:test", sender="@leader:test", event_id="$assignment",
        timestamp=42, content={"org.agentteams.coordination": {}})
    if failure:
        with pytest.raises(OSError):
            asyncio.run(adapter._on_room_message(event))
        adapter._recover_tasks.assert_not_awaited()
        assert order == ["persist"]
    else:
        asyncio.run(adapter._on_room_message(event))
        assert order == ["persist", "claim"]
    assert adapter._accept_matrix_input.await_args.kwargs == {"task_id": "T"}
    assert adapter._accept_matrix_input.await_args.args[-1] == "task_notification"


def test_overlapping_native_callbacks_keep_their_source_timestamp(monkeypatch):
    import task_recovery
    import worker_matrix_state
    module, native, adapter = fixture(monkeypatch)
    adapter._accept_matrix_input = module.MatrixAdapter._accept_matrix_input.__get__(adapter)
    monkeypatch.setattr(task_recovery, "enabled", lambda: True)
    accepted = []
    monkeypatch.setattr(worker_matrix_state, "WorkerMatrix", lambda runtime: SimpleNamespace(
        accept=lambda *a, **kw: accepted.append(a)))
    async def native_input(self, event):
        await asyncio.sleep(0)
        await self._accept_matrix_input(str(event.room_id), str(event.sender), str(event.event_id),
            event.content, "message")
    monkeypatch.setattr(native, "_on_room_message", native_input, raising=False)
    async def run():
        adapter._task_recovery_lock = asyncio.Lock()
        await asyncio.gather(*(adapter._on_room_message(SimpleNamespace(room_id="!room:test",
            sender="@human:test", event_id=f"${n}", timestamp=n, content={"body": str(n)})) for n in [1, 2]))
        assert module._matrix_source_event.get() is None
    asyncio.run(run())
    assert [(args[1], args[2]["origin_server_ts"], args[2]["content"]["body"]) for args in accepted] == [
        ("$1", 1, "1"), ("$2", 2, "2")]


def test_failed_task_cleanup_clears_running_state_and_allows_next_trigger(monkeypatch):
    import hermes_matrix.task_inputs as task_inputs
    from worker_matrix_state import matrix_event_key
    module, native, adapter = fixture(monkeypatch)
    room, event_id = '!task:test', '$first'
    key = matrix_event_key(room, event_id)
    adapter._task_runs = {}
    adapter._pending_matrix = SimpleNamespace(running={key})
    backend = AsyncMock(return_value='native result')
    monkeypatch.setattr(native, '_process_message_background', backend, raising=False)
    monkeypatch.setattr(task_inputs, 'close', Mock(side_effect=[OSError('native cleanup failed'), None]))
    event = SimpleNamespace(raw_message={}, source=SimpleNamespace(chat_id=room), message_id=event_id,
                            _agentteams_recovery={'task_id': 'node'})
    async def run():
        before = module._coordination_turn.get()
        with pytest.raises(OSError, match='cleanup failed'):
            await adapter._process_managed_message(event, 'native-session')
        assert adapter._task_runs == {}
        assert not adapter._pending_matrix.running
        assert module._coordination_turn.get() == before
        assert await adapter._process_managed_message(event, 'native-session') == 'native result'
        assert adapter._task_runs == {}
        assert module._coordination_turn.get() == before
    asyncio.run(run())
    assert backend.await_count == 2
