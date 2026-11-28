"""Harness-owned native tracing boundary checks against shared Core telemetry."""
import asyncio
import json
import os
import subprocess
import sys
from pathlib import Path
from types import SimpleNamespace
import pytest

ROOT = Path(__file__).resolve().parents[1]
SOURCE = Path(os.environ.get("AT_SOURCE", ROOT / ".deps/AgentTeams"))
NATIVE = ROOT / "telemetry"
sys.path.insert(0, str(SOURCE / "shared/lib"))
sys.path.insert(0, str(SOURCE / "shared/lib/telemetry"))
sys.path.insert(0, str(NATIVE))
from opentelemetry import context, trace
from opentelemetry.sdk.trace import TracerProvider
from opentelemetry.sdk.trace.export import SimpleSpanProcessor
from opentelemetry.sdk.trace.export.in_memory_span_exporter import InMemorySpanExporter
import at_telemetry as telemetry
import at_runtime_adapters as common
import at_trace_transport as transport


@pytest.mark.parametrize('event_id,expected', [
    ('recovery:team-a/task-1:3', None),
    ('$matrix-event:server', None),
    ('recovery:team-a/task-1:0', None),
    ('recovery:team-a/task-1:not-a-generation', None),
    (None, None),
])
def test_hermes_recovery_trace_retains_task_and_execution_generation(monkeypatch, event_id, expected):
    from types import SimpleNamespace
    import at_native_telemetry as adapters
    callbacks, captured = [], []
    monkeypatch.setattr(adapters.wrapt, 'wrap_function_wrapper', lambda module, name, callback: callbacks.append(callback))
    monkeypatch.setattr(adapters, 'start_turn', lambda fields, request: captured.append(fields) or trace.NonRecordingSpan(trace.INVALID_SPAN_CONTEXT))
    monkeypatch.setattr(adapters, 'enrich_task', lambda fields, workspace: fields)
    monkeypatch.setitem(sys.modules, 'at_trace_transport', SimpleNamespace(remember_reply=lambda *args: None, incoming=lambda *args: {}))
    adapters.hermes_turn(object())
    result = callbacks[0](lambda *args, **kwargs: {'ok': True},
                          SimpleNamespace(session_id='native-session', _chat_id='!task:server'),
                          ('resume',), {'persist_user_platform_id': event_id})
    assert result == {'ok': True}
    fields = captured[0]
    assert fields['agentteams.runtime.session_id'] == 'native-session'
    if expected:
        assert (fields['agentteams.team.id'], fields['agentteams.task.id'], fields['agentteams.execution.generation']) == expected
    else:
        assert 'agentteams.task.id' not in fields
        assert 'agentteams.execution.generation' not in fields


@pytest.mark.parametrize('generation', [None, 4])
def test_hermes_native_trace_uses_verified_current_frame(monkeypatch, generation):
    from types import SimpleNamespace
    import at_native_telemetry as adapters
    callbacks, captured = [], []
    metadata = {'room_id': '!canonical:test', 'event_id': '$canonical-event', 'task_id': 'task-actual',
        'team_id': 'team-actual', 'teamharness_coordination': {'id': 'a' * 32, 'kind': 'task_assigned'}}
    if generation is not None:
        metadata['execution_generation'] = generation
    monkeypatch.setitem(sys.modules, 'hermes_matrix.public_tasks', SimpleNamespace(
        current_turn=SimpleNamespace(get=lambda: SimpleNamespace(trace_metadata=lambda: metadata))))
    monkeypatch.setitem(sys.modules, 'at_trace_transport', SimpleNamespace(remember_reply=lambda *a: None, incoming=lambda *a: {}))
    monkeypatch.setattr(adapters.wrapt, 'wrap_function_wrapper', lambda module, name, callback: callbacks.append(callback))
    monkeypatch.setattr(adapters, 'start_turn', lambda fields, request: captured.append(fields) or trace.NonRecordingSpan(trace.INVALID_SPAN_CONTEXT))
    monkeypatch.setattr(adapters, 'enrich_task', lambda fields, workspace: fields)
    adapters.hermes_turn(object())
    callbacks[0](lambda *a, **kw: {'ok': True}, SimpleNamespace(session_id='opaque:other-task:999'), (), {})
    actual = captured[0]
    assert actual['agentteams.task.id'] == 'task-actual'
    assert actual['agentteams.event.id'] == '$canonical-event'
    assert actual['agentteams.room.id'] == '!canonical:test'
    assert actual['agentteams.coordination.id'] == 'a' * 32
    assert actual['agentteams.coordination.kind'] == 'task_assigned'
    assert actual.get('agentteams.execution.generation') == generation



def test_gateway_receipt_binds_native_task_and_clears_after_delivery(monkeypatch):
    import at_native_telemetry as adapters
    callbacks = []
    monkeypatch.setattr(adapters.wrapt, 'wrap_function_wrapper', lambda module, name, callback: callbacks.append(callback))
    monkeypatch.setattr(adapters, 'incoming', lambda content, **kwargs: {'agentteams.room.id': kwargs['room_id'], 'agentteams.event.id': kwargs['event_id']})
    adapters.hermes_gateway(object())
    event = SimpleNamespace(raw_message={'content': {}}, source=SimpleNamespace(chat_id='!room:test'), message_id='$event')
    seen = []
    async def wrapped(*args, **kwargs):
        seen.append(dict(telemetry._correlation.get()))
        assert transport._delivery.get() == {}
        return 'delivered'
    before = dict(telemetry._correlation.get())
    assert asyncio.run(callbacks[0](wrapped, None, (event,), {})) == 'delivered'
    assert seen[0]['agentteams.room.id'] == '!room:test'
    assert seen[0]['agentteams.event.id'] == '$event'
    assert seen[0]['agentteams.message.received_at_ns'] > 0
    assert telemetry._correlation.get() == before
    assert transport._delivery.get() is None
