"""Actual native dispatch and HTTP recorder: no vendor code classification."""
import asyncio
import json
from types import SimpleNamespace
from unittest.mock import Mock

import httpx
import pytest
from tools import mcp_tool_handlers as native, mcp_tool_transport as transport
from hermes_matrix.mcp_transport_outcomes import install


@pytest.mark.parametrize('status,matching,read_only', [(s, True, False) for s in [400, 401, 404, 409, 422, 429, 500, 503, None]] + [(503, False, False), (None, True, True)])
def test_native_terminal_http_error_is_known_but_a_missing_response_is_not(monkeypatch, status, matching, read_only):
    server = SimpleNamespace(_http_rejection={})

    async def invoke(server, name, tool, args):
        if status:
            request = httpx.Request('POST', 'https://fixture.invalid/mcp', json={
                'jsonrpc':'2.0','id':1,'method':'tools/call','params':{'name':tool,'arguments':args if matching else {'value':'another-call'}}})
            response = httpx.Response(status, text='Concrete fixture failure', request=request)
            await transport._make_http_rejection_recorder(server._http_rejection)(response)
        raise RuntimeError('Native opaque RPC failure')

    monkeypatch.setattr(native, '_tool_is_read_only', lambda *args: read_only)
    monkeypatch.setattr(native, '_call_tool_racing_stdio_death', invoke)
    # Register originals with pytest so the process-wide native hooks are restored.
    monkeypatch.setattr(native, '_dispatch', native._dispatch)
    monkeypatch.setattr(transport, '_make_http_rejection_recorder', transport._make_http_rejection_recorder)
    monkeypatch.setattr(native._loop, '_run_on_mcp_loop', lambda call, **kwargs: asyncio.run(call()))
    install()
    failed = Mock()
    result = json.loads(native._dispatch('fixture', server, 'tools/call write',
        lambda: native._call_tool_racing_stdio_death(server, 'fixture', 'write', {'value':1}), 1, (), failed))
    failed.assert_called_once()
    if status and matching:
        assert result['http_status'] == status
        assert 'Concrete fixture failure' in result['error']
        assert not result.get('outcome_uncertain')
    else:
        assert bool(result.get('outcome_uncertain')) is not read_only
        assert 'Native opaque RPC failure' in result['error']


def test_previous_http_failure_cannot_settle_an_unrelated_interrupted_call(monkeypatch):
    server = SimpleNamespace(_http_rejection={'_agentteams_call_rejection':{'status':503,'body':'old'}})
    monkeypatch.setattr(native, '_dispatch', native._dispatch)
    monkeypatch.setattr(native, '_call_tool_racing_stdio_death', native._call_tool_racing_stdio_death)
    monkeypatch.setattr(transport, '_make_http_rejection_recorder', transport._make_http_rejection_recorder)
    monkeypatch.setattr(native._loop, '_run_on_mcp_loop', lambda *a, **k: (_ for _ in ()).throw(RuntimeError('connection lost')))
    install()
    result = json.loads(native._dispatch('fixture', server, 'tools/call write', lambda: None, 1, (), lambda _: None))
    assert result['outcome_uncertain'] is True
    assert 'http_status' not in result
