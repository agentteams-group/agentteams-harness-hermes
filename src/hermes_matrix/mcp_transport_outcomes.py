"""Preserve actual HTTP failures versus missing MCP responses in native results."""
from functools import wraps
import hashlib
import json


def call_digest(name, arguments):
    return hashlib.sha256(json.dumps([name, arguments], sort_keys=True, separators=(',', ':')).encode()).hexdigest()


def install():
    from tools import mcp_tool_handlers as native, mcp_tool_transport as transport
    if getattr(native._dispatch, '_agentteams_outcomes', False):
        return
    recorder, invoke, dispatch = transport._make_http_rejection_recorder, native._call_tool_racing_stdio_death, native._dispatch

    def recording(sink):
        original = recorder(sink)

        async def response_hook(response):
            await original(response)
            if response.status_code < 400 or response.request.method != 'POST':
                return
            try:
                request = json.loads(response.request.content)
                params = request.get('params') or {}
                digest = call_digest(params.get('name'), params.get('arguments') or {})
            except (ValueError, TypeError, AttributeError):
                return
            if request.get('method') == 'tools/call' and digest == sink.get('_agentteams_expected_call'):
                sink['_agentteams_call_rejection'] = {'status': response.status_code,
                                                      'body': native._sanitize_error(sink.get('body', ''))}
        return response_hook

    @wraps(invoke)
    async def calling(server, server_name, tool_name, args, *rest, **kwargs):
        server._agentteams_call_read_only = native._tool_is_read_only(server_name, tool_name)
        sink = getattr(server, '_http_rejection', None)
        if isinstance(sink, dict):
            sink['_agentteams_expected_call'] = call_digest(tool_name, args)
            sink.pop('_agentteams_call_rejection', None)
        return await invoke(server, server_name, tool_name, args, *rest, **kwargs)

    @wraps(dispatch)
    def executing(server_name, server, op, call, tool_timeout, recoverers, on_final_failure, record_outcome=False):
        failed = []
        server._agentteams_call_read_only = False

        def failure(error):
            failed.append(error)
            on_final_failure(error)

        sink = getattr(server, '_http_rejection', None)
        if isinstance(sink, dict):
            sink.pop('_agentteams_expected_call', None)
            sink.pop('_agentteams_call_rejection', None)
        result = dispatch(server_name, server, op, call, tool_timeout, recoverers, failure, record_outcome)
        if not failed:
            return result
        rejection = (getattr(server, '_http_rejection', None) or {}).get('_agentteams_call_rejection')
        if rejection:
            return native.tool_error('MCP request failed (HTTP ' + str(rejection['status']) + '): ' + rejection['body'],
                                     http_status=rejection['status'])
        # A native exception without a matching HTTP response does not prove
        # whether a write happened. Keep the native sanitized root cause.
        payload = json.loads(result)
        if not server._agentteams_call_read_only:
            payload['outcome_uncertain'] = True
        return json.dumps(payload)

    executing._agentteams_outcomes = True
    native._dispatch = executing
    native._call_tool_racing_stdio_death = calling
    transport._make_http_rejection_recorder = recording
