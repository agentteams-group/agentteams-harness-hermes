"""Retain native MCP uncertainty before executor rendering/spilling loses it."""
from functools import wraps
import inspect
import json


def unconfirmed(agent, rows):
    uncertain = getattr(agent, '_agentteams_uncertain_mcp_calls', set())
    return any(row.get('effect_disposition') == 'unknown' or row.get('tool_call_id') in uncertain
               for row in rows if row.get('role') == 'tool')


def receipts(agent, rows):
    failed = getattr(agent, '_agentteams_failed_mcp_calls', {})
    return [{"id": row['tool_call_id'], "content": row['content'],
             **({'status': 'error', 'error': failed[row['tool_call_id']]} if row['tool_call_id'] in failed else {})}
            for row in rows if row.get('role') == 'tool']


def install():
    from agent import tool_executor
    from tools import mcp_tool_handlers as native
    from tools.registry import registry
    original = tool_executor.note_tool_result
    if getattr(original, '_agentteams_mcp_outcomes', False):
        return
    # Bind to the installed native handler definitions, not a model-visible
    # tool name or a JSON fragment inside user-controlled tool output.
    owned_codes = {
        native._make_tool_handler('_', '_', 1).__code__,
        native._make_list_resources_handler('_', 1).__code__,
    }

    @wraps(original)
    def observe(agent, name, call_id, function_result, persisted_result):
        entry = registry.get_entry(name)
        handler = inspect.unwrap(entry.handler) if entry else None
        if getattr(handler, '__code__', None) in owned_codes:
            try:
                result = json.loads(function_result) if isinstance(function_result, str) else function_result
            except (ValueError, TypeError):
                result = None
            if isinstance(result, dict) and result.get('error'):
                if not hasattr(agent, '_agentteams_failed_mcp_calls'):
                    agent._agentteams_failed_mcp_calls = {}
                agent._agentteams_failed_mcp_calls[call_id] = result['error']
            if isinstance(result, dict) and result.get('outcome_uncertain') is True:
                if not hasattr(agent, '_agentteams_uncertain_mcp_calls'):
                    agent._agentteams_uncertain_mcp_calls = set()
                agent._agentteams_uncertain_mcp_calls.add(call_id)
        return original(agent, name, call_id, function_result, persisted_result)

    observe._agentteams_mcp_outcomes = True
    tool_executor.note_tool_result = observe
