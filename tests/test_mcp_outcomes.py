"""Actual pinned native handler identity and raw-to-spilled result boundary."""
import json
from types import SimpleNamespace
from unittest.mock import Mock, patch

import pytest
from agent import tool_executor
from tools import mcp_tool_handlers as native
from tools.registry import registry
from hermes_matrix.mcp_outcomes import install, unconfirmed
from hermes_matrix.message_execution import execute_tools
from task_recovery import RecoveryConflict


def observe(agent, call, raw, *, owned=True):
    handler = native._make_tool_handler('fixture', 'write', 1) if owned else lambda *a: raw
    with patch.object(tool_executor, 'note_tool_result', new=lambda *args: None), \
            patch.object(registry, 'get_entry', return_value=SimpleNamespace(handler=handler)):
        install()
        tool_executor.note_tool_result(agent, 'fixture-tool', call, raw, 'spilled result reference')


def test_native_uncertainty_survives_spill_without_parsing_display_text():
    agent = SimpleNamespace()
    raw = native.tool_error('Native connection lost', outcome_uncertain=True)
    observe(agent, 'one', raw)
    row = {'role':'tool', 'tool_call_id':'one', 'content':'a persisted spill reference'}
    assert unconfirmed(agent, [row])
    assert not unconfirmed(agent, [{**row, 'tool_call_id':'other'}])
    assert not unconfirmed(SimpleNamespace(), [row]), 'another agent does not inherit this result'


@pytest.mark.parametrize('raw,owned', [
    (json.dumps({'result':'{"outcome_uncertain":true}'}), True),
    (native.tool_error('Returned server error'), True),
    (native.tool_error('Untrusted handler output', outcome_uncertain=True), False),
])
def test_known_errors_and_user_output_are_not_classified_as_unknown(raw, owned):
    agent = SimpleNamespace()
    observe(agent, 'one', raw, owned=owned)
    assert not unconfirmed(agent, [{'role':'tool','tool_call_id':'one','content':raw}])


def test_ordinary_tool_batch_does_not_commit_an_uncertain_native_mcp_result():
    agent, matrix = SimpleNamespace(), Mock()
    assistant = SimpleNamespace(tool_calls=[SimpleNamespace(id='one', function=SimpleNamespace(name='fixture-tool',arguments='{}'))])
    messages = []
    def body(*args):
        observe(agent, 'one', native.tool_error('Native transport lost', outcome_uncertain=True))
        messages.append({'role':'tool','tool_call_id':'one','content':'spilled result reference'})
    with pytest.raises(RecoveryConflict, match='requires verification'):
        execute_tools(matrix, {'event_key':'source'}, body, agent, assistant, messages, 'task', 1)
    matrix.message_begin_tools.assert_called_once()
    matrix.message_finish_tools.assert_not_called()


def test_executor_native_unknown_flag_remains_fenced():
    assert unconfirmed(SimpleNamespace(), [{'role':'tool','tool_call_id':'one','effect_disposition':'unknown'}])
    assert not unconfirmed(SimpleNamespace(), [{'role':'user','effect_disposition':'unknown'}])


def test_actual_native_registry_binding_survives_result_spilling():
    name = 'agentteams_mcp_outcome_fixture'
    registry.register(name, 'mcp_fixture', {'name':name,'parameters':{'type':'object'}},
                      native._make_tool_handler('fixture', 'write', 1))
    agent = SimpleNamespace()
    with patch.object(tool_executor, 'note_tool_result', new=lambda *args: None):
        install()
        tool_executor.note_tool_result(agent, name, 'actual-call',
            native.tool_error('Native interrupted operation', outcome_uncertain=True), 'spilled reference')
    assert unconfirmed(agent, [{'role':'tool','tool_call_id':'actual-call','content':'spilled reference'}])


def test_known_native_mcp_failure_reaches_shared_receipt_after_spilling():
    agent, matrix = SimpleNamespace(), Mock()
    assistant = SimpleNamespace(tool_calls=[SimpleNamespace(id='failed', function=SimpleNamespace(name='fixture-tool', arguments='{}'))])
    messages = []
    def body(*args):
        observe(agent, 'failed', native.tool_error('Concrete returned HTTP failure'))
        messages.append({'role':'tool', 'tool_call_id':'failed', 'content':'spilled reference'})
    execute_tools(matrix, {'event_key':'source'}, body, agent, assistant, messages, 'task', 1)
    result = matrix.message_finish_tools.call_args.args[2][0]
    assert result['status'] == 'error'
    assert result['error'] == 'Concrete returned HTTP failure'
    assert result['content'] == 'spilled reference'


def test_success_with_a_nested_error_word_remains_success():
    from hermes_matrix.mcp_outcomes import receipts
    agent = SimpleNamespace()
    observe(agent, 'ok', json.dumps({'result':{'error':'a user-owned field', 'idempotent_hit':True}}))
    result = receipts(agent, [{'role':'tool','tool_call_id':'ok','content':'native successful result'}])[0]
    assert 'status' not in result
