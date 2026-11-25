import asyncio
import importlib.util
from types import SimpleNamespace

from hermes_matrix.progress import MatrixProgress


def test_native_failure_survives_gateway_error_handling():
    async def check():
        sent = []
        async def send(room, content):
            sent.append(content)
            return '$failed'
        display = MatrixProgress(send, SimpleNamespace(message_id='$source', source=SimpleNamespace(chat_id='!task')))
        display.failed = True
        await display.finish()
        assert sent[0]['org.agentteams.run']['blocks'][0]['kind'] == 'failed'
    asyncio.run(check())


def test_parallel_same_name_calls_keep_exact_ids_results_and_one_run():
    if importlib.util.find_spec('agent') is None:
        import pytest
        pytest.skip('Requires the installed Hermes SDK; also run by native_public_tasks_smoke')
    async def check():
        sent = []
        async def send(room, content):
            assert room == '!task'
            sent.append(content)
            return '$' + str(len(sent))
        event = SimpleNamespace(message_id='$source', source=SimpleNamespace(chat_id='!task'))
        display = MatrixProgress(send, event)
        display.reasoning('独立检查页面。')
        display.tool_started('one', 'read_file', {'path': 'index.html', 'api_key': 'private'})
        display.tool_started('two', 'read_file', {'path': 'missing.html'})
        display.tool_completed('two', 'read_file', {'path': 'missing.html'}, '{"error":"not found"}')
        display.tool_completed('one', 'read_file', {'path': 'index.html', 'api_key': 'private'}, '{"content":"<html>ok</html>"}')
        await display.finish()
        blocks = [(row.get('m.new_content') or row)['org.agentteams.run']['blocks'][0] for row in sent]
        assert len(blocks) == 6
        assert blocks[0] == {'type': 'thinking', 'content': '独立检查页面。'}
        assert [block['tool_call_id'] for block in blocks[1:5]] == ['one', 'two', 'two', 'one']
        assert blocks[3]['status'] == 'failed' and blocks[4]['status'] == 'succeeded'
        assert blocks[4]['arguments']['api_key'] == '[REDACTED]'
        assert blocks[4]['result'] == '{"content":"<html>ok</html>"}'
        assert sent[3]['m.relates_to']['event_id'] == '$3'
        assert sent[4]['m.relates_to']['event_id'] == '$2'
        assert blocks[5]['kind'] == 'quiet'
        assert all((row.get('m.new_content') or row)['org.agentteams.progress_only'] for row in sent)
        assert len({(row.get('m.new_content') or row)['org.agentteams.run']['run_id'] for row in sent}) == 1
    asyncio.run(check())


def test_successful_task_phase_ends_with_its_result_and_keeps_its_run_on_recovery():
    async def check():
        sent = []
        async def send(room, content):
            sent.append(content)
        identity = {"team_id": "squad", "task_id": "stage", "generation": 1}
        run_ids = []
        for source in ("$assignment", "recovery:squad/stage:1"):
            event = SimpleNamespace(message_id=source, source=SimpleNamespace(chat_id="!task"),
                                    _agentteams_recovery=identity)
            display = MatrixProgress(send, event)
            run_ids.append(display.run_id)
            await display.finish(phase_complete=True)
        assert run_ids[0] == run_ids[1]
        assert sent == []
    asyncio.run(check())
