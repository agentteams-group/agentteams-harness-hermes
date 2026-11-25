import asyncio
from types import SimpleNamespace

from hermes_matrix.progress import MatrixProgress


def test_parallel_same_name_calls_keep_exact_ids_results_and_one_run():
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
