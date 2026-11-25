"""Installed Hermes SDK admission smoke; no model or external service required."""
import asyncio
import json
import os
import tempfile
import threading
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from pathlib import Path
from types import SimpleNamespace
from unittest.mock import AsyncMock

from gateway.config import PlatformConfig
from mautrix.types import MessageEvent as MatrixEvent
from hermes_matrix.overlay_adapter import MatrixAdapter, _coordination_turn


def main():
    with tempfile.TemporaryDirectory() as tmp:
        token = Path(tmp) / 'token'
        token.write_text('scoped-test-token')
        os.environ.update(AGENTTEAMS_AUTH_TOKEN_FILE=str(token), AGENTTEAMS_AGENT_ROLE='leader', MATRIX_ALLOWED_USERS='@lead:test')
        current = {}
        class API(BaseHTTPRequestHandler):
            def log_message(self, *args): pass
            def do_GET(self):
                assert self.headers['Authorization'] == 'Bearer scoped-test-token'
                value = current['receipt'] if '/coordination/' in self.path else current['task']
                self.send_response(200); self.end_headers(); self.wfile.write(json.dumps(value).encode())
        api = ThreadingHTTPServer(('127.0.0.1', 0), API)
        threading.Thread(target=api.serve_forever, daemon=True).start()
        os.environ['AGENTTEAMS_CONTROLLER_URL'] = f'http://127.0.0.1:{api.server_port}'
        async def check():
            adapter = MatrixAdapter(PlatformConfig(enabled=True))
            adapter._user_id = '@lead:test'
            adapter._is_allowed_matrix_room_event = AsyncMock(return_value=True)
            adapter._is_duplicate_event = lambda _: False
            adapter.handle_message = AsyncMock()
            for kind in ['task_assigned', 'task_submitted', 'handoff', 'main_task_ready']:
                event = dict(id='a'*32, kind=kind, task_id='t', main_task_id='m', project_id='', delivery_id='d', submission_id='s')
                control = {'session_id': f'coordination:{kind}', 'channel_meta': {'sender_id':'@lead:test','agent_matrix_user_id':'@lead:test','room_id':'!r:test','teamharness_coordination':event}, 'input':[{'content':[{'text': 'Canonical '+kind}]}]}
                current['receipt'] = dict(status='running', native_id='matrix:$'+kind, actor='@lead:test', recipient='@lead:test', room_id='!r:test', control=control)
                current['task'] = dict(task_id='t', main_task_id='m', project_id='', status='assigned' if kind=='task_assigned' else 'submitted', assigned_to='@lead:test', notifyTxn='d', submission_id='s') if kind != 'main_task_ready' else dict(task_kind='main', status='in_progress',tasks=[dict(task_id='t',status='completed')])
                raw = MatrixEvent.deserialize({'type':'m.room.message', 'room_id':'!r:test', 'sender':'@lead:test', 'event_id':'$'+kind, 'origin_server_ts':0, 'content':{'msgtype':'m.text', 'org.agentteams.coordination':control, 'body':'Visible projection'}})
                await adapter._on_room_message(raw)
                assert adapter.handle_message.call_count == ['task_assigned','task_submitted','handoff','main_task_ready'].index(kind)+1
                message = adapter.handle_message.call_args.args[0]
                assert message.text == 'Canonical '+kind
                assert message.source.thread_id == '$'+kind
                assert kind in adapter._event_session_key(message)
                assert message.internal is (kind in {'main_task_ready', 'handoff'})
                assert message.allow_gateway_control is False
            current['task']['tasks'][0]['status'] = 'submitted'
            await adapter._on_room_message(raw)
            assert adapter.handle_message.call_count == 4
            adapter._client = SimpleNamespace(send_message_event=AsyncMock(return_value='$sent'))
            adapter._wrap_send_message_event()
            scope = _coordination_turn.set(True)
            try:
                content = {'msgtype':'m.text','body':'@worker:test Accepted'}
                await adapter._client.send_message_event('!r:test','m.room.message',content)
                assert content['org.agentteams.progress_only'] is True
                assert content['m.mentions'] == {'user_ids': []}
            finally:
                _coordination_turn.reset(scope)
        try: asyncio.run(check())
        finally: api.shutdown()
    print('Hermes native admission, isolated sessions, stale closure and progress echo: passed')

if __name__ == '__main__': main()
