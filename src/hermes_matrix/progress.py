"""Publish native lifecycle callbacks as ID-correlated Matrix run blocks."""
import asyncio
from contextvars import ContextVar
import json
import threading
import time

current_progress = ContextVar('agentteams_matrix_progress', default=None)


def bounded(value):
    from runtime_observability import redact
    text = redact(json.dumps(value, ensure_ascii=False, default=str))
    return json.loads(text) if len(text) <= 4096 else {'preview': text[:4096], 'truncated': True}


def task_run_id(team, task, generation):
    return f"hermes:{team}/{task}:{generation}"


class MatrixProgress:
    def __init__(self, send, event):
        self.send, self.room = send, event.source.chat_id
        identity = getattr(event, '_agentteams_recovery', None) or {}
        self.run_id = (task_run_id(identity["team_id"], identity["task_id"], identity["generation"])
                       if identity else f"hermes:{event.message_id}:0")
        self.loop = asyncio.get_running_loop()
        self.lock, self.reasoning_lock = asyncio.Lock(), threading.Lock()
        self.pending, self.calls, self.reasoning_text = [], {}, ''
        self.failed = False

    def submit(self, operation):
        self.pending.append(asyncio.run_coroutine_threadsafe(operation, self.loop))

    def reasoning(self, delta):
        with self.reasoning_lock:
            self.reasoning_text = (self.reasoning_text + delta)[:4096]

    def take_reasoning(self):
        with self.reasoning_lock:
            text, self.reasoning_text = self.reasoning_text, ''
        return text

    def tool_started(self, identity, name, arguments):
        from runtime_observability import redact
        text = redact(self.take_reasoning())[:4096]
        if text:
            self.submit(self.publish('reasoning:' + identity, {'type': 'thinking', 'content': text}))
        self.submit(self.tool(identity, name, arguments))

    def tool_completed(self, identity, name, arguments, result):
        self.submit(self.tool(identity, name, arguments, result, finished=True))

    async def tool(self, identity, name, arguments, result=None, *, finished=False):
        from agent.display import _detect_tool_failure
        block = {'type': 'tool_call', 'tool_name': name, 'tool_call_id': identity,
                 'arguments': bounded(arguments or {}), 'status': 'running'}
        if finished:
            failed, _ = _detect_tool_failure(name, result)
            block.update(status='failed' if failed else 'succeeded', result=bounded(result), finished_at=int(time.time() * 1000))
        await self.publish('tool:' + identity, block)

    async def publish(self, key, block):
        async with self.lock:
            previous = self.calls.get(key)
            if block['type'] == 'tool_call':
                block['started_at'] = previous['started_at'] if previous else int(time.time() * 1000)
            content = {'msgtype': 'm.notice', 'body': block.get('tool_name') or '执行过程',
                       'org.agentteams.progress_only': True, 'm.mentions': {'user_ids': []},
                       'org.agentteams.run': {'version': '1', 'run_id': self.run_id, 'blocks': [block]}}
            if previous:
                content = {**content, 'm.new_content': content,
                           'm.relates_to': {'rel_type': 'm.replace', 'event_id': previous['event_id']}}
            event_id = await self.send(self.room, content)
            self.calls.setdefault(key, {'event_id': event_id, 'started_at': block.get('started_at')})

    async def finish(self, *, failed=False, phase_complete=False):
        failed = failed or self.failed
        from runtime_observability import redact
        text = redact(self.take_reasoning())[:4096]
        if text:
            self.submit(self.publish('reasoning:final', {'type': 'thinking', 'content': text}))
        for future in self.pending:
            await asyncio.wrap_future(future)
        if phase_complete and not failed:
            return
        await self.publish('ending', {'type': 'error', 'kind': 'failed' if failed else 'quiet',
                                     'title': '执行失败' if failed else '已处理'})
