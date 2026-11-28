"""Translate Core-admitted Task inputs to Hermes' ticketed native steer queue."""
import os
from pathlib import Path

import yaml

from runtime_control import task_supplement, admit_native_supplement, retire_native_supplement
from task_recovery import Recovery
from worker_matrix_state import matrix_event_key


async def steer(adapter, entry):
    run = adapter._task_runs.get(entry['room_id'])
    if not run:
        return None
    event = run['event']
    identity = getattr(event, '_agentteams_recovery', None)
    source = entry['input']
    content = source['content']
    runtime = yaml.safe_load(Path(os.environ['TEAMHARNESS_RUNTIME_CONFIG']).read_text())
    if not task_supplement(runtime, {**identity, 'room_id': event.source.chat_id}, {
            'room_id': entry['room_id'], 'event_id': entry['event_id'], 'sender': source['sender'],
            'body': content.get('body'), 'mentions': (content.get('m.mentions') or {}).get('user_ids', []),
            'answer': content.get('com.agentteams.answer'), 'execution_control': content.get('com.agentteams.control')}):
        return None
    runner = getattr(adapter._message_handler, '__self__', None)
    agent = runner._running_agents.get(run['session_key']) if runner else None
    turn = getattr(event, '_agentteams_task_turn', None)
    if agent is None or turn is None:
        return False  # Native startup/finish boundary: leave the durable input pending.
    key = matrix_event_key(entry['room_id'], entry['event_id'])
    if key in turn.supplements:
        return True
    import asyncio
    from worker_matrix_state import WorkerMatrix
    if not await asyncio.to_thread(admit_native_supplement, WorkerMatrix('hermes'), key):
        return True
    text = '[Message ' + entry['event_id'] + ']\n' + content['body']
    if not agent.steer(text, ticket=key):
        return False
    turn.supplements[key] = {'entry': entry, 'text': text, 'agent': agent}
    return True


def commit(turn, agent, messages, conversation_history):
    candidates = {key: item for key, item in list(turn.supplements.items())
                  if any(row.get('role') == 'user' and item['text'] in str(row.get('content', '')) for row in messages)}
    if not candidates:
        return
    from worker_matrix_state import WorkerMatrix
    matrix = WorkerMatrix('hermes')
    agent._ensure_db_session()
    agent._persist_session(messages, conversation_history)
    rows = agent._session_db.get_messages(agent.session_id, include_ancestors=True)
    digest = Recovery.digest({'session_id': agent.session_id, 'messages': rows})
    for key, item in candidates.items():
        if not any(row.get('role') == 'user' and item['text'] in str(row.get('content', '')) for row in rows):
            raise RuntimeError('Hermes has not persisted the supplementary input')
        snapshot = matrix.native_input(key)
        if not matrix.native_current(snapshot, agent.session_id, digest):
            matrix.native_committed(key, snapshot, agent.session_id, digest)
        retire_native_supplement(matrix, key)
        agent.withdraw_steer(key)
        turn.supplements.pop(key)


def close(event):
    turn = getattr(event, '_agentteams_task_turn', None)
    for key, item in list(getattr(turn, 'supplements', {}).items()):
        item['agent'].withdraw_steer(key)
    # Unconsumed inputs stay in Core's pending queue for the next ordinary turn.
