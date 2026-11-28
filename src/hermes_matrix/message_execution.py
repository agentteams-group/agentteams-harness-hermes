"""Route durable ordinary room inputs to Hermes or deliver their saved replies."""
import asyncio
from contextvars import ContextVar
from copy import copy
from dataclasses import dataclass
from functools import wraps
from types import SimpleNamespace
import json
import logging
import time

from task_coordination import requires_room_history
from task_recovery import RecoveryConflict
from worker_sessions import WorkerSessions
from worker_matrix_state import matrix_event_key
from hermes_matrix.session_cache import session_fingerprint

logger = logging.getLogger(__name__)
# The pinned gateway copies this context when it submits the agent worker thread.
ordinary_turn = ContextVar("hermes_ordinary_turn", default=None)


def execute_tools(matrix, identity, native, agent, assistant, messages, effective_task_id, api_call_count):
    # Only the resolver installed by our current native frame delegates to the
    # canonical authority. A similarly named external MCP tool does not.
    from hermes_matrix.public_tasks import current_turn, is_canonical_task_call
    public = current_turn.get()
    canonical = [call for call in assistant.tool_calls if public and is_canonical_task_call(call)]
    business = [call for call in assistant.tool_calls if call not in canonical]
    saved = None
    if business:
        calls = [{"id": call.id, "name": call.function.name, "arguments": call.function.arguments} for call in business]
        operation = matrix.message_begin_tools(identity, calls)
        selected = copy(assistant)
        selected.tool_calls = business
        offset = len(messages)
        native(agent, selected, messages, effective_task_id, api_call_count)
        from hermes_matrix.mcp_outcomes import unconfirmed
        if unconfirmed(agent, messages[offset:]):
            raise RecoveryConflict("Native Hermes tool effect requires verification")
        from hermes_matrix.mcp_outcomes import receipts
        results = receipts(agent, messages[offset:])
        saved = matrix.message_finish_tools(identity, operation, results)
    if canonical:
        selected = copy(assistant)
        selected.tool_calls = canonical
        native(agent, selected, messages, effective_task_id, api_call_count)
    return saved or matrix.message_current(identity)


def install_ordinary_guard():
    from run_agent import AIAgent
    from gateway import run_turn_runner
    original = AIAgent._execute_tool_calls
    if getattr(original, "_agentteams_ordinary", False):
        return

    @wraps(original)
    def guarded(agent, assistant_message, messages, effective_task_id, api_call_count=0):
        turn = ordinary_turn.get()
        if turn is None:
            return original(agent, assistant_message, messages, effective_task_id, api_call_count)
        # Nested agents run inside their parent's already fenced tool call.
        if agent.session_id != turn.session.session_id:
            raise RecoveryConflict("Ordinary tool batch belongs to another native session")
        scope = ordinary_turn.set(None)
        try:
            saved = execute_tools(turn.matrix, turn.identity, original, agent, assistant_message, messages, effective_task_id, api_call_count)
            turn.checkpoint_digest = saved["checkpoint_digest"]
        finally:
            ordinary_turn.reset(scope)

    native_held = run_turn_runner.stream_held
    # Native held-stream mode still runs the full model turn. Only the final
    # text is published, after its bytes and stable transaction are durable.
    run_turn_runner.stream_held = lambda: ordinary_turn.get() is not None or native_held()
    AIAgent._execute_tool_calls = guarded
    guarded._agentteams_ordinary = True


class PendingInputs:
    def __init__(self, adapter, matrix):
        self.adapter, self.matrix = adapter, matrix
        self.lock = asyncio.Lock()
        self.live = {}
        self.running = set()

    async def permitted(self, room, sender, *, thread=None):
        adapter = self.adapter
        is_dm = await adapter._is_dm_room(room)
        return (not adapter._is_self_sender(sender) and not adapter._is_system_or_bridge_sender(sender)
                and not any(pattern.search(sender) for pattern in adapter._ignored_user_patterns)
                and await adapter._is_allowed_matrix_room_event(room)
                and adapter._dual_allow.permits(sender, is_dm=is_dm)
                and adapter._is_sender_authorized(sender, chat_type="dm" if is_dm else "group", chat_id=room, thread_id=thread) is True)

    async def drain(self):
        if self.lock.locked():
            return
        async with self.lock:
            snapshot = await asyncio.to_thread(self.matrix.load)
            live, self.live = self.live, {}
            if not snapshot["state"]["pending"]:
                return
            joined = None
            busy = set()
            history = {}
            for key in snapshot["state"]["pending"]:
                entry = await asyncio.to_thread(self.matrix.input, key)
                if entry is None:
                    raise RecoveryConflict("Pending Matrix input disappeared")
                room = entry["room_id"]
                if entry["intent"] == "context":
                    history.setdefault(room, []).append(entry)
                    continue
                if entry["intent"] == "control":
                    if (key not in self.running and not entry.get("native_commit")
                            and (not snapshot["state"].get("rooms", {}).get(room, {}).get("gaps")
                                 or not await asyncio.to_thread(requires_room_history, entry, str(self.adapter._user_id)))):
                        try:
                            if joined is None:
                                joined = set(await self.adapter._client.get_joined_rooms())
                            if room not in joined:
                                continue
                            await self.resume_control(key, entry)
                        except Exception as exc:
                            logger.warning("Matrix control %s remains pending (%s)", key, type(exc).__name__)
                    continue
                if entry["intent"] not in {"message", "task_notification"}:
                    continue
                try:
                    if joined is None:
                        joined = set(await self.adapter._client.get_joined_rooms())
                    if room not in joined:
                        continue
                    if entry["intent"] == "task_notification":
                        if (not entry.get("native_commit")
                                and (not snapshot["state"].get("rooms", {}).get(room, {}).get("gaps")
                                     or not await asyncio.to_thread(requires_room_history, entry, str(self.adapter._user_id)))
                                and await self.adapter._is_allowed_matrix_room_event(room)):
                            await self.resume_assignment(entry)
                        continue
                    relation = entry["input"]["content"].get("m.relates_to") or {}
                    thread = relation.get("event_id") if relation.get("rel_type") == "m.thread" else None
                    if not await self.permitted(room, entry["input"]["sender"], thread=thread):
                        continue
                    view = await asyncio.to_thread(self.matrix.message, key)
                    if view["recovery"] == "backfill":
                        continue
                    if key in self.running or (room in busy and view["recovery"] == "resume"):
                        busy.add(room)
                        continue
                    claimed = await asyncio.to_thread(self.matrix.message_claim, key)
                    mode = claimed["recovery"]
                    if mode == "deliver":
                        await self.deliver(claimed)
                    elif mode == "resume":
                        busy.add(room)
                        await self.resume(key, claimed, live.get(key), history.get(room, []))
                    elif mode == "verify":
                        busy.add(room)
                except Exception as exc:
                    busy.add(room)
                    logger.warning("Matrix work %s remains pending (%s)", key, type(exc).__name__)

    async def resume_control(self, key, entry):
        from task_coordination import IgnoredNotification, notification
        source = entry["input"]
        if "org.agentteams.coordination" not in source["content"]:
            return
        try:
            packet = await asyncio.to_thread(notification, source["content"], entry["room_id"],
                source["sender"], str(self.adapter._user_id), event_id=entry["event_id"])
        except IgnoredNotification:
            return
        if not packet or packet["meta"]["teamharness_coordination"]["kind"] == "task_assigned":
            return
        if not await self.adapter._is_allowed_matrix_room_event(entry["room_id"]):
            return
        event = SimpleNamespace(room_id=entry["room_id"], event_id=entry["event_id"], sender=source["sender"], content=source["content"])
        if await self.adapter._dispatch_coordination(event, packet):
            self.running.add(key)

    async def resume_assignment(self, entry):
        # A limited batch may have withheld native admission after the source
        # was retained. Revalidate its original receipt and exact Task scope.
        from task_coordination import IgnoredNotification, notification
        work = entry.get("work") or {}
        if work.get("kind") != "task" or not work.get("team_id") or not work.get("task_id"):
            raise RecoveryConflict("Stored assignment has no Worker Task scope")
        source = entry["input"]
        try:
            packet = await asyncio.to_thread(notification, source["content"], entry["room_id"],
                source["sender"], str(self.adapter._user_id), event_id=entry["event_id"])
        except IgnoredNotification:
            return
        canonical = packet["meta"]["teamharness_coordination"] if packet else {}
        if canonical.get("kind") != "task_assigned" or canonical.get("task_id") != work["task_id"]:
            raise RecoveryConflict("Stored notification and current assignment differ")
        event = SimpleNamespace(event_id=entry["event_id"], sender=source["sender"], room_id=entry["room_id"])
        await self.adapter._recover_tasks(work["task_id"], only_team=work["team_id"], source_event=event)

    async def deliver(self, saved):
        from hermes_matrix.saved_reply import deliver, check_binding
        if saved["recovery"] != "deliver" or saved["execution"]["state"] != "active":
            raise RecoveryConflict("Worker cannot send an unowned or unverified reply")
        entry = saved["input"]
        relation = entry["input"]["content"].get("m.relates_to") or {}
        reply = {"m.in_reply_to": {"event_id": entry["event_id"]}}
        if relation.get("rel_type") == "m.thread" and relation.get("event_id"):
            reply.update(rel_type="m.thread", event_id=relation["event_id"], is_falling_back=True)
        async def bind(cp):
            fresh = await asyncio.to_thread(self.matrix.message_checkpoint, saved["identity"], cp)
            check_binding(saved["execution"], cp, fresh)
        result = await deliver(self.adapter._client, entry["room_id"], saved["execution"]["checkpoint"],
            saved["saved_result"], {"msgtype": "m.text", "m.relates_to": reply}, bind)
        await asyncio.to_thread(self.matrix.message_delivered, saved["identity"], str(result))
        return str(result)

    async def resume(self, key, claimed, native_event, history):
        from gateway.platforms.event import MessageEvent
        entry = claimed["input"]
        room, event_id = entry["room_id"], entry["event_id"]
        sender, content = entry["input"]["sender"], entry["input"]["content"]
        runner = getattr(self.adapter._message_handler, "__self__", None)
        if runner is None or getattr(runner, "session_store", None) is None:
            raise RecoveryConflict("Native Hermes session store is unavailable")
        if native_event is None:
            relation = content.get("m.relates_to") or {}
            thread = relation.get("event_id") if relation.get("rel_type") == "m.thread" else None
            source = self.adapter.build_source(chat_id=room, chat_type="dm" if await self.adapter._is_dm_room(room) else "group",
                user_id=sender, message_id=event_id, thread_id=thread)
            native_event = MessageEvent(text=claimed["resume_text"], source=source, message_id=event_id,
                raw_message=content, allow_gateway_control=False)
            # Retain only the same-room context admitted by Matrix policy.
            limit = self.adapter._history.limit
            allowed = [item["input"] for item in (history[-limit:] if limit else [])
                       if await self.permitted(room, item["input"]["sender"], thread=thread)]
            if allowed and await asyncio.to_thread(requires_room_history, entry, str(self.adapter._user_id)):
                native_event.text += "\nEarlier room messages (context only):\n" + json.dumps(allowed, ensure_ascii=False)
        elif claimed["execution"].get("checkpoint"):
            native_event.text = claimed["resume_text"] + "\n" + native_event.text
        from hermes_matrix.task_inputs import steer
        if await steer(self.adapter, entry) is not None:
            return
        session_key = runner._session_key_for_source(native_event.source)
        if runner._is_session_running(session_key) or session_key in self.adapter._active_sessions:
            return
        identity = claimed["identity"]
        sessions = WorkerSessions("hermes", recovery=self.matrix.recovery)
        work = {"kind": "message", "event_key": key}
        view = await asyncio.to_thread(sessions.load, work)
        if view["generation"] != identity["generation"] or view["checkpoint_digest"] != claimed["checkpoint_digest"]:
            raise RecoveryConflict("Matrix message checkpoint changed before session selection")
        store = runner.session_store
        current = await asyncio.to_thread(store.lookup_by_session_key, session_key)
        fingerprint = await asyncio.to_thread(session_fingerprint, store, current)
        reuse = sessions.usable(view, current.session_id if current else None, fingerprint)
        session = await asyncio.to_thread(store.get_or_create_session, native_event.source, force_new=not reuse, touch_activity=False)
        if not reuse or session.session_id != current.session_id:
            runner._evict_cached_agent(session_key)
        latest = await asyncio.to_thread(self.matrix.message_current, identity)
        if latest["checkpoint_digest"] != view["checkpoint_digest"]:
            raise RecoveryConflict("Native session was superseded before admission")
        native_event._agentteams_message = NativeTurn(self.matrix, identity, sessions, work, view["config_generation"],
            view["checkpoint_digest"], session, store)
        self.running.add(key)
        try:
            await self.adapter.handle_message(native_event)
            if not getattr(native_event, "_gateway_accepted", False):
                raise RecoveryConflict("Native Hermes gateway deferred ordinary work")
        except BaseException:
            self.running.discard(key)
            raise



@dataclass
class NativeTurn:
    matrix: object
    identity: dict
    sessions: WorkerSessions
    work: dict
    config_generation: int
    checkpoint_digest: str
    session: object
    store: object

    def remember_input(self, agent):
        if (agent.session_id != self.session.session_id
                or not getattr(agent, "_persist_user_message_platform_id", None)
                or matrix_event_key(agent._chat_id, agent._persist_user_message_platform_id) != self.work["event_key"]):
            raise RecoveryConflict("Hermes persisted a different source or native session")
        view = self.sessions.load(self.work)
        if view["checkpoint_digest"] != self.checkpoint_digest or view["config_generation"] != self.config_generation:
            raise RecoveryConflict("Native input belongs to an older checkpoint or configuration")
        fingerprint = session_fingerprint(self.store, self.session)
        if fingerprint:
            self.sessions.remember(self.work, view, self.identity, agent.session_id, fingerprint, int(time.time()) + 86400)
