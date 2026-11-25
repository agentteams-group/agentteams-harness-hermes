"""AgentTeams policy overlay on hermes-agent's native Matrix adapter.

This module no longer reimplements Matrix transport with ``matrix-nio``.
Instead, it subclasses hermes-agent's stock mautrix-based adapter and only
injects the AgentTeams-specific policy layer that differs from upstream:

  * outbound ``m.mentions`` enrichment for full-MXID pings
  * separate DM / group allow-lists
  * copaw-style history buffering for unmentioned group chatter
  * image downgrade when the active model has no vision capability

All transport-heavy capabilities (media upload/download, streaming edits,
typing, reactions, threads, E2EE, read receipts) stay in the upstream adapter.
"""
from __future__ import annotations

import logging
import asyncio
import json
import os
import time
from pathlib import Path
import types
from contextvars import ContextVar
from typing import Any, Dict, Optional

from gateway.config import PlatformConfig
from gateway.platforms.event import MessageType
from plugins.platforms.matrix.adapter import MatrixAdapter as _NativeMatrixAdapter
from hermes_matrix.approvals import MatrixApprovals, KEY as APPROVAL_KEY

from hermes_matrix.policies import (
    DualAllowList,
    HistoryBuffer,
    apply_outbound_mentions,
)

logger = logging.getLogger(__name__)
_coordination_turn = ContextVar("agentteams_task_notification", default=False)
_matrix_source_event = ContextVar("agentteams_matrix_source", default=None)
try:
    from runtime_observability import event as trace_event
except ImportError:
    def trace_event(*args, **kwargs):
        pass  # Standalone packages can run without container logging.

try:
    from agentteams_receipts import Receipts, capture_audience, credentials, matrix_request
except ModuleNotFoundError:
    Receipts = None  # Standalone older installations do not advertise support.
_IMAGE_FILENAME_EXTENSIONS = frozenset({
    ".png",
    ".jpg",
    ".jpeg",
    ".gif",
    ".webp",
    ".bmp",
    ".tiff",
    ".tif",
    ".heic",
    ".heif",
})


def _truthy_env(name: str, default: bool = False) -> bool:
    raw = os.getenv(name)
    if raw is None:
        return default
    return raw.strip().lower() in ("true", "1", "yes", "on")


def _is_commandish(text: str) -> bool:
    return (text or "").startswith(("/", "!"))


def _normalize_image_body(body: str) -> str:
    """Drop Matrix transport filenames so image-only messages stay image-first.

    Matrix image events commonly populate ``body`` with the uploaded filename
    even when the user did not provide any textual caption. Feeding that raw
    filename to Hermes causes the downstream agent to treat the turn like a
    file lookup (for example ``search_files("foo.png")``) instead of relying
    on the already-cached image plus automatic vision enrichment.
    """
    stripped = (body or "").strip()
    if not stripped:
        return ""
    if "\n" in stripped or "\r" in stripped:
        return stripped

    leaf = stripped.rsplit("/", 1)[-1].rsplit("\\", 1)[-1]
    stem, ext = os.path.splitext(leaf)
    if stem and ext.lower() in _IMAGE_FILENAME_EXTENSIONS:
        return ""
    return stripped


def _describe_dropped_media(source_content: dict, fallback_body: str) -> str:
    """Return a copaw-style textual summary for a media event."""
    body = (fallback_body or "").strip()
    msgtype = source_content.get("msgtype", "")
    if msgtype == "m.image":
        return f"[sent an image: {body}]" if body else "[sent an image]"
    if msgtype == "m.file":
        return f"[sent a file: {body}]" if body else "[sent a file]"
    if msgtype == "m.audio":
        return f"[sent audio: {body}]" if body else "[sent audio]"
    if msgtype == "m.video":
        return f"[sent a video: {body}]" if body else "[sent a video]"
    return body


class MatrixAdapter(_NativeMatrixAdapter):
    """Thin subclass that overlays AgentTeams policy on hermes native Matrix."""

    _session_receipts = None

    def __init__(self, config: PlatformConfig):
        super().__init__(config)
        self._dual_allow = DualAllowList.from_env()
        self._history = HistoryBuffer.from_env()
        self._vision_enabled = _truthy_env("MATRIX_VISION_ENABLED", default=False)
        self._session_receipts = Receipts(os.getenv("AGENTTEAMS_WORKER_HOME") or os.environ["HOME"]) if Receipts else None
        self._direct_runs: dict[str, dict] = {}
        self._dashboard_approvals = MatrixApprovals(self._send_dashboard_approval)
        self._recovered_admissions = set()
        self._task_recovery_lock = asyncio.Lock()
        if os.getenv("AGENTTEAMS_CONTROLLER_URL") and os.getenv("AGENTTEAMS_PROCESS_ID"):
            from hermes_matrix.input_commit import install_native_input_guard
            install_native_input_guard()

    async def _recover_tasks(self, only_task=None, *, only_team=None, claim_only=False, source_event=None):
        if not os.getenv("AGENTTEAMS_CONTROLLER_URL") or not os.getenv("AGENTTEAMS_PROCESS_ID"):
            return
        from task_recovery import Recovery, enabled
        if not enabled():
            return
        async with self._task_recovery_lock:
            await self._recover_tasks_serial(only_task, only_team=only_team, claim_only=claim_only, source_event=source_event)

    async def _recover_tasks_serial(self, only_task, *, only_team, claim_only, source_event):
        from task_recovery import Recovery, RecoveryConflict
        from worker_matrix_state import WorkerMatrix
        from worker_sessions import WorkerSessions
        from hermes_matrix.session_cache import session_fingerprint
        recovery = Recovery(device_delivery=True)
        await asyncio.to_thread(WorkerMatrix("hermes", recovery=recovery).start)
        sessions = WorkerSessions("hermes", recovery=recovery)
        snapshot = await asyncio.to_thread(recovery.discover)
        from gateway.platforms.event import MessageEvent
        for entry in snapshot["tasks"]:
            task_id, team = entry["task"]["task_id"], entry["team_id"]
            if only_task is not None and task_id != only_task:
                continue
            if only_team is not None and team != only_team:
                continue
            if source_event and entry["task"].get("room_id") != str(source_event.room_id):
                continue
            claimed = await asyncio.to_thread(recovery.claim, snapshot, entry)
            if claim_only:
                continue
            task = claimed["task"]
            if task["execution"]["state"] == "needs_verification":
                trace_event("task.recovery.verify", task_id=task_id, team_id=team,
                    generation=task["execution"]["generation"])
                continue
            admission = (team, task_id, task["execution"]["generation"])
            cp = task["execution"].get("checkpoint") or {}
            room = task["room_id"]
            if cp.get("delivery") == "pending":
                from hermes_matrix.saved_reply import task_reply
                result = await task_reply(self, recovery, task_id, team, room, claimed, claimed["saved_result"])
                await asyncio.to_thread(recovery.delivered, task_id, team, str(result))
                task = await asyncio.to_thread(recovery.current, task_id, team)
                if task is None:
                    continue
            if task["status"] == "submitted":
                continue
            if admission in self._recovered_admissions:
                continue
            runner = getattr(self._message_handler, "__self__", None)
            if runner is None or getattr(runner, "session_store", None) is None:
                raise RuntimeError("native Hermes session store is unavailable for Task recovery")
            store = runner.session_store
            work = {"kind": "task", "team_id": team, "task_id": task_id}
            view = await asyncio.to_thread(sessions.load, work)
            if view["generation"] != task["execution"]["generation"] or view["checkpoint_digest"] != recovery.digest(task["execution"].get("checkpoint")):
                raise RecoveryConflict("Task checkpoint advanced before native session selection")
            descriptor = view["cache"] or {}
            cached = next((session for session in await asyncio.to_thread(store.list_sessions)
                if session.session_id == descriptor.get("native_ref")), None)
            digest = await asyncio.to_thread(session_fingerprint, store, cached)
            if not sessions.usable(view, cached.session_id if cached else None, digest):
                cached = None
            # Recovery is a trusted local admission, never a forged Matrix
            # source event or a migrated foreign native session.
            event = MessageEvent(text=recovery.resume_text({**task, "team_id": team, "spec": claimed["spec"]}),
                source=self.build_source(chat_id=room, chat_type="group",
                    user_id=str(source_event.sender if source_event else
                        cached.origin.user_id if cached and cached.origin else self._user_id)),
                message_id=str(source_event.event_id) if source_event else "recovery:"+team+"/"+task_id+":"+str(task["execution"]["generation"]), raw_message={},
                internal=True, allow_gateway_control=False)
            event._agentteams_recovery = {"task_id": task_id, "team_id": team,
                **{key: task["execution"][key] for key in ("generation", "instance_id", "process_id")}}
            session_key = runner._session_key_for_source(event.source)
            if runner._is_session_running(session_key) or session_key in self._active_sessions:
                raise RuntimeError("native Hermes session is busy; Task recovery must wait")
            current = await asyncio.to_thread(store.lookup_by_session_key, session_key)
            reuse = bool(cached and current and current.session_id == cached.session_id)
            # The SDK also checks its own expiration and persisted session row.
            # Only a matching shared checkpoint permits attempting native reuse.
            session = await asyncio.to_thread(store.get_or_create_session,
                event.source, force_new=not reuse, touch_activity=False)
            if not reuse or session.session_id != cached.session_id:
                # SessionStore routing alone does not replace a warm AIAgent:
                # its cached transcript would keep writing the previous row.
                runner._evict_cached_agent(session_key)
            event._agentteams_recovery["session_id"] = session.session_id
            trace_event("task.recovery.session", task_id=task_id, team_id=team,
                generation=task["execution"]["generation"], session_id=session.session_id,
                checkpoint_version=cp.get("version", 0), reused=bool(reuse and session.session_id == cached.session_id))
            current = await asyncio.to_thread(recovery.current, task_id, team)
            if (not self._owns_recovery(current, event._agentteams_recovery, recovery.process)
                    or recovery.digest(current["execution"].get("checkpoint")) != view["checkpoint_digest"]):
                raise RuntimeError("Task execution changed before native recovery admission")
            await self.handle_message(event)
            if not getattr(event, "_gateway_accepted", False):
                raise RuntimeError("native Hermes gateway did not admit recovered Task")
            self._recovered_admissions.add(admission)

    @staticmethod
    def _owns_recovery(task, identity, process, *, delivery=False):
        execution = task.get("execution", {}) if task else {}
        return bool(task and task["status"] in {"in_progress", "assigned", "submitted"}
            and execution.get("runtime") == "hermes"
            and execution.get("state") in ({"active", "released"} if delivery else {"active"})
            and identity["process_id"] == process
            and all(execution.get(key) == identity[key] for key in ("generation", "instance_id", "process_id")))

    async def _remember_task_session(self, recovery, identity, saved, session_key):
        from worker_sessions import WorkerSessions
        from task_recovery import RecoveryConflict, RecoveryUnavailable
        from hermes_matrix.session_cache import session_fingerprint
        if saved.get("task", {}).get("status") == "submitted":
            return
        runner = getattr(self._message_handler, "__self__", None)
        store = runner.session_store if runner else None
        entry = await asyncio.to_thread(store.lookup_by_session_key, session_key) if store else None
        if not entry or entry.session_id != identity["session_id"]:
            return
        try:
            sessions = WorkerSessions("hermes", recovery=recovery)
            work = {"kind": "task", "team_id": identity["team_id"], "task_id": identity["task_id"]}
            view = await asyncio.to_thread(sessions.load, work)
            if view["checkpoint_digest"] != recovery.digest(saved["execution"]["checkpoint"]):
                return # Do not relabel old native content with a newer checkpoint.
            digest = await asyncio.to_thread(session_fingerprint, store, entry)
            if not digest:
                return
            expires = int(time.time()) + 86400
            await asyncio.to_thread(sessions.remember, work, view, identity, entry.session_id, digest, expires)
            trace_event("task.recovery.cache", task_id=identity["task_id"], team_id=identity["team_id"],
                session_id=entry.session_id, native_digest=digest, expires_at=expires,
                checkpoint_digest=view["checkpoint_digest"])
        except (RecoveryConflict, RecoveryUnavailable) as exc:
            # Delivery already committed; an optional stale/unavailable cache
            # must rebuild on restart rather than replace the saved reply.
            trace_event("task.recovery.cache_skipped", task_id=identity["task_id"], error_type=type(exc).__name__)

    async def _send_final_text(self, event, session_key, text_content, metadata,
                               is_ephemeral_response, ephemeral_ttl, record_delivery):
        ordinary = getattr(event, "_agentteams_message", None)
        if ordinary:
            saved = await asyncio.to_thread(ordinary.matrix.message_save_reply, ordinary.identity, text_content)
            reply_id = await self._pending_matrix.deliver(saved)
            record_delivery(types.SimpleNamespace(success=True, message_id=reply_id))
            return
        identity = getattr(event, "_agentteams_recovery", None)
        if not identity:
            delivered = record_delivery
            if os.getenv("AGENTTEAMS_AGENT_ROLE") == "manager" and self._session_receipts:
                def delivered(result):
                    record_delivery(result)
                    if result.success and result.message_id and event.message_id:
                        self._session_receipts.enqueue(event.source.chat_id, event.message_id, "completed")
            return await super()._send_final_text(event, session_key, text_content, metadata,
                is_ephemeral_response, ephemeral_ttl, delivered)
        from task_recovery import Recovery
        recovery = Recovery(device_delivery=True)
        task_id, team = identity["task_id"], identity["team_id"]
        from task_recovery import RecoveryConflict
        current = await asyncio.to_thread(recovery.current, task_id, team)
        if not self._owns_recovery(current, identity, recovery.process, delivery=True):
            raise RecoveryConflict("Native Task final callback was replaced or cancelled")
        saved, text_content = await asyncio.to_thread(recovery.prepare_reply, task_id, team, text_content)
        cp = saved["execution"]["checkpoint"]
        if cp.get("delivery") == "delivered":
            record_delivery(types.SimpleNamespace(success=True, message_id=cp["reply_event"]))
            return
        from hermes_matrix.saved_reply import task_reply
        result = await task_reply(self, recovery, task_id, team, event.source.chat_id, saved, text_content)
        saved = await asyncio.to_thread(recovery.delivered, task_id, team, str(result))
        record_delivery(types.SimpleNamespace(success=True, message_id=str(result)))
        # The pinned SDK persists its final transcript before this send hook.
        await self._remember_task_session(recovery, identity, saved, session_key)

    async def _accept_matrix_input(self, room, sender, event_id, content, intent, *, task_id=None):
        from task_recovery import enabled
        if not enabled():
            return
        from worker_matrix_state import WorkerMatrix
        source = _matrix_source_event.get()
        decoded = {"sender": sender, "type": "m.room.message",
            "origin_server_ts": int(getattr(source, "timestamp", 0) or 0), "content": content}
        shared = getattr(self, "_shared_sync", None)
        if shared is not None:
            from hermes_matrix.sync_state import current_admission
            admission = current_admission()
            if admission is not None:
                return await admission.retain(shared.matrix, room, event_id, decoded, intent, task_id)
        async with self._task_recovery_lock:
            return await asyncio.to_thread(WorkerMatrix("hermes").accept,
                room, event_id, decoded, intent, task_id=task_id)

    async def _on_room_message(self, event):
        token = _matrix_source_event.set(event)
        error = None
        try:
            return await self._on_matrix_input(event)
        except Exception as exc:
            error = exc
            raise
        finally:
            if getattr(self, "_shared_sync", None) is not None:
                from hermes_matrix.sync_state import current_admission
                admission = current_admission()
                if admission is not None:
                    admission.error = error
                    admission.ready.set()
            _matrix_source_event.reset(token)

    async def _on_matrix_input(self, event):
        content = getattr(event, "content", None)
        content = content if isinstance(content, dict) else content.serialize() if hasattr(content, "serialize") else {}
        if "org.agentteams.coordination" not in content and content.get("org.agentteams.progress_only") is not True:
            return await super()._on_room_message(event)
        try:
            from task_coordination import notification
            packet = await asyncio.to_thread(notification, content, str(event.room_id), str(event.sender), str(self._user_id))
        except (ImportError, ValueError):
            return
        if not await self._is_allowed_matrix_room_event(str(event.room_id)) or self._is_duplicate_event(str(event.event_id)):
            return
        from task_recovery import enabled
        canonical = packet["meta"]["teamharness_coordination"]
        if canonical["kind"] == "task_assigned" and enabled():
            # Retain this verified Matrix source when its Task input enters
            # the native session. Startup-only recovery has no source event
            # and must never manufacture a Matrix read receipt.
            await self._accept_matrix_input(str(event.room_id), str(event.sender), str(event.event_id),
                content, "task_notification", task_id=canonical["task_id"])
            await self._recover_tasks(canonical["task_id"], source_event=event)
            return
        await self._accept_matrix_input(str(event.room_id), str(event.sender), str(event.event_id), content, "control")
        if await self._dispatch_coordination(event, packet):
            from worker_matrix_state import matrix_event_key
            if getattr(self, "_pending_matrix", None) is not None:
                self._pending_matrix.running.add(matrix_event_key(str(event.room_id), str(event.event_id)))

    async def _dispatch_coordination(self, event, packet):
        # The receipt admits your self-addressed control input without
        # weakening the native own-echo policy for ordinary messages.
        # A real Matrix event starts the native thread/session. Keep opaque
        # Controller receipt session IDs out of Matrix transport thread IDs.
        source = self.build_source(chat_id=str(event.room_id), chat_type="group",
            user_id=str(event.sender), thread_id=str(event.event_id))
        from gateway.platforms.event import MessageEvent
        content = event.content if isinstance(event.content, dict) else event.content.serialize()
        own_control = (str(event.sender) == str(self._user_id)
                       and packet["meta"]["teamharness_coordination"]["kind"] in {"main_task_ready", "assistant_digest"})
        message = MessageEvent(text=packet["text"], source=source,
            message_id=str(event.event_id), raw_message=content,
            internal=own_control, allow_gateway_control=False)
        message._task_protocol_meta = packet["meta"]
        await self.handle_message(message)
        return getattr(message, "_gateway_accepted", False)

    async def _direct_member(self, event) -> bool:
        if not event.source or not event.message_id or not self._client:
            return False
        sender = event.source.user_id
        if not self._dual_allow.permits(sender, is_dm=True):
            return False
        try:
            members = await self._client.get_joined_members(event.source.chat_id)
            return set(members) == {self._user_id, sender}
        except Exception:
            return False

    async def _execution_status(self, run, status):
        from mautrix.types import EventType, RoomID
        await self._client.send_message_event(RoomID(run["room"]), EventType.ROOM_MESSAGE, {
            "msgtype": "m.notice", "body": {
                "running": "正在执行", "stopping": "正在停止", "stopped": "已停止",
                "completed": "本轮已结束", "failed": "执行失败",
            }[status],
            "com.agentteams.execution": {
                "run_id": run["event"].message_id, "status": status,
                "capabilities": ["stop", "steer"], "steering_mode": "native",
                **({"message_id": run["message_id"]} if run.get("message_id") else {}),
            },
        })

    async def _process_message_background(self, event, session_key):
        if os.getenv("AGENTTEAMS_CONTROLLER_URL") and os.getenv("AGENTTEAMS_PLUGIN_DIR"):
            from hermes_matrix.public_tasks import common, install, task_turn
            install()
            public_event = (getattr(event, "_task_protocol_meta", {}) or {}).get("teamharness_coordination")
            recovered = getattr(event, "_agentteams_recovery", None)
            if not public_event and recovered:
                task = await asyncio.to_thread(common("task_native").controller_json,
                    "tasks/" + recovered["task_id"] + "/meta")
                if task.get("task_plan_id"):
                    public_event = {"kind": "task_assigned", "task_id": task["task_id"],
                        "main_task_id": task["main_task_id"], "project_id": task.get("project_id", ""),
                        "delivery_id": task["notifyTxn"]}
            prepared, phase = {}, None
            try:
                if public_event:
                    public_event = dict(public_event)
                    if public_event["kind"] in {"handoff", "task_submitted", "main_task_ready"}:
                        phase = common("phase_recovery")
                        admission = await phase.notify(public_event["id"], event.message_id, "begin")
                        if admission.get("active") is not True:
                            return
                        if public_event["kind"] == "handoff":
                            public_event["pending_task_ids"] = admission["pending_task_ids"]
                    prepared = await asyncio.to_thread(common("task_native").invoke,
                        {"operation": "prepare_event", "event": public_event})
                    if prepared.get("ok") is False:
                        if phase and prepared.get("retryable"):
                            await phase.notify(public_event["id"], event.message_id, "retry")
                        raise RuntimeError("Public Task context is unavailable; no model turn was started")
                role = os.getenv("AGENTTEAMS_AGENT_ROLE", "worker")
                if role in {"manager", "leader", "team_leader"} and (public_event or {}).get("kind") != "assistant_digest":
                    arguments = {"operation": "roster", "role": role}
                    if role != "manager":
                        import yaml
                        runtime = yaml.safe_load(Path(os.environ["TEAMHARNESS_RUNTIME_CONFIG"]).read_text())
                        arguments["team"] = runtime["team"]["name"]
                    prepared["roster"] = await asyncio.to_thread(common("task_native").invoke, arguments)
                event._task_protocol_meta = {**(getattr(event, "_task_protocol_meta", {}) or {}),
                    "agent_matrix_user_id": str(self._user_id)}
                from hermes_matrix.progress import MatrixProgress, current_progress
                display = None if (public_event or {}).get('kind') == 'assistant_digest' else MatrixProgress(self._send_room_message, event)
                display_token = current_progress.set(display)
                failed = False
                try:
                    with task_turn(event, prepared, progress=display):
                        if public_event:
                            trace_event("coordination.native_begin", event_id=event.message_id,
                                        coordination_id=public_event.get("id", ""), phase=public_event["kind"])
                        return await self._process_native_message_background(event, session_key)
                except BaseException:
                    failed = True
                    raise
                finally:
                    try:
                        if display:
                            await display.finish(failed=failed)
                    except Exception:
                        logger.exception('Matrix native progress delivery failed')
                    finally:
                        current_progress.reset(display_token)
            except BaseException as error:
                if public_event and not isinstance(error, asyncio.CancelledError):
                    trace_event("coordination.native_error", event_id=event.message_id,
                                coordination_id=public_event.get("id", ""), phase=public_event["kind"],
                                error_type=type(error).__name__)
                    logger.warning("Task coordination failed before or during native dispatch", exc_info=True)
                if phase and phase.retryable_error(error):
                    await phase.notify(public_event["id"], event.message_id, "retry")
                raise
        return await self._process_native_message_background(event, session_key)

    async def _process_native_message_background(self, event, session_key):
        ordinary = getattr(event, "_agentteams_message", None)
        if not ordinary:
            return await self._process_managed_message(event, session_key)
        from hermes_matrix.message_execution import ordinary_turn
        scope = ordinary_turn.set(ordinary)
        try:
            return await self._process_managed_message(event, session_key)
        finally:
            ordinary_turn.reset(scope)
            self._pending_matrix.running.discard(ordinary.identity["event_key"])

    async def _process_managed_message(self, event, session_key):
        if "org.agentteams.coordination" in (event.raw_message or {}) or getattr(event, "_agentteams_recovery", None):
            kind = (getattr(event, "_task_protocol_meta", None) or {}).get("teamharness_coordination", {}).get("kind")
            token = _coordination_turn.set(kind or True)
            try:
                return await super()._process_message_background(event, session_key)
            finally:
                _coordination_turn.reset(token)
                if getattr(self, "_pending_matrix", None) is not None:
                    from worker_matrix_state import matrix_event_key
                    self._pending_matrix.running.discard(matrix_event_key(event.source.chat_id, event.message_id))
        if not await self._direct_member(event):
            return await super()._process_message_background(event, session_key)
        room = event.source.chat_id
        run = {"event": event, "room": room, "task": asyncio.current_task(),
               "session_key": session_key, "stopped": False,
               "inputs": getattr(event, "_agentteams_inputs", [event.text]),
               "input_ids": getattr(event, "_agentteams_input_ids", [event.message_id])}
        self._direct_runs[room] = run
        await self._execution_status(run, "running")
        status = "completed"
        try:
            return await super()._process_message_background(event, session_key)
        except BaseException:
            status = "failed"
            raise
        finally:
            if self._direct_runs.get(room) is run:
                self._direct_runs.pop(room)
            await self._execution_status(run, "stopped" if run["stopped"] else status)

    async def handle_message(self, event):
        if getattr(self, "_pending_matrix", None) is not None and not getattr(event, "_agentteams_message", None):
            from hermes_matrix.sync_state import current_admission
            from worker_matrix_state import matrix_event_key
            admission = current_admission()
            item = admission.item if admission is not None else None
            if item and item["intent"] == "message" and item["event_id"] == event.message_id:
                self._pending_matrix.live[matrix_event_key(item["room_id"], event.message_id)] = event
                return
        raw = event.raw_message
        control = raw.get("com.agentteams.control") if isinstance(raw, dict) else None
        if not isinstance(raw, dict) or "com.agentteams.control" not in raw:
            return await super().handle_message(event)
        if not isinstance(control, dict):
            return
        action = control.get("action")
        if action not in {"stop", "steer"} or control.get("agent_id") != self._user_id:
            return
        run = self._direct_runs.get(event.source.chat_id)
        if not run or run["event"].message_id != control.get("run_id"):
            if action == "steer":
                return await super().handle_message(event)
            return
        if event.source.user_id != run["event"].source.user_id or not await self._direct_member(event):
            return
        if self._direct_runs.get(event.source.chat_id) is not run or run["task"].done():
            if action == "steer":
                return await super().handle_message(event)
            return
        if action == "steer" and not getattr(event, "media_urls", None):
            if not event.text.strip():
                return
            # Use the native command path: it owns steering, source context and
            # the queued follow-up when completion wins the boundary race.
            from copy import copy
            command = copy(event)
            command.text = "/steer " + event.text
            return await super().handle_message(command)
        runner = getattr(self._message_handler, "__self__", None)
        # The native runner owns a worker thread. Cancelling its asyncio waiter
        # would leave shell/MCP work alive; request its hard interrupt instead.
        from agent.interrupt_compat import request_hard_interrupt
        while not run["task"].done():
            agent = runner._running_agents.get(run["session_key"]) if runner else None
            if agent is not None and request_hard_interrupt(agent):
                run["stopped"] = True
                await self._execution_status(run, "stopping")
                break
            await asyncio.sleep(0.05)
        await asyncio.shield(run["task"])
        if action == "steer":
            inputs = [*run["inputs"], event.text]
            event._agentteams_inputs = inputs
            event._agentteams_input_ids = [*run.get("input_ids", [run["event"].message_id]), event.message_id]
            event.text = "Continue the original request using the supplementary information below. Preserve earlier results and conversation context.\n\n" + "\n\n".join(
                f"[Message {identity}]\n{text}" for identity, text in zip(event._agentteams_input_ids, inputs))
            if self._session_receipts:
                await asyncio.to_thread(self._session_receipts.link, event.source.chat_id, event.message_id, event._agentteams_input_ids)
            return await super().handle_message(event)
    async def _send_dashboard_approval(self, room_id, text, approval):
        from mautrix.types import EventType, RoomID
        return await self._client.send_message_event(
            RoomID(room_id), EventType.ROOM_MESSAGE,
            {"msgtype": "m.text", "body": text, APPROVAL_KEY: approval},
        )

    async def _send_exec_approval_prompt(self, prompt):
        result = await self._dashboard_approvals.prompt(prompt)
        return result if result is not None else await super()._send_exec_approval_prompt(prompt)

    async def _connect_initial_sync(self, client):
        if not os.getenv("AGENTTEAMS_CONTROLLER_URL") or not os.getenv("AGENTTEAMS_PROCESS_ID"):
            return await super()._connect_initial_sync(client)
        from task_recovery import enabled
        if not enabled():
            return await super()._connect_initial_sync(client)
        from hermes_matrix.sync_state import NativeSync
        from hermes_matrix.message_execution import PendingInputs, install_ordinary_guard
        install_ordinary_guard()
        self._shared_sync = NativeSync(self, client)
        self._pending_matrix = PendingInputs(self, self._shared_sync.matrix)
        self._pending_matrix_ready = False
        # Each source event has its own execution and reply transaction.
        self._text_batch_delay_seconds = 0
        self._shared_sync.install()
        # The shared timeline cursor includes messages received while offline.
        # The SDK's five-second startup cutoff would discard those messages.
        self._startup_ts = 0
        await super()._connect_initial_sync(client)
        # The native initial-sync helper logs and suppresses errors. A failed
        # shared commit must not be advertised as successful startup.
        if self._shared_sync.error is not None:
            raise self._shared_sync.error
        if not self._shared_sync.initialized:
            raise RuntimeError("No committed Matrix response on initial sync")

    async def _absorb_sync(self, client, sync_data, *, initial=False):
        shared = getattr(self, "_shared_sync", None)
        if shared is None:
            return await super()._absorb_sync(client, sync_data, initial=initial)
        native = super()._absorb_sync
        cursor = await shared.absorb(sync_data, lambda data: native(client, data, initial=initial))
        if initial:
            # The gateway bounds transport connect to 30s. Its managed sync loop
            # owns startup recovery and history, and cancels them on disconnect.
            shared.poll_immediately = True
            return cursor
        pending = getattr(self, "_pending_matrix", None)
        startup = pending is not None and not self._pending_matrix_ready
        if startup:
            await self._recover_tasks()
            await pending.drain()
            from worker_handoff import recovery_scanned
            recovery_scanned()
            await self._report_connected()
            self._pending_matrix_ready = True
        if shared.history_pending:
            from hermes_matrix.sync_history import recover_history
            for room_id, failure in (await recover_history(shared)).items():
                logger.warning("Matrix history pending in %s: %s", room_id, failure)
        if pending is not None and (shared.history_pending or not startup):
            await pending.drain()
        return cursor

    async def connect(self, *, is_reconnect: bool = False) -> bool:
        trace_event("startup.stage", stage="matrix.connect")
        try:
            # Native connect can dispatch its initial sync synchronously.
            # Claim before connecting; waiting for a post-connect gate in a
            # message callback would deadlock and advance past that input.
            await self._recover_tasks(claim_only=True)
            ok = await super().connect(is_reconnect=is_reconnect)
        except Exception as exc:
            trace_event("startup.failed", stage="matrix.connect", error_type=type(exc).__name__, error=str(exc)[:4096])
            raise
        if ok and self._client is not None:
            self._wrap_send_message_event()
            if getattr(self, "_shared_sync", None) is None:
                await self._recover_tasks()
                await self._report_connected()
        else:
            trace_event("startup.failed", stage="matrix.connect", error="Native Matrix adapter did not connect")
        return ok

    async def _report_connected(self):
        trace_event("runtime.ready", evidence="native_matrix_connect")
        if os.getenv("AGENTTEAMS_CONTROLLER_URL"):
            reporter = await asyncio.create_subprocess_exec(
                "agt", "worker", "report-ready", "--name",
                os.getenv("AGENTTEAMS_WORKER_CR_NAME") or os.environ["AGENTTEAMS_WORKER_NAME"],
            )
            if await reporter.wait() != 0:
                logger.warning("Controller readiness report failed after Matrix connected")

    def _is_sender_authorized(self, user_id: str, *args, **kwargs):
        if os.getenv("AGENTTEAMS_AGENT_ROLE") == "manager":
            return user_id in DualAllowList.from_env().group_combined_allow
        return super()._is_sender_authorized(user_id, *args, **kwargs)

    def _wrap_send_message_event(self) -> None:
        """Inject MSC3952 ``m.mentions`` into every outgoing Matrix event."""
        if self._client is None:
            return
        if getattr(self._client, "_agentteams_mentions_wrapped", False):
            return

        original = self._client.send_message_event

        async def wrapped(
            client: Any,
            room_id: Any,
            event_type: Any,
            content: Any,
            *args: Any,
            **kwargs: Any,
        ) -> Any:
            if isinstance(content, dict):
                from hermes_matrix.progress import current_progress
                display = current_progress.get()
                if display:
                    content['org.agentteams.run_id'] = display.run_id
                    if isinstance(content.get('m.new_content'), dict):
                        content['m.new_content']['org.agentteams.run_id'] = display.run_id
                if _coordination_turn.get() == "assistant_digest" and "org.agentteams.approval" not in content:
                    return None
                apply_outbound_mentions(content, self_user_id=self._user_id)
                if _coordination_turn.get():
                    content["org.agentteams.progress_only"] = True
                    content["m.mentions"] = {"user_ids": []}
                    if isinstance(content.get("m.new_content"), dict):
                        content["m.new_content"]["org.agentteams.progress_only"] = True
                        content["m.new_content"]["m.mentions"] = {"user_ids": []}
                if self._session_receipts and str(event_type) == "m.room.message":
                    request = lambda *args: matrix_request(*credentials(self._session_receipts.workspace))(*args)
                    content = await asyncio.to_thread(capture_audience, request, str(room_id), content)
            try:
                result = await original(room_id, event_type, content, *args, **kwargs)
            except Exception as exc:
                trace_event("message.send_failed", room_id=str(room_id), error_type=type(exc).__name__, error=str(exc)[:4096])
                raise
            trace_event("message.sent", room_id=str(room_id), event_id=str(result), message_type=str(event_type))
            run = self._direct_runs.get(str(room_id))
            if run and str(event_type) == "m.room.message":
                serialized = content if isinstance(content, dict) else content.serialize()
                if serialized.get("msgtype") == "m.text":
                    relation = serialized.get("m.relates_to", {})
                    run["message_id"] = relation.get("event_id") if relation.get("rel_type") == "m.replace" else str(result)
            return result

        self._client.send_message_event = types.MethodType(wrapped, self._client)
        self._client._agentteams_mentions_wrapped = True  # type: ignore[attr-defined]
        logger.debug("Matrix: wrapped send_message_event for outbound mentions")

    async def _resolve_message_context(
        self,
        room_id: str,
        sender: str,
        event_id: str,
        body: str,
        source_content: dict,
        relates_to: dict,
        mention_claimed: bool = False,
        voice_gate: Any = None,
    ) -> Optional[tuple]:
        """Apply AgentTeams allow/history policy around native mention gating."""
        if os.getenv("AGENTTEAMS_AGENT_ROLE") == "manager":
            self._dual_allow = DualAllowList.from_env()
        is_dm = await self._is_dm_room(room_id)
        if not self._dual_allow.permits(sender, is_dm=is_dm):
            return None

        if await self._dashboard_approvals.answer(room_id, sender, body,
                content=source_content, agent_id=self._user_id,
                before_answer=lambda: self._accept_matrix_input(room_id, sender, event_id, source_content, "control")):
            return None

        ctx = await super()._resolve_message_context(
            room_id,
            sender,
            event_id,
            body,
            source_content,
            relates_to,
            mention_claimed=mention_claimed,
            voice_gate=voice_gate,
        )
        if ctx is None:
            if not is_dm:
                await self._accept_matrix_input(room_id, sender, event_id, source_content, "context")
                display = await self._get_display_name(room_id, sender)
                history_body = _describe_dropped_media(source_content, body)
                self._history.record(room_id, display, history_body, event_id)
            return None

        intent = "control" if (_is_commandish(body) or "com.agentteams.control" in source_content) else "message"
        await self._accept_matrix_input(room_id, sender, event_id, source_content, intent)

        trace_event("message.received", room_id=room_id, event_id=event_id, sender=sender)
        body, is_dm, chat_type, thread_id, display_name, source = ctx
        if is_dm:
            return body, is_dm, chat_type, thread_id, display_name, source

        if _is_commandish(body):
            # Match copaw: slash commands do not receive a history prefix and
            # should clear any accumulated unmentioned context.
            self._history.clear(room_id)
            return body, is_dm, chat_type, thread_id, display_name, source

        body = f"{display_name}: {body}"
        history_ids = self._history.event_ids(room_id)
        history_prefix = self._history.drain(room_id)
        if history_prefix:
            body = history_prefix + body
            if self._session_receipts:
                await asyncio.to_thread(self._session_receipts.link, room_id, event_id, history_ids)
        return body, is_dm, chat_type, thread_id, display_name, source

    def _background_read_receipt(self, room_id, event_id):
        from task_recovery import enabled
        if not enabled():
            return super()._background_read_receipt(room_id, event_id)
        # Native mention gating precedes input admission and persistence.
        # Managed reads come from actual persisted SDK rows, never this gate.

    async def _handle_media_message(
        self,
        room_id: str,
        sender: str,
        event_id: str,
        event_ts: float,
        source_content: Dict[str, Any],
        relates_to: Dict[str, Any],
        msgtype: str,
        mention_claimed: bool = False,
    ) -> None:
        """Downgrade images to text when the active model lacks vision."""
        if msgtype == "m.image" and not self._vision_enabled:
            body = source_content.get("body", "") or ""
            display_body = body or "image"
            placeholder = (
                "[User sent an image (current model does not support image "
                f"input): {display_body}]"
            )
            text_content = dict(source_content)
            text_content["msgtype"] = "m.text"
            text_content["body"] = placeholder

            # Resolve the context exactly once.  The resolved tuple already
            # contains the drained group-history prefix and already emitted the
            # read receipt/thread mark.  Passing it into _build_inbound_event
            # avoids the old downgrade path resolving (and consuming history)
            # a second time through _handle_text_message.
            ctx = await self._resolve_message_context(
                room_id,
                sender,
                event_id,
                placeholder,
                source_content,
                relates_to,
                mention_claimed=mention_claimed,
            )
            if ctx is None:
                return
            msg_event = await self._build_inbound_event(
                room_id,
                sender,
                event_id,
                placeholder,
                text_content,
                relates_to,
                ctx=ctx,
            )
            if msg_event is None:
                return
            if (
                msg_event.message_type == MessageType.TEXT
                and self._text_batch_delay_seconds > 0
            ):
                self._enqueue_text_event(msg_event)
            else:
                await self.handle_message(msg_event)
            return

        if msgtype == "m.image":
            normalized_content = dict(source_content)
            normalized_body = _normalize_image_body(
                source_content.get("body", "") or ""
            )
            if normalized_body != (source_content.get("body", "") or ""):
                logger.debug(
                    "Matrix: stripping transport filename from inbound image body %r",
                    source_content.get("body", ""),
                )
                normalized_content["body"] = normalized_body
            await super()._handle_media_message(
                room_id,
                sender,
                event_id,
                event_ts,
                normalized_content,
                relates_to,
                msgtype,
                mention_claimed=mention_claimed,
            )
            return

        await super()._handle_media_message(
            room_id,
            sender,
            event_id,
            event_ts,
            source_content,
            relates_to,
            msgtype,
            mention_claimed=mention_claimed,
        )
