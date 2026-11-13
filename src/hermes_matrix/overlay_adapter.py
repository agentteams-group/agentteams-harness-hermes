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
import os
import types
from typing import Any, Dict, Optional

from gateway.config import PlatformConfig
from gateway.platforms.event import MessageType
from plugins.platforms.matrix.adapter import MatrixAdapter as _NativeMatrixAdapter

from hermes_matrix.policies import (
    DualAllowList,
    HistoryBuffer,
    apply_outbound_mentions,
)

logger = logging.getLogger(__name__)
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

    def __init__(self, config: PlatformConfig):
        super().__init__(config)
        self._dual_allow = DualAllowList.from_env()
        self._history = HistoryBuffer.from_env()
        self._vision_enabled = _truthy_env("MATRIX_VISION_ENABLED", default=False)
        self._direct_runs: dict[str, dict] = {}

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
        await self._client.send_message_event(run["room"], "m.room.message", {
            "msgtype": "m.notice", "body": {
                "running": "正在执行", "stopping": "正在停止", "stopped": "已停止",
                "completed": "本轮已结束", "failed": "执行失败",
            }[status],
            "com.agentteams.execution": {
                "run_id": run["event"].message_id, "status": status,
                "capabilities": ["stop", "steer"],
                **({"message_id": run["message_id"]} if run.get("message_id") else {}),
            },
        })

    async def _process_message_background(self, event, session_key):
        if not await self._direct_member(event):
            return await super()._process_message_background(event, session_key)
        room = event.source.chat_id
        run = {"event": event, "room": room, "task": asyncio.current_task(),
               "session_key": session_key, "stopped": False,
               "inputs": getattr(event, "_agentteams_inputs", [event.text])}
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
            event.text = "Continue the original request using the supplementary information below. Preserve earlier results and conversation context.\n\n" + "\n\n".join(inputs)
            return await super().handle_message(event)

    async def connect(self, *, is_reconnect: bool = False) -> bool:
        ok = await super().connect(is_reconnect=is_reconnect)
        if ok and self._client is not None:
            self._wrap_send_message_event()
            if os.getenv("AGENTTEAMS_CONTROLLER_URL"):
                reporter = await asyncio.create_subprocess_exec(
                    "agt", "worker", "report-ready", "--name",
                    os.getenv("AGENTTEAMS_WORKER_CR_NAME") or os.environ["AGENTTEAMS_WORKER_NAME"],
                )
                if await reporter.wait() != 0:
                    logger.warning("Controller readiness report failed after Matrix connected")
        return ok

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
                apply_outbound_mentions(content, self_user_id=self._user_id)
            result = await original(room_id, event_type, content, *args, **kwargs)
            run = self._direct_runs.get(str(room_id))
            if run and str(event_type) == "m.room.message":
                serialized = content if isinstance(content, dict) else content.serialize()
                if serialized.get("msgtype") == "m.text":
                    run["message_id"] = str(result)
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
                display = await self._get_display_name(room_id, sender)
                history_body = _describe_dropped_media(source_content, body)
                self._history.record(room_id, display, history_body)
            return None

        body, is_dm, chat_type, thread_id, display_name, source = ctx
        if is_dm:
            return body, is_dm, chat_type, thread_id, display_name, source

        if _is_commandish(body):
            # Match copaw: slash commands do not receive a history prefix and
            # should clear any accumulated unmentioned context.
            self._history.clear(room_id)
            return body, is_dm, chat_type, thread_id, display_name, source

        body = f"{display_name}: {body}"
        history_prefix = self._history.drain(room_id)
        if history_prefix:
            body = history_prefix + body
        return body, is_dm, chat_type, thread_id, display_name, source

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
