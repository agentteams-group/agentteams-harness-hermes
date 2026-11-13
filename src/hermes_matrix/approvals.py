"""Dashboard transport for Hermes' request-bound native approval queue."""
from __future__ import annotations

import asyncio
import hashlib
import json
import os
import re
import time

KEY = "org.agentteams.approval"
COMMAND = re.compile(r"^/agentteams-approval (approve|deny) ([\w.-]+) ([\w.-]+)$")


class MatrixApprovals:
    def __init__(self, send):
        self.send = send
        self.pending = {}

    async def prompt(self, prompt):
        from gateway.platforms.base import SendResult
        from tools import approval as native

        if not prompt.request_id or "once" not in prompt.choices or "deny" not in prompt.choices:
            return None  # Leave unsupported native prompt types to Hermes.
        context = hashlib.sha256(f"{prompt.chat_id}:{prompt.session_key}".encode()).hexdigest()
        key = (prompt.chat_id, prompt.request_id, context)
        # Hermes v0.23's settle observer reports a reason but omits the
        # committed choice. Retain the exact queue entry so decisions made on
        # another native surface produce the same truthful receipt.
        with native._lock:
            queued = next((item for item in native._gateway_queues.get(prompt.session_key, [])
                           if item.data.get("request_id") == prompt.request_id), None)
        if queued is None:
            return SendResult(success=False, error="Approval is no longer pending")
        data = {
            "version": 1, "runtime": "hermes", "request_id": prompt.request_id,
            "context_id": context, "status": "pending", "tool_name": "terminal",
            "parameters": json.dumps({"command": prompt.command}, ensure_ascii=False),
            "risk_details": prompt.description,
        }
        entry = {"prompt": prompt, "data": data, "decision": None}
        self.pending[key] = entry
        loop = asyncio.get_running_loop()

        def settled(reason):
            # Called by the native waiter thread, after it commits the outcome.
            status = ("approved" if queued.result in {"once", "session", "always"} else "rejected") if reason == "resolved" else (
                "expired" if reason == "timeout" else "cancelled")
            asyncio.run_coroutine_threadsafe(self.finish(key, status), loop)

        if not native.observe_gateway_settle(prompt.session_key, prompt.request_id, settled):
            self.pending.pop(key, None)
            return SendResult(success=False, error="Approval is no longer pending")
        if isinstance(queued.data.get("timeout_s"), (int, float)):
            data["expires_at"] = int((time.time() + queued.data["timeout_s"]) * 1000)
        event_id = await self.send(prompt.chat_id, prompt.text, data)
        return SendResult(success=True, message_id=str(event_id))

    async def answer(self, room_id, sender, body):
        match = COMMAND.fullmatch(re.sub(r"^(?:@\S+\s+)+", "", body.strip()))
        if not match:
            return False
        action, request_id, context = match.groups()
        key = (room_id, request_id, context)
        entry = self.pending.get(key)
        if entry is None:
            return True  # Stale controls must never become model instructions.
        prompt = entry["prompt"]
        requester = (prompt.metadata or {}).get("requester_user_id")
        admin_name = os.getenv("AGENTTEAMS_ADMIN_USER", "")
        domain = os.getenv("AGENTTEAMS_MATRIX_DOMAIN", "")
        admin = admin_name if admin_name.startswith("@") else f"@{admin_name}:{domain}" if admin_name and domain else None
        if requester and sender not in {requester, admin}:
            return True
        from tools.approval import resolve_gateway_approval_card
        entry["decision"] = "approved" if action == "approve" else "rejected"
        if not resolve_gateway_approval_card(
                prompt.session_key, "once" if action == "approve" else "deny", request_id):
            entry["decision"] = None
        return True

    async def finish(self, key, status):
        entry = self.pending.pop(key, None)
        if entry:
            data = {**entry["data"], "status": status}
            await self.send(key[0], f"Approval {status}", data)
