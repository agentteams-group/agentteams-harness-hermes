"""Installed SDK session/admission contracts; Controller requests are mocked.

This is not model/tool continuation or real Matrix delivery acceptance.
"""
import asyncio
import hashlib
import os
from pathlib import Path
import tempfile
import time
from types import SimpleNamespace, MethodType
from unittest.mock import AsyncMock, Mock, patch
from urllib.parse import parse_qs, urlsplit

from gateway.config import PlatformConfig, GatewayConfig
from gateway.run import GatewayRunner
from gateway.session import SessionStore
from hermes_matrix.overlay_adapter import MatrixAdapter
from hermes_matrix.session_cache import session_fingerprint
from task_recovery import Recovery, RecoveryConflict


async def check(workspace):
    os.environ.update(AGENTTEAMS_WORKER_HOME=str(workspace), AGENTTEAMS_PROCESS_ID="actual-start",
                      AGENTTEAMS_CONTROLLER_URL="http://task-client-fixture.invalid",
                      AGENTTEAMS_AGENT_ROLE="worker", AGENTTEAMS_OTEL_RUNTIME="hermes", AGENTTEAMS_WORKER_NAME="alice")
    task = {"task_id": "stable-task", "status": "in_progress", "room_id": "!task:test", "execution": {
        "generation": 2, "instance_id": "new-start", "process_id": "actual-start", "runtime": "hermes", "state": "active",
        "checkpoint": {"version": 3, "completed_steps": ["one"], "remaining_steps": ["two", "three"]}}}
    recovery = Mock(process="actual-start")
    recovery.digest = Recovery.digest
    recovery.discover.return_value = {"instance_id": "new-start", "runtime": "hermes", "tasks": [{"team_id": "team", "task": task, "etag": "task-etag"}]}
    recovery.claim.return_value = {"task": task, "execution": task["execution"], "spec": "Do one, two, three", "saved_result": "Saved authoritative answer"}
    recovery.current.return_value = task
    recovery.resume_text.return_value = "Resume step two from checkpoint three"
    caches = {}
    config_generation = 1
    root = {"active_start": {"instance_id": "new-start", "process_id": "actual-start", "runtime": "hermes"}}
    order = []

    def request(method, path, body=None, etag=None, **kwargs):
        if path == "runtime/matrix":
            order.append("register-start" if method == "POST" else "read-start")
            return {"state": root, "etag": "root-etag", "instance_id": "new-start", "runtime": "hermes"}
        assert path.startswith("runtime/sessions?")
        query = parse_qs(urlsplit(path).query)
        key = (query["team"][0], query["task"][0])
        cp = task["execution"]["checkpoint"]
        cache = caches.get(key)
        view = {"work": {"kind": "task", "team_id": key[0], "task_id": key[1]}, "instance_id": "new-start", "runtime": "hermes",
                "generation": task["execution"]["generation"], "checkpoint_version": cp["version"],
                "checkpoint_digest": Recovery.digest(cp), "config_generation": config_generation,
                "cache": cache, "etag": "cache-etag" if cache else "", "compatible": False}
        if method == "PUT":
            assert body["generation"] == view["generation"]
            assert body["checkpoint_digest"] == view["checkpoint_digest"]
            assert body["config_generation"] == view["config_generation"]
            assert bool(etag) == bool(cache)
            caches[key] = {**body, "runtime": "hermes"}
            order.append("native-descriptor")
            return {**view, "cache": caches[key], "etag": "cache-etag", "compatible": True}
        assert method == "GET"
        if cache:
            view["compatible"] = cache["runtime"] == "hermes" and cache["expires_at"] > time.time() and all(
                cache[k] == view[k] for k in ("checkpoint_version", "checkpoint_digest", "config_generation"))
        return view

    recovery.request.side_effect = request
    adapter = MatrixAdapter(PlatformConfig(enabled=True))
    adapter._user_id = "@alice:test"
    adapter._client = SimpleNamespace(mxid="@worker:test", whoami=AsyncMock(return_value=SimpleNamespace(user_id="@worker:test",device_id="A")), send_message_event=AsyncMock(return_value="$sent"))
    observed = []
    runner = GatewayRunner.__new__(GatewayRunner)
    runner.config = SimpleNamespace(multiplex_profiles=False)
    runner.session_store = SessionStore(workspace / "sessions", GatewayConfig())
    runner._is_session_running = Mock(return_value=False)
    runner._peek_session_state = Mock(return_value=None)
    runner._running_agent_ids = Mock(return_value=set())
    runner._spawn_release_thread = Mock()
    source = adapter.build_source(chat_id=task["room_id"], chat_type="group", user_id=adapter._user_id)
    key = runner._session_key_for_source(source)
    runner._agent_cache = {key: (SimpleNamespace(session_id="old-native-conversation"), None, 0)}

    async def admit(self, event):
        observed.append(event)
        event._gateway_accepted = True
    adapter._message_handler = MethodType(admit, runner)
    adapter.handle_message = adapter._message_handler
    with patch("task_recovery.Recovery", return_value=recovery):
        await asyncio.gather(adapter._recover_tasks(), adapter._recover_tasks())
        assert order[:2] == ["read-start", "register-start"]
        assert len(observed) == 1 and key not in runner._agent_cache
        event = observed[0]
        assert event.text == "Resume step two from checkpoint three"
        assert event.internal is True and event.allow_gateway_control is False
        assert event._agentteams_recovery["instance_id"] == "new-start"
        runner._is_user_authorized_for_source = Mock(return_value=False)
        runner._scale_to_zero_note_real_inbound = Mock()
        runner._hm_pre_gateway_dispatch_hook = AsyncMock(return_value=event)
        assert await runner._hm_admit_event(event) == (event, event.source, True)
        runner._is_user_authorized_for_source.assert_not_called()
        event.internal = False
        assert await runner._hm_admit_event(event) is None
        runner._is_user_authorized_for_source.assert_called_once()
        event.internal = True
        assert event.source.thread_id is None
        key = adapter._event_session_key(event)
        first_session = runner.session_store.lookup_by_session_key(key).session_id
        adapter._client.send_message_event.assert_not_called()
        await adapter._recover_tasks()
        assert len(observed) == 1
        cp = {"version": 4, "delivery": "pending", "reply_txn": "stable-reply",
              "reply_attempt":{"state":"sending","user_id":"@worker:test","device_id":"A"},
              "result":{"size":len(b"Saved authoritative answer"),"sha256":hashlib.sha256(b"Saved authoritative answer").hexdigest()}}
        task["execution"]["checkpoint"] = cp
        await adapter._recover_tasks()
        assert len(observed) == 1
        assert adapter._client.send_message_event.call_args.kwargs["txn_id"] == "stable-reply"
        recovery.delivered.assert_called_once_with("stable-task", "team", "$sent")
        adapter._client.send_message_event.reset_mock()
        # Emulate the native SDK writes that precede its final-send hook.
        db = runner.session_store._db_for_key(key)
        db.append_message(first_session, "user", "Persisted native Task input")
        db.append_message(first_session, "assistant", "Native final answer")
        recovery.prepare_reply.return_value = ({"execution": {"checkpoint": cp}}, "Saved authoritative answer")
        def delivered(*args):
            cp.update(delivery="delivered", reply_event="$sent")
            return {"task": {"status": "in_progress"}, "execution": {"checkpoint": cp}}
        recovery.delivered.side_effect = delivered
        recorded = Mock()
        await adapter._send_final_text(event, key, "Final answer", {}, False, 0, recorded)
        assert adapter._client.send_message_event.call_args.args[2]["body"] == "Saved authoritative answer"
        assert adapter._client.send_message_event.call_args.kwargs["txn_id"] == "stable-reply"
        assert recorded.call_args.args[0].success is True
        descriptor = caches[("team", "stable-task")]
        assert descriptor["native_ref"] == first_session
        assert descriptor["native_digest"] == session_fingerprint(runner.session_store, runner.session_store.lookup_by_session_key(key))
        assert runner.session_store.get_session_metadata(key, "agentteams_recovery") is None
        # Unknown side effects never enter a normal native model turn.
        task["execution"]["state"] = "needs_verification"
        task["execution"]["generation"] += 1
        await adapter._recover_tasks()
        assert len(observed) == 1
        task["execution"]["state"] = "active"
        for case in ("valid", "expired", "stale", "foreign", "missing", "empty", "changed-native", "changed-config"):
            entry = runner.session_store.lookup_by_session_key(key)
            old_id = entry.session_id
            db = runner.session_store._db_for_key(key)
            if case != "empty":
                db.append_message(old_id, "user", "Latest native Task history")
            else:
                runner.session_store = SessionStore(workspace / "empty-case", GatewayConfig())
                entry = runner.session_store.get_or_create_session(event.source, force_new=True)
                key, old_id = entry.session_key, entry.session_id
                db = runner.session_store._db_for_key(key)
            caches[("team", "stable-task")] = {"runtime": "hermes", "native_ref": old_id,
                "native_digest": session_fingerprint(runner.session_store, entry) or "0"*64,
                "expires_at": time.time()+3600, "checkpoint_version": cp["version"],
                "checkpoint_digest": Recovery.digest(cp), "config_generation": config_generation}
            cache = caches[("team", "stable-task")]
            if case == "expired": cache["expires_at"] = time.time()-1
            elif case == "stale": cache["checkpoint_digest"] = "older-checkpoint"
            elif case == "foreign": cache["runtime"] = "codex"
            elif case == "missing": runner.session_store.remove_by_session_id(old_id)
            elif case == "changed-native": db.append_message(old_id, "assistant", "new native bytes after snapshot")
            elif case == "changed-config": config_generation += 1
            store_path = workspace / ("empty-case" if case == "empty" else "sessions")
            runner.session_store = SessionStore(store_path, GatewayConfig())
            task["execution"]["generation"] += 1
            await adapter._recover_tasks()
            event = observed[-1]
            key = adapter._event_session_key(event)
            assert (event._agentteams_recovery["session_id"] == old_id) == (case == "valid"), case
        task["status"] = "submitted"
        before = len(observed)
        task["execution"]["generation"] += 1
        await adapter._recover_tasks()
        assert len(observed) == before
        task["status"] = "in_progress"
        task["execution"]["generation"] += 1
        await adapter._recover_tasks("stable-task", source_event=SimpleNamespace(event_id="$assignment", sender="@leader:test", room_id=task["room_id"]))
        event = observed[-1]
        assert event.message_id == "$assignment" and event.source.user_id == "@leader:test"
        key = adapter._event_session_key(event)
        entry = runner.session_store.lookup_by_session_key(key)
        db = runner.session_store._db_for_key(key)
        db.append_message(entry.session_id, "user", "Persisted verified assignment")
        caches[("team", "stable-task")] = {"runtime": "hermes", "native_ref": entry.session_id,
            "native_digest": session_fingerprint(runner.session_store, entry), "expires_at": time.time()+3600,
            "checkpoint_version": cp["version"], "checkpoint_digest": Recovery.digest(cp), "config_generation": config_generation}
        runner.session_store = SessionStore(workspace / "sessions", GatewayConfig())
        task["execution"]["generation"] += 1
        await adapter._recover_tasks()
        event = observed[-1]
        assert event.source.user_id == "@leader:test" and event._agentteams_recovery["session_id"] == entry.session_id
        with patch.object(adapter, "_remember_task_session", new=AsyncMock()):
            with patch.object(recovery, "current", return_value=None):
                try:
                    await adapter._send_final_text(event, key, "late answer", {}, False, 0, Mock())
                    raise AssertionError("cancelled callback was admitted")
                except RecoveryConflict:
                    pass
    print("Hermes installed SDK/common descriptor, startup/admission, native content and saved-reply contracts passed (mocked Controller; no model E2E)")


if __name__ == "__main__":
    with tempfile.TemporaryDirectory() as directory:
        asyncio.run(check(Path(directory)))
