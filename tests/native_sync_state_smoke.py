"""Real SDK sync dispatch/store; Controller and Matrix HTTP are fixtures.

No real-model, history backfill or task-continuation acceptance is claimed.
"""
import asyncio
import copy
import os
import tempfile
import time
from types import SimpleNamespace
import unittest
from unittest.mock import AsyncMock, patch

from gateway.config import PlatformConfig
from mautrix.client import Client
from mautrix.client.state_store import MemoryStateStore, MemorySyncStore
from mautrix.types import MessageEvent, UserID
from hermes_matrix.overlay_adapter import MatrixAdapter
from hermes_matrix.policies import DualAllowList
from hermes_matrix.sync_state import NativeSync
from task_recovery import RecoveryConflict, RecoveryUnavailable
from worker_matrix_state import WorkerMatrix, matrix_event_key


class MatrixFixture:
    consumer = WorkerMatrix.consumer
    sync = WorkerMatrix.sync

    def __init__(self):
        self.runtime = "hermes"
        self.recovery = SimpleNamespace(process="actual-start")
        self.snapshot = {"runtime": "hermes", "instance_id": "container-start", "etag": "version",
            "state": {"active_start": {"runtime": "hermes", "instance_id": "container-start",
                "process_id": "actual-start"}, "consumers": {}, "rooms": {}, "pending": []}}
        self.inputs, self.order = {}, []
        self.failed_inputs = set()
        self.fail_cursor = False

    def start(self):
        return copy.deepcopy(self.snapshot)

    load = start

    def input(self, key):
        return self.inputs.get(key)

    def prepare_input(self, room, event_id, source, intent, **kwargs):
        if event_id in self.failed_inputs:
            raise RecoveryUnavailable("fixture input storage unavailable")
        self.order.append("prepare:" + event_id)
        return {"room_id": room, "event_id": event_id, "input": source, "intent": intent}

    def ingest(self, snapshot, consumer, inputs, rooms=None):
        if self.fail_cursor:
            raise RecoveryConflict("fixture conditional commit failed")
        self.order.append("cursor:" + consumer["token"])
        for item in inputs:
            key = matrix_event_key(item["room_id"], item["event_id"])
            self.inputs[key] = item
            if key not in self.snapshot["state"]["pending"]:
                self.snapshot["state"]["pending"].append(key)
        self.snapshot["state"]["consumers"]["native"] = consumer
        self.snapshot["state"]["rooms"].update(rooms or {})
        self.last_inputs = inputs
        return self.start()


def event(identifier, *, mention=True, sender="@human:test"):
    return {"type": "m.room.message", "event_id": identifier, "sender": sender,
        "origin_server_ts": int(time.time() * 1000), "content": {
            "msgtype": "m.text", "body": "Continue work " + identifier,
            "m.mentions": {"user_ids": ["@worker:test"] if mention else []}}}


def response(*events, next_batch="next", limited=False):
    return {"next_batch": next_batch, "rooms": {"join": {"!room:test": {
        "timeline": {"events": list(events), "limited": limited, "prev_batch": "history"}}}}}


class NativeSyncTests(unittest.IsolatedAsyncioTestCase):
    async def asyncSetUp(self):
        self.directory = tempfile.TemporaryDirectory()
        self.addCleanup(self.directory.cleanup)
        env = patch.dict(os.environ, AGENTTEAMS_WORKER_HOME=self.directory.name,
            AGENTTEAMS_PROCESS_ID="actual-start", AGENTTEAMS_CONTROLLER_URL="http://fixture.invalid",
            AGENTTEAMS_AGENT_ROLE="worker")
        env.start()
        self.addCleanup(env.stop)
        room_authority = patch("task_coordination.controller_json", return_value={})
        self.room_authority = room_authority.start()
        self.addCleanup(room_authority.stop)
        self.matrix = MatrixFixture()
        for module in ["worker_matrix_state.WorkerMatrix", "hermes_matrix.sync_state.WorkerMatrix"]:
            mocked = patch(module, return_value=self.matrix)
            mocked.start()
            self.addCleanup(mocked.stop)
        self.adapter = MatrixAdapter(PlatformConfig(enabled=True))
        self.adapter._user_id = "@worker:test"
        self.adapter._dual_allow = DualAllowList(group_policy="open")
        self.adapter._is_allowed_matrix_room_event = AsyncMock(return_value=True)
        self.adapter._is_dm_room = AsyncMock(return_value=False)
        self.adapter._resolve_room_identity = AsyncMock(return_value=SimpleNamespace(
            display_name="Room", room_topic="Topic", server_name="test"))
        self.adapter._get_display_name = AsyncMock(return_value="Human")
        self.adapter._require_mention = True
        self.adapter._auto_thread = False
        self.adapter._matrix_session_scope = "room"
        self.adapter._text_batch_delay_seconds = 0
        self.adapter._startup_ts = time.time() - 10
        self.adapter._refresh_dm_cache = AsyncMock()
        self.admitted = []
        self.adapter.handle_message = AsyncMock(side_effect=lambda e: self.admitted.append(e.message_id))
        self.client = Client(mxid=UserID("@worker:test"), device_id="device", base_url="http://fixture.invalid",
            state_store=MemoryStateStore(), sync_store=MemorySyncStore())
        self.addAsyncCleanup(self.client.api.session.close)
        self.native_http = AsyncMock()
        self.client.sync = self.native_http
        from mautrix.types import EventType
        self.client.add_event_handler(EventType.ROOM_MESSAGE, self.adapter._on_room_message, wait_sync=True)
        self.adapter._client = self.client
        self.shared = NativeSync(self.adapter, self.client)
        self.adapter._shared_sync = self.shared
        self.shared.install()

    async def absorb(self, data):
        self.native_http.return_value = data
        polled = await self.client.sync(since="stale-native-cache", timeout=30000)
        return await self.adapter._absorb_sync(self.client, polled)

    async def test_verified_task_control_runs_while_ordinary_chat_waits_for_history(self):
        raw = event("$task-wake")
        raw["type"] = "org.agentteams.coordination"
        raw["content"]["org.agentteams.coordination"] = {}
        ordinary = event("$ordinary")
        packet = {"meta": {"teamharness_coordination": {"kind": "handoff"}}, "text": "task context"}
        def authority(content, *args, **kwargs):
            return packet if "org.agentteams.coordination" in content else None
        with patch("task_coordination.notification", side_effect=authority):
            self.native_http.return_value = response(raw, ordinary, limited=True)
            polled = await self.client.sync(timeout=30000)
            await self.adapter._absorb_sync(self.client, polled, initial=True)
        assert self.admitted == ["$task-wake"]
        assert self.matrix.snapshot["state"]["rooms"]["!room:test"]["gaps"] == ["history"]
        assert len(self.matrix.snapshot["state"]["pending"]) == 2

    async def test_task_room_supplement_does_not_wait_for_chat_history(self):
        self.room_authority.return_value = {"taskId": "main"}
        self.native_http.return_value = response(event("$task-supplement"), limited=True)
        polled = await self.client.sync(timeout=30000)
        await self.adapter._absorb_sync(self.client, polled, initial=True)
        assert self.admitted == ["$task-supplement"]
        self.room_authority.assert_called_once_with("tasks/room-context?roomId=%21room%3Atest")
        assert self.matrix.snapshot["state"]["rooms"]["!room:test"]["gaps"] == ["history"]

    async def test_ready_task_discovery_continues_without_an_at_notification(self):
        self.adapter._pending_matrix_ready = True
        self.adapter._pending_matrix = SimpleNamespace(drain=AsyncMock())
        self.adapter._recover_tasks = AsyncMock()
        with patch("hermes_matrix.public_tasks.retain_confirmed_handoffs"):
            await self.absorb(response(next_batch="scan-one"))
            await self.absorb(response(next_batch="scan-two"))
        assert self.admitted == []
        assert self.adapter._recover_tasks.await_count == 2

    async def test_private_control_uses_original_id_and_never_becomes_chat(self):
        raw = event("$wake")
        raw["type"] = "org.agentteams.coordination"
        raw["content"]["org.agentteams.coordination"] = {}
        forged = event("$unbound")
        forged["type"] = raw["type"]
        packet = {"meta": {"teamharness_coordination": {"kind": "handoff"}}, "text": "plan"}
        with patch("task_coordination.notification", return_value=packet) as validate:
            await self.absorb(response(raw, forged))
        assert validate.call_args.kwargs == {"event_id": "$wake"}
        assert self.admitted == ["$wake"]
        assert len(self.matrix.last_inputs) == 1
        assert self.matrix.last_inputs[0]["input"]["type"] == raw["type"]
        assert self.matrix.last_inputs[0]["input"]["origin_server_ts"] == raw["origin_server_ts"]

    async def test_real_dispatch_retains_server_order_before_native_cursor(self):
        put = self.client.sync_store.put_next_batch
        async def native_put(token):
            assert self.matrix.order[-1] == "cursor:" + token
            self.matrix.order.append("native:" + token)
            await put(token)
        self.client.sync_store.put_next_batch = native_put
        assert await self.absorb(response(event("$z"), event("$a", mention=False))) == "next"
        assert [i["event_id"] for i in self.matrix.last_inputs] == ["$z", "$a"]
        assert [i["intent"] for i in self.matrix.last_inputs] == ["message", "context"]
        assert self.matrix.order[-2:] == ["cursor:next", "native:next"]
        assert await self.client.sync_store.get_next_batch() == "next"
        assert self.admitted == ["$z"]

    async def test_caught_native_handler_failure_cannot_advance_cursor(self):
        self.matrix.failed_inputs.add("$failed")
        with self.assertRaises(RecoveryUnavailable):
            await self.absorb(response(event("$ok"), event("$failed")))
        assert not self.matrix.snapshot["state"]["consumers"]
        assert not await self.client.sync_store.get_next_batch()
        assert self.admitted == []
        # Both siblings are admitted exactly once after a successful batch commit.
        self.matrix.failed_inputs.clear()
        await self.absorb(response(event("$ok"), event("$failed")))
        assert self.admitted == ["$ok", "$failed"]
        assert [i["event_id"] for i in self.matrix.last_inputs] == ["$ok", "$failed"]

    async def test_failed_shared_cas_preserves_native_cursor_and_retry_inputs(self):
        self.matrix.fail_cursor = True
        with self.assertRaises(RecoveryConflict):
            await self.absorb(response(event("$once")))
        assert not await self.client.sync_store.get_next_batch()
        assert self.admitted == []
        self.matrix.fail_cursor = False
        await self.absorb(response(event("$once")))
        assert self.admitted == ["$once"]
        assert [i["event_id"] for i in self.matrix.last_inputs] == ["$once"]

    async def test_only_current_native_account_device_format_restores_cursor(self):
        identity = {"runtime": "hermes", "account": "@worker:test", "device": "device",
            "format": "mautrix/sync-v3"}
        self.matrix.snapshot["state"]["consumers"] = {
            "foreign": {**identity, "runtime": "qwenpaw", "token": "foreign"},
            "old-device": {**identity, "device": "old-device", "token": "old-device"},
            "native": {**identity, "token": "authoritative"}}
        await self.absorb(response(event("$restored")))
        assert self.native_http.call_args.kwargs["since"] == "authoritative"

    async def test_missing_same_native_cursor_never_imports_foreign_cursor(self):
        self.matrix.snapshot["state"]["consumers"] = {"foreign": {
            "runtime": "qwenpaw", "account": "@worker:test", "device": "device",
            "format": "mautrix/sync-v3", "token": "foreign"}}
        await self.absorb(response(event("$fresh")))
        assert self.native_http.call_args.kwargs["since"] is None

    async def test_limited_timeline_retains_explicit_gap(self):
        self.matrix.snapshot["state"]["rooms"]["!room:test"] = {"through": "$old"}
        await self.absorb(response(event("$new"), limited=True))
        assert self.matrix.snapshot["state"]["rooms"]["!room:test"]["gaps"] == ["history"]

    async def test_initial_native_error_suppression_does_not_report_success(self):
        # Native connect's initial helper catches this error. The overlay must
        # still propagate it rather than report readiness and lose the batch.
        self.native_http.side_effect = RecoveryUnavailable("fixture shared state unavailable")
        with self.assertRaises(RecoveryUnavailable):
            await self.adapter._connect_initial_sync(self.client)
        assert not await self.client.sync_store.get_next_batch()

    async def test_unmatched_response_cannot_commit_a_guessed_cursor(self):
        with self.assertRaises(RecoveryConflict):
            await self.adapter._absorb_sync(self.client, response(event("$unpolled")))
        self.adapter.handle_message.assert_not_awaited()
        assert not await self.client.sync_store.get_next_batch()

    async def test_decrypted_background_callback_finishes_before_cursor(self):
        entered, release = asyncio.Event(), asyncio.Event()
        plain = event("$encrypted")
        async def decrypt(source):
            entered.set()
            await release.wait()
            return MessageEvent.deserialize({**plain, "room_id": str(source.room_id)})
        self.client.crypto = SimpleNamespace(decrypt_megolm_event=decrypt)
        encrypted = {**plain, "type": "m.room.encrypted", "content": {
            "algorithm": "m.megolm.v1.aes-sha2", "ciphertext": "fixture",
            "sender_key": "fixture-key", "device_id": "fixture-device", "session_id": "fixture-session"}}
        running = asyncio.create_task(self.absorb(response(encrypted)))
        await asyncio.wait_for(entered.wait(), timeout=1)
        assert not self.matrix.snapshot["state"]["consumers"]
        assert not await self.client.sync_store.get_next_batch()
        release.set()
        await asyncio.wait_for(running, timeout=1)
        assert self.admitted == ["$encrypted"]
        saved = self.matrix.last_inputs[0]
        assert saved["input"]["type"] == "m.room.message"
        assert saved["input"]["content"]["body"] == plain["content"]["body"]
        assert await self.client.sync_store.get_next_batch() == "next"

    async def test_offline_message_enters_normal_initial_sync(self):
        old = event("$offline")
        old["origin_server_ts"] -= 86400000
        self.native_http.return_value = response(old)
        # A fresh client runs through the actual SDK full-state initial helper.
        self.client.sync = self.native_http
        await self.adapter._connect_initial_sync(self.client)
        assert self.admitted == ["$offline"]
        assert self.matrix.last_inputs[0]["input"]["origin_server_ts"] == old["origin_server_ts"]

    async def test_encrypted_non_message_does_not_block_the_next_batch(self):
        from mautrix.types import Event, EventType
        plain = event("$reaction")
        reacted = {**plain, "room_id": "!room:test", "type": "m.reaction", "content": {
            "m.relates_to": {"rel_type": "m.annotation", "event_id": "$other", "key": "x"}}}
        self.client.crypto = SimpleNamespace(decrypt_megolm_event=AsyncMock(return_value=Event.deserialize(reacted)))
        received = AsyncMock()
        self.client.add_event_handler(EventType.REACTION, received, wait_sync=True)
        encrypted = {**plain, "type": "m.room.encrypted", "content": {
            "algorithm": "m.megolm.v1.aes-sha2", "ciphertext": "fixture",
            "sender_key": "key", "device_id": "dev", "session_id": "session"}}
        await asyncio.wait_for(self.absorb(response(encrypted)), timeout=2)
        received.assert_awaited_once()
        assert not self.admitted
        assert await self.client.sync_store.get_next_batch() == "next"

    async def test_native_cache_write_failure_cannot_roll_back_shared_cursor(self):
        self.client.sync_store.put_next_batch = AsyncMock(side_effect=OSError("fixture local cache write failed"))
        with self.assertRaises(OSError):
            await self.absorb(response(event("$saved")))
        assert self.matrix.snapshot["state"]["consumers"]["native"]["token"] == "next"
        self.native_http.return_value = response(next_batch="newer")
        await self.client.sync(since="old-local-cache")
        assert self.native_http.call_args.kwargs["since"] == "next"
        assert self.admitted == ["$saved"]


if __name__ == "__main__":
    unittest.main()
