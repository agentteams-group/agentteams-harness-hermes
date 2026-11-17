"""Pinned Hermes executor and gateway boundaries, with fixture transport and authority."""
import hashlib
import os
from pathlib import Path
import tempfile
import unittest
from types import MethodType, SimpleNamespace
from unittest.mock import AsyncMock, Mock, patch

from agent.tool_dispatch_helpers import make_tool_result_message
from task_recovery import RecoveryConflict
from hermes_matrix.message_execution import PendingInputs, execute_tools


class OrdinaryTests(unittest.IsolatedAsyncioTestCase):
    async def asyncSetUp(self):
        self.key = "durable-key"
        self.identity = {"event_key": self.key, "generation": 2, "instance_id": "container", "process_id": "process"}
        self.matrix = Mock()
        self.matrix.load.return_value = {"state": {"pending": [self.key]}}
        self.entry = {"input": {"sender": "@person:test", "content": {"msgtype": "m.text", "body": "hello"}},
                      "event_id": "$source", "room_id": "!room:test", "intent": "message"}
        self.matrix.input.return_value = self.entry
        self.saved = {"input": self.entry, "recovery": "deliver", "identity": self.identity,
                      "execution": {"state": "active", "checkpoint": {"version":1,"delivery": "pending", "reply_txn": "same-txn", "result":{"size":len(b"durable answer"),"sha256":hashlib.sha256(b"durable answer").hexdigest()},"reply_attempt":{"state":"sending","user_id":"@worker:test","device_id":"A"}}},
                      "saved_result": "durable answer"}
        self.matrix.message.return_value = self.saved
        self.matrix.message_claim.return_value = self.saved
        self.matrix.message_delivered.return_value = {"recovery": "done"}
        self.client = SimpleNamespace(mxid="@worker:test", whoami=AsyncMock(return_value=SimpleNamespace(user_id="@worker:test",device_id="A")), get_joined_rooms=AsyncMock(return_value=["!room:test"]),
            send_message_event=AsyncMock(side_effect=[TimeoutError("ACK lost"), "$canonical"]))
        self.adapter = SimpleNamespace(_client=self.client, _dual_allow=Mock(), _is_dm_room=AsyncMock(return_value=True),
            _is_allowed_matrix_room_event=AsyncMock(return_value=True), _is_self_sender=Mock(return_value=False),
            _is_sender_authorized=Mock(return_value=True), _is_system_or_bridge_sender=Mock(return_value=False), _ignored_user_patterns=[],
            handle_message=AsyncMock())
        self.ordinary = PendingInputs(self.adapter, self.matrix)

    async def test_restart_retries_same_saved_bytes_and_txn_without_inference(self):
        await self.ordinary.drain()
        self.matrix.message_delivered.assert_not_called()
        await self.ordinary.drain()
        self.adapter.handle_message.assert_not_awaited()
        sends = self.client.send_message_event.await_args_list
        self.assertEqual(len(sends), 2)
        for args, kwargs in sends:
            self.assertEqual(args[2]["body"], "durable answer")
            self.assertEqual(args[2]["m.relates_to"]["m.in_reply_to"]["event_id"], "$source")
            self.assertEqual(kwargs["txn_id"], "same-txn")
        self.matrix.message_delivered.assert_called_once_with(self.identity, "$canonical")

    async def test_changed_device_recovers_original_event_and_never_resends(self):
        from mautrix.types import Event
        reply = Event.deserialize({"room_id":"!room:test","type":"m.room.message","event_id":"$canonical",
            "sender":"@worker:test","origin_server_ts":12345,"content":{"msgtype":"m.text","body":"durable answer","org.agentteams.reply_txn":"same-txn"}})
        self.client.whoami.return_value.device_id = "B"
        self.client.get_messages = AsyncMock(return_value=SimpleNamespace(events=[reply],end=None))
        await self.ordinary.drain()
        self.client.send_message_event.assert_not_awaited()
        self.matrix.message_delivered.assert_called_once_with(self.identity, "$canonical")
        self.matrix.message_delivered.reset_mock()
        reply.sender = "@other:test"
        await self.ordinary.drain()
        self.matrix.message_delivered.assert_not_called()
        self.client.send_message_event.assert_not_awaited()

    async def test_reassigned_room_or_unknown_outcome_never_dispatches(self):
        self.client.get_joined_rooms.return_value = []
        await self.ordinary.drain()
        self.matrix.message_claim.assert_not_called()
        self.client.get_joined_rooms.return_value = ["!room:test"]
        self.matrix.message.return_value = {**self.saved, "recovery": "verify"}
        self.matrix.message_claim.return_value = {**self.saved, "recovery": "verify"}
        await self.ordinary.drain()
        self.client.send_message_event.assert_not_awaited()
        self.adapter.handle_message.assert_not_awaited()

    async def test_empty_sync_recovers_only_verified_assignment_after_room_gap_closes(self):
        self.adapter._user_id = "@worker:test"
        self.adapter._recover_tasks = AsyncMock()
        self.entry.update(intent="task_notification", work={"kind":"task", "team_id":"team", "task_id":"T"})
        packet = {"meta":{"teamharness_coordination":{"kind":"task_assigned","task_id":"T"}}}
        room = {"gaps":["older-page"]}
        self.matrix.load.return_value["state"]["rooms"] = {"!room:test":room}
        with patch("task_coordination.notification", return_value=packet) as verified:
            await self.ordinary.drain()
            self.adapter._recover_tasks.assert_not_awaited()
            room["gaps"] = []
            await self.ordinary.drain()
            recovered = self.adapter._recover_tasks.await_args
            self.assertEqual(recovered.args, ("T",))
            self.assertEqual(recovered.kwargs["only_team"], "team")
            self.assertEqual(recovered.kwargs["source_event"].event_id, "$source")
            verified.assert_called_once_with(self.entry["input"]["content"], "!room:test", "@person:test", "@worker:test")
            self.entry["native_commit"] = {"runtime":"hermes"}
            await self.ordinary.drain()
            self.adapter._recover_tasks.assert_awaited_once()
        self.client.send_message_event.assert_not_awaited()

    async def test_quiet_start_builds_native_session_from_current_checkpoint(self):
        from gateway.config import GatewayConfig, PlatformConfig
        from gateway.run import GatewayRunner
        from gateway.session import SessionStore
        from hermes_matrix.overlay_adapter import MatrixAdapter
        from hermes_matrix.session_cache import session_fingerprint
        import time
        with tempfile.TemporaryDirectory() as tmp, patch.dict(os.environ, {"HERMES_HOME": tmp}):
            adapter = MatrixAdapter(PlatformConfig(enabled=True))
            adapter._user_id, adapter._client = "@worker:test", self.client
            adapter._is_dm_room = AsyncMock(return_value=False)
            adapter._is_allowed_matrix_room_event = AsyncMock(return_value=True)
            adapter._is_sender_authorized = Mock(return_value=True)
            adapter._dual_allow = Mock()
            runner = GatewayRunner.__new__(GatewayRunner)
            runner.config = SimpleNamespace(multiplex_profiles=False)
            runner.session_store = SessionStore(Path(tmp) / "sessions", GatewayConfig())
            runner._is_session_running = Mock(return_value=False)
            runner._evict_cached_agent = Mock()
            admitted = []
            async def handle(_runner, event):
                event._gateway_accepted = True
                admitted.append(event)
            adapter._message_handler = adapter.handle_message = MethodType(handle, runner)
            ordinary = PendingInputs(adapter, self.matrix)
            self.matrix.recovery.process = "process"
            claim = {**self.saved, "recovery": "resume", "checkpoint_digest": "current",
                     "resume_text": "Step one verified. Continue at two."}
            claim["execution"] = {"generation": 2, "checkpoint": {"version": 2}}
            self.matrix.message.return_value = self.matrix.message_claim.return_value = self.matrix.message_current.return_value = claim
            view = {"runtime": "hermes", "instance_id": "container", "generation": 2,
                    "checkpoint_version": 2, "checkpoint_digest": "current", "config_generation": 3,
                    "etag": "", "cache": None, "compatible": False}
            self.matrix.recovery.request.return_value = view
            await ordinary.drain()
            self.assertEqual(len(admitted), 1)
            first = admitted[-1]
            self.assertEqual(first.message_id, "$source")
            self.assertEqual(first.text, claim["resume_text"])
            self.assertFalse(first.allow_gateway_control)
            self.assertFalse(first.internal)
            native = first._agentteams_message.session
            db = runner.session_store._db_for_key(native.session_key)
            db.append_message(native.session_id, "user", "durable source")
            view.update(cache={"runtime":"hermes", "native_ref":native.session_id,
                "native_digest":session_fingerprint(runner.session_store, native),
                "expires_at": time.time()+3600, "checkpoint_digest":"current",
                "checkpoint_version":2, "config_generation":3}, compatible=True)
            for current in (True, False):
                if not current:
                    db.append_message(native.session_id, "assistant", "uncheckpointed stale context")
                runner.session_store = SessionStore(Path(tmp) / "sessions", GatewayConfig())
                await PendingInputs(adapter, self.matrix).drain()
                selected = admitted[-1]._agentteams_message.session
                self.assertEqual(selected.session_id == native.session_id, current)
            self.assertEqual(len(admitted), 3)
            self.client.send_message_event.assert_not_awaited()

    def test_sdk_pairs_real_tool_results_and_fences_unknown_timeout(self):
        call = SimpleNamespace(id="call", function=SimpleNamespace(name="terminal", arguments='{"command":"pwd"}'))
        assistant = SimpleNamespace(tool_calls=[call])
        self.matrix.message_begin_tools.return_value = "op-id"
        for disposition in (None, "unknown"):
            with self.subTest(disposition=disposition):
                self.matrix.reset_mock()
                rows = []
                def native(agent, assistant, messages, task, iteration):
                    self.matrix.message_begin_tools.assert_called_once()
                    self.matrix.message_finish_tools.assert_not_called()
                    messages.append(make_tool_result_message("terminal", "actual-native-return", "call", effect_disposition=disposition))
                if disposition == "unknown":
                    with self.assertRaises(RecoveryConflict):
                        execute_tools(self.matrix, self.identity, native, None, assistant, rows, "task", 1)
                    self.matrix.message_finish_tools.assert_not_called()
                else:
                    execute_tools(self.matrix, self.identity, native, None, assistant, rows, "task", 1)
                    self.matrix.message_finish_tools.assert_called_once_with(self.identity, "op-id", [{"id":"call","content":"actual-native-return"}])


if __name__ == "__main__":
    unittest.main()
