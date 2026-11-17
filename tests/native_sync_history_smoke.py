"""Pinned Hermes/mautrix history decoding and policy with fixture HTTP and authority."""
import copy
from types import SimpleNamespace
import unittest
from unittest.mock import AsyncMock, Mock

from mautrix.types import Event
from hermes_matrix.sync_history import recover_history
from task_recovery import RecoveryConflict
from worker_matrix_state import matrix_event_key
import native_sync_state_smoke as fixture


class HistoryTests(unittest.IsolatedAsyncioTestCase):
    async def asyncSetUp(self):
        await fixture.NativeSyncTests.asyncSetUp(self)
        self.room = "!room:test"
        self.client.get_joined_rooms = AsyncMock(return_value=[self.room])
        self.gap = {"from_token": "history", "until": "$anchor", "before": "new-input"}
        self.matrix.snapshot["state"]["rooms"][self.room] = {"gaps": ["history"],
            "backfill": {"history": self.gap}, "through": "$latest"}
        self.matrix.backfill = Mock(side_effect=lambda snapshot, page: snapshot)

    def message(self, event_id, **kwargs):
        return {**fixture.event(event_id, **kwargs), "room_id": self.room}

    def page(self, events, *, start="history", end="older"):
        self.client.api.request = AsyncMock(return_value={"start": start, "end": end, "chunk": events})

    async def test_native_reverse_page_prepares_without_running_or_rebuffering(self):
        anchor = self.message("$anchor")
        self.matrix.inputs[matrix_event_key(self.room, "$anchor")] = {"room_id": self.room, "event_id": "$anchor",
            "intent": "message", "input": {"sender": "@human:test", "type": "m.room.message",
                "content": anchor["content"]}, "native_commit": {"runtime": "hermes"}}
        self.page([self.message("$latest"), self.message("$filtered", sender="@worker:test"),
            self.message("$context", mention=False), anchor, self.message("$already-covered")])
        self.adapter._processed_events.append("$latest")
        self.adapter._processed_events_set.add("$latest")
        assert await recover_history(self.shared) == {}
        params = self.client.api.request.await_args.kwargs["query_params"]
        assert params["dir"] == "b" and params["from"] == "history"
        page = self.matrix.backfill.call_args.args[1]
        assert [i["event_id"] for i in page["inputs"]] == ["$latest", "$context", "$anchor"]
        assert [i["intent"] for i in page["inputs"]] == ["message", "context", "message"]
        assert "native_commit" not in page["inputs"][-1]
        assert page["from_token"] == "history" and page["next_token"] == "older"
        assert self.admitted == []
        assert self.adapter._history.event_ids(self.room) == []
        assert self.adapter._processed_events_set == {"$latest"}

    async def test_actual_decoder_identity_and_errors_guard_entire_page(self):
        raw = self.message("$encrypted")
        encrypted = {**raw, "type": "m.room.encrypted", "content": {"algorithm": "m.megolm.v1.aes-sha2",
            "ciphertext": "fixture", "sender_key": "k", "device_id": "device", "session_id": "session"}}
        self.page([self.message("$sibling"), encrypted])
        decrypt = AsyncMock(side_effect=RuntimeError("missing key"))
        self.client.crypto = SimpleNamespace(decrypt_megolm_event=decrypt)
        failures = await recover_history(self.shared)
        assert self.room in failures and not self.admitted
        self.matrix.backfill.assert_not_called()
        decrypt.side_effect = None
        decrypt.return_value = Event.deserialize({**copy.deepcopy(raw), "room_id": "!foreign:test"})
        assert isinstance((await recover_history(self.shared))[self.room], RecoveryConflict)
        self.matrix.backfill.assert_not_called()
        decrypt.return_value = Event.deserialize(copy.deepcopy(raw))
        assert await recover_history(self.shared) == {}
        saved = self.matrix.backfill.call_args.args[1]["inputs"][-1]
        assert saved["input"]["content"]["body"] == raw["content"]["body"] and not self.admitted

    async def test_native_history_membership_and_page_start_must_match(self):
        self.page([self.message("$new")], start="unrelated")
        self.client.get_joined_rooms.return_value = ["!other:test"]
        assert self.room in await recover_history(self.shared)
        self.client.api.request.assert_not_awaited()
        self.client.get_joined_rooms.return_value = [self.room]
        assert self.room in await recover_history(self.shared)
        self.matrix.backfill.assert_not_called()
        self.page([{**self.message("$new"), "room_id": "!other:test"}])
        assert self.room in await recover_history(self.shared)
        self.matrix.backfill.assert_not_called()

    async def test_absorb_drives_preexisting_history_without_new_room_messages(self):
        self.page([self.message("$offline")], end=None)
        await fixture.NativeSyncTests.absorb(self, {"next_batch": "tick"})
        self.matrix.backfill.assert_called_once()
        page = self.matrix.backfill.call_args.args[1]
        assert page["inputs"][0]["event_id"] == "$offline" and page["next_token"] == ""
        assert self.admitted == []


if __name__ == "__main__":
    unittest.main()
