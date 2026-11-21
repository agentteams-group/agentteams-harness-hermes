"""Drive Hermes' real overlay ingress against public Controller receipts."""
import asyncio
import os
from pathlib import Path
import sys
from types import SimpleNamespace
from unittest.mock import AsyncMock

sys.path.insert(0, str(Path(os.environ["AT_SOURCE"]) / "tests/lib"))
from assistant_fixture import AssistantFixture
from test_shared_input_boundary import fixture


def test_assistant_wakes_through_teams_and_rejects_replaced_role(monkeypatch):
    _module, _native, adapter = fixture(monkeypatch)
    with AssistantFixture() as f:
        adapter._user_id = f.own
        adapter._is_allowed_matrix_room_event = AsyncMock(return_value=True)
        adapter._is_duplicate_event = lambda _: False
        adapter._dispatch_coordination = AsyncMock(return_value=True)
        adapter._pending_matrix = None
        event = SimpleNamespace(room_id=f.room, event_id="$assistant", sender=f.own, content=f.content())
        asyncio.run(adapter._on_matrix_input(event))
        assert adapter._accept_matrix_input.await_args.args[-1] == "control"
        packet = adapter._dispatch_coordination.await_args.args[1]
        assert packet["text"] == f.text
        assert packet["session_id"] == f.control["session_id"]
        f.context["owner"] = "replaced"
        asyncio.run(adapter._on_matrix_input(event))
        assert adapter._dispatch_coordination.await_count == 1
