"""Native connect may synchronously dispatch Matrix input during first sync."""
import asyncio
from types import SimpleNamespace
from unittest.mock import Mock

from test_overlay_image_downgrade import _load_overlay_with_fake_gateway


def test_recovery_claim_precedes_connect_without_blocking_initial_sync(monkeypatch):
    module, native = _load_overlay_with_fake_gateway(monkeypatch)
    monkeypatch.delenv("AGENTTEAMS_CONTROLLER_URL", raising=False)
    order = []

    async def check():
        adapter = module.MatrixAdapter.__new__(module.MatrixAdapter)
        adapter._client = object()
        adapter._wrap_send_message_event = Mock()
        # Exercise the prior gate's real startup ordering as well: waiting for
        # this gate inside initial sync deadlocks before connect can finish.
        adapter._recovery_admission = asyncio.Event()
        adapter._recovery_admission.set()
        async def recover(*args, **kwargs):
            order.append("claim-only" if kwargs.get("claim_only") else "resume")
        async def on_message(self, event):
            order.append("native-sync")
        async def connect(self, **kwargs):
            await self._on_room_message(SimpleNamespace(content={}))
            return True
        adapter._recover_tasks = recover
        monkeypatch.setattr(native, "_on_room_message", on_message, raising=False)
        monkeypatch.setattr(native, "connect", connect, raising=False)
        assert await asyncio.wait_for(adapter.connect(), timeout=1)
    asyncio.run(check())
    assert order == ["claim-only", "native-sync", "resume"]
