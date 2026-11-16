"""A standalone Hermes SDK without Controller recovery has no Core dependency."""
import asyncio
import sys
from unittest.mock import Mock

from test_overlay_image_downgrade import _load_overlay_with_fake_gateway


def test_unconfigured_recovery_does_not_import_core_library(monkeypatch):
    module, native = _load_overlay_with_fake_gateway(monkeypatch)
    monkeypatch.delenv("AGENTTEAMS_CONTROLLER_URL", raising=False)
    monkeypatch.delenv("AGENTTEAMS_PROCESS_ID", raising=False)
    monkeypatch.setitem(sys.modules, "task_recovery", None)

    async def check():
        adapter = module.MatrixAdapter.__new__(module.MatrixAdapter)
        adapter._client = object()
        adapter._wrap_send_message_event = Mock()
        async def connect(self, **kwargs):
            return True
        monkeypatch.setattr(native, "connect", connect, raising=False)
        assert await adapter.connect(is_reconnect=True)
    asyncio.run(check())
