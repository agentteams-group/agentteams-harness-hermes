"""Manager completion comes from the installed SDK's acknowledged final send."""
import asyncio
from pathlib import Path
from tempfile import TemporaryDirectory
from types import SimpleNamespace
from unittest.mock import AsyncMock, Mock, patch

from agentteams_receipts import Receipts
from hermes_matrix.overlay_adapter import MatrixAdapter


async def check():
    with TemporaryDirectory() as directory, patch.dict("os.environ", {"AGENTTEAMS_AGENT_ROLE": "manager"}):
        adapter = MatrixAdapter.__new__(MatrixAdapter)
        adapter._session_receipts = Receipts(Path(directory))
        event = SimpleNamespace(source=SimpleNamespace(chat_id="!dm:test"), message_id="$source")
        row = Mock()
        with patch.object(adapter, "send_final_ledgered", new=AsyncMock(side_effect=[
            (SimpleNamespace(success=False, message_id=None), adapter),
            (SimpleNamespace(success=True, message_id="$final"), adapter),
        ])):
            await adapter._send_final_text(event, "key", "final", {}, False, 0, row)
            unacked = Mock()
            adapter._session_receipts.flush(unacked)
            unacked.assert_not_called()
            await adapter._send_final_text(event, "key", "final", {}, False, 0, row)
        request = Mock(return_value={"event_id": "$receipt"})
        adapter._session_receipts.flush(request)
        assert request.call_args.args[2]["status"] == "completed"
        assert request.call_args.args[2]["m.relates_to"]["event_id"] == "$source"
        assert row.call_count == 2


asyncio.run(check())
print("Hermes Manager writes completion only after the native final send succeeds")
