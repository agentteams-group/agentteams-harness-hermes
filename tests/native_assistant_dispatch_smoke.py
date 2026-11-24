"""Check the installed Hermes event's authorization flags after AT admission."""
import asyncio
from types import SimpleNamespace
from unittest.mock import AsyncMock

from gateway.config import PlatformConfig
from hermes_matrix.overlay_adapter import MatrixAdapter


async def main():
    adapter = MatrixAdapter(PlatformConfig(enabled=True))
    adapter._user_id = "@leader:test"
    adapter.handle_message = AsyncMock()
    for sender, kind, internal in [
        ("@leader:test", "assistant_digest", True),
        ("@human:test", "assistant_digest", False),
        ("@leader:test", "task_assigned", False),
    ]:
        event = SimpleNamespace(sender=sender, room_id="!assistant:test", event_id="$assistant",
            content={"msgtype":"m.notice", "body":"Background Teams wake"})
        packet = {"text":"Canonical admitted input", "meta":{"teamharness_coordination":{"kind":kind}}}
        await adapter._dispatch_coordination(event, packet)
        message = adapter.handle_message.call_args.args[0]
        assert message.internal is internal, (sender, kind, message.internal)
        assert message.allow_gateway_control is False
        assert message.source.thread_id == "$assistant"
    print("Installed Hermes SDK: only admitted own assistant input is internal; gateway controls remain disabled")


if __name__ == "__main__":
    asyncio.run(main())
