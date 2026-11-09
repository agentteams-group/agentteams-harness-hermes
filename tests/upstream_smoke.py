"""Exercise the installed SDK and Matrix plugin without external services."""
from __future__ import annotations

import asyncio
import inspect
import os
import shutil
import subprocess
import tempfile
from pathlib import Path
from types import SimpleNamespace
from unittest.mock import AsyncMock, patch


def main() -> None:
    tag = subprocess.check_output(
        ["git", "-C", "/opt/hermes-src", "describe", "--tags", "--exact-match", "HEAD"],
        text=True,
    ).strip()
    assert tag == "v0.23.0", tag
    with tempfile.TemporaryDirectory() as temp:
        home = Path(temp)
        os.environ["HERMES_HOME"] = str(home)
        os.environ["MATRIX_HOMESERVER"] = "http://matrix.invalid:8008"
        os.environ["MATRIX_ACCESS_TOKEN"] = "synthetic-smoke-token"
        os.environ["MATRIX_USER_ID"] = "@worker:matrix.invalid"
        os.environ["MATRIX_ALLOWED_USERS"] = "@alice:matrix.invalid"
        shutil.copytree("/opt/agentteams/hermes-plugins", home / "plugins")

        from hermes_worker.bridge import bridge_openclaw_to_hermes
        from hermes_worker.worker import Worker

        bridge_openclaw_to_hermes({
            "channels": {"matrix": {
                "homeserver": os.environ["MATRIX_HOMESERVER"],
                "accessToken": os.environ["MATRIX_ACCESS_TOKEN"],
                "userId": os.environ["MATRIX_USER_ID"],
            }},
            "models": {"providers": {"gw": {
                "baseUrl": "http://gateway.invalid/v1",
                "apiKey": "synthetic-smoke-key",
                "models": [{"id": "test-model", "input": ["text"]}],
            }}},
            "agents": {"defaults": {"model": {"primary": "gw/test-model"}}},
        }, home)
        Worker._load_env_file(home / ".env")

        from gateway.config import PlatformConfig, load_gateway_config
        from gateway.run import start_gateway
        from plugins.platforms.matrix.adapter import MatrixAdapter as Native
        from hermes_matrix.overlay_adapter import MatrixAdapter
        from hermes_cli.plugins import discover_plugins
        from gateway.platform_registry import platform_registry

        assert issubclass(MatrixAdapter, Native)
        inspect.signature(start_gateway).bind(None, replace=False, verbosity=0)
        inspect.signature(Native.connect).bind(None, is_reconnect=True)
        inspect.signature(Native._resolve_message_context).bind(
            None, "!room", "@alice", "$event", "body", {}, {},
            mention_claimed=False, voice_gate=None,
        )
        inspect.signature(Native._handle_media_message).bind(
            None, "!room", "@alice", "$event", 0.0, {}, {}, "m.image",
            mention_claimed=False,
        )
        inspect.signature(Native._build_inbound_event).bind(
            None, "!room", "@alice", "$event", "body", {}, {}, ctx=None,
        )

        discover_plugins()
        entry = platform_registry.get("matrix")
        assert entry is not None and entry.adapter_factory is MatrixAdapter, entry
        gateway = load_gateway_config()
        assert any(str(getattr(k, "value", k)) == "matrix" and v.enabled
                   for k, v in gateway.platforms.items()), gateway.platforms

        async def check_mentions() -> None:
            adapter = MatrixAdapter(PlatformConfig(enabled=True))
            adapter._user_id = "@worker:matrix.invalid"
            sent = AsyncMock(return_value="$reply")
            adapter._client = SimpleNamespace(send_message_event=sent)
            with patch.object(Native, "connect", new=AsyncMock(return_value=True)):
                assert await adapter.connect(is_reconnect=True)
            content = {"msgtype": "m.text", "body": "Hello @alice:matrix.invalid"}
            await adapter._client.send_message_event("!room", "m.room.message", content)
            assert "@alice:matrix.invalid" in content["m.mentions"]["user_ids"]
            assert sent.await_count == 1

        asyncio.run(check_mentions())
    print(f"PASS: {tag}, gateway config, native Matrix interfaces, plugin discovery, reconnect and mentions")


if __name__ == "__main__":
    main()
