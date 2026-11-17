"""Exercise the installed SDK and Matrix plugin without external services."""
from __future__ import annotations

import asyncio
import inspect
import os
import shutil
import subprocess
import tempfile
import yaml
from pathlib import Path
from types import SimpleNamespace
from unittest.mock import AsyncMock, patch


def main() -> None:
    for helper in ("install-worker-skill.sh", "safe-extract-worker-skill.py", "push-worker-skills.sh"):
        assert Path("/opt/agentteams/agent/skills/worker-management/scripts", helper).is_file(), helper
    tag = subprocess.check_output(
        ["git", "-C", "/opt/hermes-src", "describe", "--tags", "--exact-match", "HEAD"],
        text=True,
    ).strip()
    assert tag == "v0.23.0", tag
    with tempfile.TemporaryDirectory() as temp:
        workspace = Path(temp)
        home = workspace / ".hermes"
        os.environ["HERMES_HOME"] = str(home)
        os.environ["AGENTTEAMS_AGENT_ROLE"] = "worker"
        os.environ["AGENTTEAMS_WORKER_ROLE"] = "worker"
        os.environ["AGENTTEAMS_WORKER_NAME"] = "smoke-worker"
        os.environ["AGENTTEAMS_OTEL_RUNTIME"] = "hermes"
        os.environ["AGENTTEAMS_LOG_ROLE"] = "worker"
        os.environ["AGENTTEAMS_LOG_RUNTIME"] = "hermes"
        os.environ["MATRIX_HOMESERVER"] = "http://matrix.invalid:8008"
        os.environ["MATRIX_ACCESS_TOKEN"] = "synthetic-smoke-token"
        os.environ["MATRIX_USER_ID"] = "@worker:matrix.invalid"
        os.environ["MATRIX_ALLOWED_USERS"] = "@alice:matrix.invalid"
        os.environ["WORKER_SMOKE_GATEWAY_KEY"] = "worker-scoped-smoke-key"
        os.environ["WORKER_SMOKE_MATRIX_TOKEN"] = "worker-scoped-matrix-test-token"
        runtime = workspace / "runtime/runtime.yaml"
        runtime.parent.mkdir()
        record = yaml.safe_dump({
            "member": {"runtime": "hermes", "role": "worker", "runtimeName": "smoke-worker",
                       "matrixUserId": "@worker:matrix.invalid", "personalRoomId": "!home:matrix.invalid"},
            "matrix": {"transport": {"homeserver": "http://matrix.invalid:8008", "encryption": False,
                "dm": {"policy": "allowlist", "allowFrom": ["@alice:matrix.invalid"]},
                "groupPolicy": "allowlist", "groupAllowFrom": ["@alice:matrix.invalid"]}},
            "desired": {"model": {"model": "worker-model", "providerId": "agentteams-gateway",
                "gatewayUrl": "http://worker-gateway.invalid", "input": ["text"]}},
            "credentials": {"gatewayKeyEnv": "WORKER_SMOKE_GATEWAY_KEY", "matrixTokenEnv": "WORKER_SMOKE_MATRIX_TOKEN"},
        })
        runtime.write_text(record)

        from hermes_worker.bridge import bridge_openclaw_to_hermes
        from hermes_worker.worker import Worker
        from hermes_worker.sync import FileSync

        sync = FileSync(endpoint="http://storage.invalid", access_key="fixture", secret_key="fixture",
                        bucket="fixture", worker_name="smoke-worker", local_dir=workspace)
        for empty_native_directory in (False, True):
            if empty_native_directory:
                shutil.rmtree(home)
            else:
                home.mkdir()
                (home / "config.yaml").write_text(yaml.safe_dump({
                    "model": {"default": "old-native-model"},
                    "auxiliary": {"vision": {"api_key": "old-native-key"}},
                }))
                (home / ".env").write_text("OPENAI_API_KEY=old-native-key\n")
            shutil.copytree("/opt/agentteams/hermes-plugins", home / "plugins")
            # Exercise the startup loader without a compatibility file. Only
            # the object-store read is a fixture; the bridge and SDK are real.
            with patch.object(sync, "_cat", side_effect=lambda key: record if key.endswith("/runtime/runtime.yaml") else None):
                projected = sync.get_config()
            assert not (workspace / "openclaw.json").exists()
            bridge_openclaw_to_hermes(projected, home)
            Worker._load_env_file(home / ".env")
            from hermes_cli.config import load_config
            effective = load_config()
            assert effective["model"]["default"] == "worker-model"
            assert effective["model"]["base_url"] == "http://worker-gateway.invalid/v1"
            assert os.environ["OPENAI_API_KEY"] == "worker-scoped-smoke-key"
            assert os.environ["MATRIX_ACCESS_TOKEN"] == "worker-scoped-matrix-test-token"
            assert "old-native-key" not in (home / "config.yaml").read_text()

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
    print(f"PASS: {tag}, Worker model reconstruction/SDK readback, gateway config, native Matrix interfaces, plugin discovery, mocked reconnect and mentions (no model/Task execution)")


if __name__ == "__main__":
    main()
