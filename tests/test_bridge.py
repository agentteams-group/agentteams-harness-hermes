"""Tests for hermes worker config bridging."""

from __future__ import annotations

import tempfile
from pathlib import Path

import yaml

import hermes_worker.bridge as bridge_module
from hermes_worker.bridge import bridge_openclaw_to_hermes


def _make_openclaw_cfg(*, vision: bool) -> dict:
    model_input = ["text", "image"] if vision else ["text"]
    return {
        "channels": {
            "matrix": {
                "homeserver": "http://agentteams-controller:6167",
                "accessToken": "tok",
                "userId": "@alice:matrix.local",
            }
        },
        "models": {
            "providers": {
                "gw": {
                    "baseUrl": "http://aigw:8080/v1",
                    "apiKey": "key123",
                    "models": [
                        {
                            "id": "qwen3.5-plus",
                            "name": "qwen3.5-plus",
                            "contextWindow": 200000,
                            "input": model_input,
                        }
                    ],
                }
            }
        },
        "agents": {
            "defaults": {
                "model": {
                    "primary": "gw/qwen3.5-plus",
                }
            }
        },
    }


def _bridge_and_read(openclaw_cfg: dict, initial_config: dict | None = None) -> dict:
    with tempfile.TemporaryDirectory() as tmpdir:
        hermes_home = Path(tmpdir) / ".hermes"
        hermes_home.mkdir(parents=True, exist_ok=True)
        if initial_config is not None:
            (hermes_home / "config.yaml").write_text(
                yaml.safe_dump(initial_config, sort_keys=False)
            )
        bridge_openclaw_to_hermes(openclaw_cfg, hermes_home)
        return yaml.safe_load((hermes_home / "config.yaml").read_text())


def test_manager_role_is_bound_to_native_system_prompt(tmp_path, monkeypatch):
    monkeypatch.setenv("AGENTTEAMS_AGENT_ROLE", "manager")
    bridge_openclaw_to_hermes(_make_openclaw_cfg(vision=False), tmp_path,
                             soul="Your administrator prefers concise answers.",
                             agents_md="You are the TeamHarness Manager. Delegate team work to the Leader.")
    soul = (tmp_path / "SOUL.md").read_text()
    assert "You are the TeamHarness Manager." in soul
    assert "Your administrator prefers concise answers." in soul
    bridge_openclaw_to_hermes(_make_openclaw_cfg(vision=False), tmp_path, soul=soul,
                             agents_md="You are the TeamHarness Manager. Delegate team work to the Leader.")
    assert (tmp_path / "SOUL.md").read_text() == soul


def test_native_transport_receives_group_members_without_expanding_dm_access(tmp_path, monkeypatch):
    from hermes_matrix.policies import DualAllowList
    config = _make_openclaw_cfg(vision=False)
    config["channels"]["matrix"]["dm"] = {"allowFrom": ["@admin:matrix.local"]}
    config["channels"]["matrix"]["groupAllowFrom"] = ["@leader:matrix.local"]
    bridge_openclaw_to_hermes(config, tmp_path)
    for key, value in bridge_module._read_env_file(tmp_path / ".env").items():
        monkeypatch.setenv(key, value)
    policy = DualAllowList.from_env()
    assert policy.permits("@leader:matrix.local", is_dm=False)
    assert not policy.permits("@leader:matrix.local", is_dm=True)
    assert "@leader:matrix.local" in bridge_module._read_env_file(tmp_path / ".env")["MATRIX_ALLOWED_USERS"]


def test_bridge_writes_auxiliary_vision_config(monkeypatch) -> None:
    monkeypatch.setattr(bridge_module, "_is_in_container", lambda: False)

    config = _bridge_and_read(
        _make_openclaw_cfg(vision=True),
        initial_config={
            "auxiliary": {
                "vision": {
                    "timeout": 42,
                    "download_timeout": 7,
                }
            }
        },
    )

    vision = config["auxiliary"]["vision"]
    assert vision["provider"] == "custom"
    assert vision["model"] == "qwen3.5-plus"
    assert vision["base_url"] == "http://aigw:18080/v1"
    assert vision["api_key"] == "key123"
    assert vision["timeout"] == 42
    assert vision["download_timeout"] == 7


def test_bridge_skips_auxiliary_vision_for_text_only_model(monkeypatch) -> None:
    monkeypatch.setattr(bridge_module, "_is_in_container", lambda: False)

    config = _bridge_and_read(_make_openclaw_cfg(vision=False))

    assert "auxiliary" not in config or "vision" not in config["auxiliary"]


def test_bridge_enables_debug_logging_when_matrix_debug_set(monkeypatch) -> None:
    monkeypatch.setenv("AGENTTEAMS_MATRIX_DEBUG", "1")

    config = _bridge_and_read(
        _make_openclaw_cfg(vision=False),
        initial_config={
            "logging": {
                "max_size_mb": 12,
                "backup_count": 9,
            }
        },
    )

    assert config["logging"]["level"] == "DEBUG"
    assert config["logging"]["max_size_mb"] == 12
    assert config["logging"]["backup_count"] == 9


def test_bridge_enables_agentteams_matrix_plugin() -> None:
    config = _bridge_and_read(
        _make_openclaw_cfg(vision=False),
        initial_config={
            "plugins": {
                "enabled": ["other-plugin"],
                "disabled": ["platforms/matrix", "matrix-platform", "legacy-plugin"],
            }
        },
    )

    assert config["plugins"]["enabled"] == ["other-plugin", "platforms/matrix"]
    assert config["plugins"]["disabled"] == ["legacy-plugin"]
