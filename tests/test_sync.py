"""Tests for Hermes worker file sync behavior."""

from __future__ import annotations

import json
import os
import subprocess
import pytest
import yaml

from hermes_worker import sync as sync_module
from hermes_worker.sync import FileSync, push_local


def test_workspace_mirror_never_writes_controller_owned_recovery_records(tmp_path, monkeypatch):
    sync = FileSync(endpoint="http://minio:9000", access_key="test", secret_key="test",
                    bucket="test", worker_name="worker", local_dir=tmp_path)
    for rel in ["runtime/runtime.yaml", "runtime/state.json", "runtime/compat-config.json",
                "runtime/matrix/state.json", "runtime/matrix/events/event.json", "runtime/sessions/hermes/work.json"]:
        path = tmp_path / rel
        path.parent.mkdir(parents=True, exist_ok=True)
        path.write_text("old local projection")
    (tmp_path / "SOUL.md").write_text("Your personality")
    monkeypatch.setattr(sync, "_ensure_alias", lambda: None)
    monkeypatch.setattr(sync, "_cat", lambda key: "")
    copies = []
    monkeypatch.setattr(sync_module, "_mc", lambda *args, **kwargs: copies.append(args))
    assert push_local(sync) == ["SOUL.md"]
    assert len(copies) == 1 and copies[0][-1].endswith("/agents/worker/SOUL.md")


def test_standard_worker_record_loads_without_compatibility_or_native_config(tmp_path, monkeypatch):
    sync = FileSync(endpoint="http://minio:9000", access_key="test", secret_key="test",
                    bucket="test", worker_name="worker", local_dir=tmp_path)
    record = {
        "member": {"runtime": "hermes", "runtimeName": "worker", "matrixUserId": "@worker:matrix.test",
                   "personalRoomId": "!worker:matrix.test"},
        "matrix": {"transport": {"homeserver": "https://matrix.test", "encryption": True,
                    "dm": {"policy": "allowlist", "allowFrom": ["@admin:matrix.test"]},
                    "groupPolicy": "allowlist", "groupAllowFrom": ["@leader:matrix.test"], "historyLimit": 24}},
        "desired": {"model": {"model": "worker-model", "gatewayUrl": "https://gateway.test/v1"}},
        "credentials": {"matrixTokenEnv": "WORKER_BOUND_MATRIX_TOKEN", "gatewayKeyEnv": "WORKER_BOUND_GATEWAY_KEY"},
    }
    monkeypatch.setenv("WORKER_BOUND_MATRIX_TOKEN", "scoped-test-token")
    monkeypatch.setenv("WORKER_BOUND_GATEWAY_KEY", "scoped-model-test-key")
    requested = []

    def read(key):
        requested.append(key)
        return yaml.safe_dump(record) if key.endswith("/runtime/runtime.yaml") else None

    monkeypatch.setattr(sync, "_cat", read)
    config = sync.get_config()
    matrix = config["channels"]["matrix"]
    assert matrix["homeserver"] == "https://matrix.test"
    assert matrix["userId"] == "@worker:matrix.test"
    assert matrix["accessToken"] == "scoped-test-token"
    assert matrix["homeRoomId"] == "!worker:matrix.test"
    assert matrix["groupAllowFrom"] == ["@leader:matrix.test"]
    assert matrix["historyLimit"] == 24
    assert yaml.safe_load((tmp_path / "runtime/runtime.yaml").read_text()) == record
    assert not any(key.endswith("openclaw.json") for key in requested)
    assert not (tmp_path / ".hermes").exists()
    from hermes_worker.bridge import bridge_openclaw_to_hermes
    bridge_openclaw_to_hermes(config, tmp_path / ".hermes")
    native = yaml.safe_load((tmp_path / ".hermes/config.yaml").read_text())
    assert native["model"]["default"] == "worker-model"
    environment = (tmp_path / ".hermes/.env").read_text()
    assert "scoped-test-token" in environment and "scoped-model-test-key" in environment
    assert "@leader:matrix.test" in environment


def test_missing_standard_matrix_binding_cannot_fall_back_to_old_token(tmp_path, monkeypatch):
    sync = FileSync(endpoint="http://minio:9000", access_key="test", secret_key="test",
                    bucket="test", worker_name="worker", local_dir=tmp_path)
    record = {"member": {"runtime": "hermes", "matrixUserId": "@worker:matrix.test"},
              "matrix": {"transport": {"homeserver": "https://matrix.test"}},
              "credentials": {"matrixTokenEnv": "WORKER_BOUND_MATRIX_TOKEN"}}
    monkeypatch.delenv("WORKER_BOUND_MATRIX_TOKEN", raising=False)
    monkeypatch.setattr(sync, "_cat", lambda key: yaml.safe_dump(record) if key.endswith("runtime.yaml")
                        else '{"channels":{"matrix":{"accessToken":"old-token"}}}')
    with pytest.raises(ValueError, match="credential"):
        sync.get_config()


def test_older_controller_keeps_compatibility_without_logging_credentials(tmp_path, monkeypatch, caplog):
    sync = FileSync(endpoint="http://minio:9000", access_key="test", secret_key="test",
                    bucket="test", worker_name="worker", local_dir=tmp_path)
    compatibility = {"channels": {"matrix": {"accessToken": "legacy-secret-test-token"}}}
    monkeypatch.setattr(sync, "_cat", lambda key: json.dumps(compatibility) if key.endswith("openclaw.json") else None)
    with caplog.at_level("INFO"):
        assert sync.get_config() == compatibility
    assert "legacy-secret-test-token" not in caplog.text


def test_standard_transport_retains_legacy_model_when_no_managed_model_is_declared(tmp_path, monkeypatch):
    sync = FileSync(endpoint="http://minio:9000", access_key="test", secret_key="test",
                    bucket="test", worker_name="worker", local_dir=tmp_path)
    record = {"member": {"runtime": "hermes", "matrixUserId": "@worker:matrix.test"},
              "matrix": {"transport": {"homeserver": "https://matrix.test"}},
              "credentials": {"matrixTokenEnv": "WORKER_BOUND_MATRIX_TOKEN"}}
    compatibility = {"models": {"providers": {"gateway": {"models": [{"id": "default-model"}]}}}}
    monkeypatch.setenv("WORKER_BOUND_MATRIX_TOKEN", "new-test-token")
    monkeypatch.setattr(sync, "_cat", lambda key: yaml.safe_dump(record) if key.endswith("runtime.yaml") else json.dumps(compatibility))
    config = sync.get_config()
    assert config["models"] == compatibility["models"]
    assert config["channels"]["matrix"]["accessToken"] == "new-test-token"


def test_projected_task_scope_wins_over_old_local_team_context(tmp_path):
    sync = FileSync(endpoint="http://minio:9000", access_key="test", secret_key="test",
                    bucket="test", worker_name="worker", local_dir=tmp_path)
    (tmp_path / "AGENTS.md").write_text("**Team**: old-team")
    runtime = tmp_path / "runtime/runtime.yaml"
    runtime.parent.mkdir()
    runtime.write_text('member: {runtime: hermes}\nteam: {name: new-team}\n')
    assert sync._get_team_id() == "new-team"
    runtime.write_text('member: {runtime: hermes}\n')
    assert sync._get_team_id() is None


def test_manager_role_projection_does_not_overwrite_personality(tmp_path, monkeypatch):
    monkeypatch.setenv("AGENTTEAMS_AGENT_ROLE", "manager")
    sync = FileSync(endpoint="http://minio:9000", access_key="test", secret_key="test",
                    bucket="test", worker_name="manager", local_dir=tmp_path)
    (tmp_path / ".hermes").mkdir()
    (tmp_path / "SOUL.md").write_text("Your personality.")
    os.utime(tmp_path / "SOUL.md", (1, 1))
    (tmp_path / ".hermes/SOUL.md").write_text(
        "<!-- agentteams-manager-role-start -->\nYou are the Manager.\n"
        "<!-- agentteams-manager-role-end -->\n\nYour updated personality.")
    monkeypatch.setattr(sync, "_ensure_alias", lambda: None)
    monkeypatch.setattr(sync, "_cat", lambda key: "")
    monkeypatch.setattr(sync_module, "_mc", lambda *args, **kwargs: None)
    sync_module.push_local(sync)
    assert (tmp_path / "SOUL.md").read_text() == "Your updated personality."


@pytest.mark.parametrize("prefix_missing", [False, True])
def test_mirror_all_restores_standard_record_without_compatibility(tmp_path, monkeypatch, prefix_missing):
    sync = FileSync(endpoint="http://minio:9000", access_key="test", secret_key="test",
                    bucket="test", worker_name="worker", local_dir=tmp_path)
    record = yaml.safe_dump({"member": {"runtime": "hermes"}, "team": {"name": "new-team"}})
    monkeypatch.setattr(sync, "_ensure_alias", lambda: None)
    monkeypatch.setattr(sync, "_cat", lambda key: record if key.endswith("runtime/runtime.yaml") else None)
    mirrors = []

    def mirror(*args, **_kwargs):
        mirrors.append(args)
        if args[1].endswith("/agents/worker/"):
            if prefix_missing:
                raise subprocess.CalledProcessError(1, args, stderr="Object does not exist")
            path = tmp_path / "runtime/runtime.yaml"
            path.parent.mkdir()
            path.write_text(record)
        return subprocess.CompletedProcess(args, 0, stdout="", stderr="")

    monkeypatch.setattr(sync_module, "_mc", mirror)
    sync.mirror_all()
    assert not (tmp_path / "openclaw.json").exists()
    assert (tmp_path / "runtime/runtime.yaml").read_text() == record
    assert any(command[1].endswith("/teams/new-team/shared/") for command in mirrors)


def test_mirror_all_falls_back_to_startup_files_when_prefix_missing(
    tmp_path, monkeypatch
) -> None:
    sync = FileSync(
        endpoint="http://minio:9000",
        access_key="minio",
        secret_key="password",
        bucket="agentteams-storage",
        worker_name="dag-team-dev",
        local_dir=tmp_path / "worker",
    )
    commands = []

    monkeypatch.setattr(sync, "_ensure_alias", lambda: None)

    def fake_mc(*args, **_kwargs):
        commands.append(args)
        if args[0] == "mirror" and args[1].endswith("/agents/dag-team-dev/"):
            raise subprocess.CalledProcessError(
                1,
                args,
                output="",
                stderr="mc.bin: <ERROR> Object does not exist.",
            )
        if args[0] == "cat" and args[1].endswith(
            "/agents/dag-team-dev/openclaw.json"
        ):
            return subprocess.CompletedProcess(
                args,
                0,
                stdout='{"team_id":"dag-team"}',
                stderr="",
            )
        if args[0] == "cat":
            raise subprocess.CalledProcessError(
                1,
                args,
                output="",
                stderr="mc.bin: <ERROR> Object does not exist.",
            )
        return subprocess.CompletedProcess(args, 0, stdout="", stderr="")

    monkeypatch.setattr(sync_module, "_mc", fake_mc)

    sync.mirror_all()

    assert json.loads((sync.local_dir / "openclaw.json").read_text()) == {
        "team_id": "dag-team"
    }
    assert (
        "mirror",
        "agentteams/agentteams-storage/teams/dag-team/shared/",
        f"{sync.local_dir / 'shared'}/",
        "--overwrite",
    ) in commands


def test_push_local_persists_user_plugins_but_skips_image_overlay(
    tmp_path, monkeypatch
) -> None:
    """Runtime plugins are worker state; only the image overlay is excluded."""
    sync = FileSync(
        endpoint="http://minio:9000",
        access_key="minio",
        secret_key="password",
        bucket="agentteams-storage",
        worker_name="worker-a",
        local_dir=tmp_path / "worker",
    )
    plugins = sync.local_dir / ".hermes" / "plugins"
    image_plugin = plugins / "platforms" / "matrix" / "adapter.py"
    custom_plugin = plugins / "custom-review" / "plugin.yaml"
    custom_adapter = plugins / "custom-review" / "adapter.py"
    for path, content in (
        (image_plugin, "image-owned"),
        (custom_plugin, "name: custom-review"),
        (custom_adapter, "def register(ctx): pass"),
    ):
        path.parent.mkdir(parents=True, exist_ok=True)
        path.write_text(content, encoding="utf-8")

    commands = []
    monkeypatch.setattr(sync, "_ensure_alias", lambda: None)
    monkeypatch.setattr(sync, "_cat", lambda _key: None)

    def fake_mc(*args, **_kwargs):
        commands.append(args)
        return subprocess.CompletedProcess(args, 0, stdout="", stderr="")

    monkeypatch.setattr(sync_module, "_mc", fake_mc)

    pushed = push_local(sync)

    assert ".hermes/plugins/custom-review/plugin.yaml" in pushed
    assert ".hermes/plugins/custom-review/adapter.py" in pushed
    assert ".hermes/plugins/platforms/matrix/adapter.py" not in pushed


@pytest.mark.parametrize("relative_path", [".hermes/platforms/matrix/store/crypto.db", ".hermes/state.db"])
def test_push_local_preserves_durable_database_with_uncheckpointed_wal(tmp_path, monkeypatch, relative_path):
    """Recreation needs committed native session/crypto rows, including WAL."""
    import os
    import shutil
    import sqlite3

    sync = FileSync(endpoint="http://minio:9000", access_key="minio", secret_key="synthetic",
                    bucket="agentteams-storage", worker_name="worker-a", local_dir=tmp_path / "worker")
    db = sync.local_dir / relative_path
    db.parent.mkdir(parents=True)
    remote = tmp_path / "restored-crypto.db"
    connection = sqlite3.connect(db)
    try:
        connection.execute("PRAGMA journal_mode=WAL")
        connection.execute("PRAGMA wal_autocheckpoint=0")
        connection.execute("CREATE TABLE sessions (id TEXT PRIMARY KEY)")
        connection.commit()
        connection.execute("PRAGMA wal_checkpoint(TRUNCATE)")
        connection.execute("INSERT INTO sessions VALUES ('persisted-session')")
        connection.commit()
        # The main DB is old; a committed WAL change alone must trigger backup.
        os.utime(db, (1, 1))
        monkeypatch.setattr(sync, "_ensure_alias", lambda: None)
        monkeypatch.setattr(sync, "_cat", lambda _key: None)
        copies = []
        def fake_mc(*args, **_kwargs):
            copies.append(args)
            if args[0] == "cp":
                shutil.copyfile(args[1], remote)
            return subprocess.CompletedProcess(args, 0, stdout="", stderr="")
        monkeypatch.setattr(sync_module, "_mc", fake_mc)
        pushed = push_local(sync, since=2)
        assert relative_path in pushed
        assert len(copies) == 1
        with sqlite3.connect(remote) as restored:
            assert restored.execute("PRAGMA integrity_check").fetchone()[0] == "ok"
            assert restored.execute("SELECT id FROM sessions").fetchone() == ("persisted-session",)
    finally:
        connection.close()
