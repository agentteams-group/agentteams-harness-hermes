"""A worker restart must retain the device owning its persisted Olm account."""
import json
import sqlite3
from contextlib import nullcontext
from types import SimpleNamespace
from unittest.mock import patch

import pytest

from hermes_worker.config import WorkerConfig
from hermes_worker.worker import Worker


@pytest.mark.parametrize("device_id,stored_device", [
    ("EXISTING-DEVICE", None), (None, None),
    ("STALE-MANAGER-DEVICE", "PERSISTED-DEVICE"),
])
def test_password_relogin_reuses_saved_device(tmp_path, device_id, stored_device):
    worker = Worker(WorkerConfig("upgrade-worker", "http://minio", "test", "test"))
    worker._hermes_home = tmp_path / ".hermes"
    worker.sync = SimpleNamespace(
        _prefix="agents/upgrade-worker", local_dir=tmp_path,
        _cat=lambda _: "synthetic-password",
    )
    matrix = {"homeserver": "http://matrix", "accessToken": "old-token", "userId": "@upgrade-worker:matrix"}
    if device_id:
        matrix["deviceId"] = device_id
    cfg = {"channels": {"matrix": matrix}}
    if stored_device:
        store = worker._hermes_home / "platforms/matrix/store/crypto.db"
        store.parent.mkdir(parents=True)
        with sqlite3.connect(store) as connection:
            connection.execute("CREATE TABLE crypto_account (account_id TEXT, device_id TEXT)")
            connection.execute("INSERT INTO crypto_account VALUES (?, ?)", (matrix["userId"], stored_device))
    expected_device = stored_device or device_id

    def login(request, timeout):
        payload = json.loads(request.data)
        if expected_device:
            assert payload["device_id"] == expected_device
        else:
            assert "device_id" not in payload
        response = SimpleNamespace(read=lambda: json.dumps({
            "access_token": "new-token", "device_id": expected_device or "FIRST-DEVICE",
        }).encode())
        return nullcontext(response)

    with patch("urllib.request.urlopen", side_effect=login):
        result = worker._matrix_relogin(cfg)
    persisted = json.loads((tmp_path / "openclaw.json").read_text())
    assert result == persisted
    assert result["channels"]["matrix"]["deviceId"] == (expected_device or "FIRST-DEVICE")
    assert result["channels"]["matrix"]["accessToken"] == "new-token"
