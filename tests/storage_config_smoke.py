"""Rebuild native configuration in two containers from real Worker S3 objects.

No Matrix/model connection or Task execution is performed. The isolated MinIO
uses synthetic root credentials; production Worker IAM policy is not tested.
"""
from __future__ import annotations

import argparse
import json
import os
from pathlib import Path
import subprocess
import tempfile
import time
import urllib.request
import uuid


def consume(endpoint: str, generation: int) -> None:
    import yaml
    from hermes_worker.bridge import bridge_openclaw_to_hermes
    from hermes_worker.sync import FileSync, _mc
    from hermes_worker.worker import Worker

    for _ in range(60):
        try:
            with urllib.request.urlopen(endpoint + "/minio/health/ready", timeout=2):
                break
        except OSError:
            time.sleep(1)
    else:
        raise AssertionError("MinIO did not become ready")

    with tempfile.TemporaryDirectory() as temp:
        workspace = Path(temp)
        home = workspace / ".hermes"
        os.environ.update({"HERMES_HOME": str(home), "AGENTTEAMS_AGENT_ROLE": "worker",
            "STORAGE_SMOKE_MATRIX_TOKEN": f"synthetic-token-{generation}",
            "STORAGE_SMOKE_GATEWAY_KEY": f"synthetic-key-{generation}"})
        record = {
            "member": {"runtime": "hermes", "role": "worker", "runtimeName": "config-worker",
                       "matrixUserId": "@config-worker:matrix.invalid", "personalRoomId": "!home:matrix.invalid"},
            "matrix": {"transport": {"homeserver": "http://matrix.invalid:8008", "encryption": False,
                "dm": {"policy": "allowlist", "allowFrom": ["@human:matrix.invalid"]}}},
            "desired": {"model": {"model": f"worker-model-{generation}", "gatewayUrl": "http://gateway.invalid"}},
            "credentials": {"matrixTokenEnv": "STORAGE_SMOKE_MATRIX_TOKEN", "gatewayKeyEnv": "STORAGE_SMOKE_GATEWAY_KEY"},
        }
        sync = FileSync(endpoint=endpoint, access_key="storage-smoke", secret_key="storage-smoke-only",
                        bucket="storage-smoke", worker_name="config-worker", local_dir=workspace)
        sync._ensure_alias()
        _mc("mb", "--ignore-existing", "agentteams/storage-smoke")
        seed = workspace / "seed.yaml"
        seed.write_text(yaml.safe_dump(record))
        _mc("cp", str(seed), "agentteams/storage-smoke/agents/config-worker/runtime/runtime.yaml")
        if generation == 1:
            seed.write_text("Your persisted Worker personality.")
            _mc("cp", str(seed), "agentteams/storage-smoke/agents/config-worker/SOUL.md")
            seed.write_text("Persisted shared artifact.")
            _mc("cp", str(seed), "agentteams/storage-smoke/shared/artifact.txt")
        seed.unlink()
        if generation == 1:
            home.mkdir()
            (home / "config.yaml").write_text(yaml.safe_dump({"model": {"default": "old-native-model"}}))
            (home / ".env").write_text("OPENAI_API_KEY=old-native-key\n")
        else:
            assert not home.exists(), "replacement container must start without native state"
        # These are the real startup calls, without patching mc or _cat.
        sync.mirror_all()
        projected = sync.get_config()
        assert not (workspace / "openclaw.json").exists()
        assert (workspace / "SOUL.md").read_text() == "Your persisted Worker personality."
        assert (workspace / "shared/artifact.txt").read_text() == "Persisted shared artifact."
        bridge_openclaw_to_hermes(projected, home)
        Worker._load_env_file(home / ".env")
        from hermes_cli.config import load_config
        effective = load_config()
        assert effective["model"]["default"] == f"worker-model-{generation}"
        assert effective["model"]["base_url"] == "http://gateway.invalid/v1"
        assert os.environ["OPENAI_API_KEY"] == f"synthetic-key-{generation}"
        assert os.environ["MATRIX_ACCESS_TOKEN"] == f"synthetic-token-{generation}"
        assert os.environ["MATRIX_USER_ID"] == "@config-worker:matrix.invalid"
        assert "old-native-key" not in (home / ".env").read_text()
        print(json.dumps({"passed": True, "configuration_revision": generation, "model": effective["model"]["default"],
                          "storage": "real MinIO and mc", "matrix_connected": False, "task_execution": False}))


def run(args) -> None:
    def docker(*command):
        return subprocess.check_output(["docker", *command], text=True).strip()

    prefix = "hermes-config-" + uuid.uuid4().hex[:8]
    network, storage = prefix + "-net", prefix + "-storage"
    created_network = created_storage = False
    try:
        docker("network", "create", network)
        created_network = True
        docker("run", "-d", "--name", storage, "--network", network, "--tmpfs", "/data",
               "-e", "MINIO_ROOT_USER=storage-smoke", "-e", "MINIO_ROOT_PASSWORD=storage-smoke-only",
               "--entrypoint", "minio", args.storage_image, "server", "/data", "--address", ":9000")
        created_storage = True
        for generation in (1, 2):
            print(docker("run", "--rm", "--network", network,
                         "--entrypoint", "/opt/venv/hermes/bin/python",
                         "-v", f"{Path(__file__).resolve()}:/storage_config_smoke.py:ro", args.image,
                         "/storage_config_smoke.py", "--endpoint", f"http://{storage}:9000",
                         "--generation", str(generation)))
    finally:
        if created_storage:
            docker("rm", "-f", storage)
        if created_network:
            docker("network", "rm", network)


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--image", default="ghcr.io/agentteams-group/agentteams-harness-hermes:ci")
    parser.add_argument("--storage-image", default="higress-registry.cn-hangzhou.cr.aliyuncs.com/higress/minio:20260216")
    parser.add_argument("--endpoint")
    parser.add_argument("--generation", type=int, choices=(1, 2))
    options = parser.parse_args()
    if options.endpoint:
        consume(options.endpoint, options.generation)
    else:
        run(options)
