"""Exercise a real old Worker, MinIO restore, Matrix E2EE and session continuation.

The model endpoint is deterministic; Matrix, runtime, session DB and Olm/Megolm
stores are real. Only resources created by this invocation are removed.
"""
from __future__ import annotations

import argparse
import asyncio
import hashlib
import json
import os
from pathlib import Path
import re
import subprocess
import tempfile
import time
import urllib.request
import uuid

PYTHON = "/opt/venv/hermes/bin/python"
WORKSPACE = "/root/agentteams-fs/agents/upgrade-worker"


def docker(*args):
    return subprocess.check_output(["docker", *args], text=True, stderr=subprocess.STDOUT).strip()


def request(url, data=None, token=None, method=None):
    headers = {"Content-Type": "application/json"}
    if token:
        headers["Authorization"] = "Bearer " + token
    req = urllib.request.Request(url, data=None if data is None else json.dumps(data).encode(),
                                 headers=headers, method=method)
    with urllib.request.urlopen(req, timeout=20) as response:
        return json.load(response)


def wait(check, label, timeout=180):
    deadline = time.monotonic() + timeout
    last = None
    while time.monotonic() < deadline:
        try:
            value = check()
            if value:
                return value
        except Exception as exc:
            last = exc
        time.sleep(2)
    raise AssertionError(f"Timed out: {label}; last error: {last}")


async def fixture():
    from aiohttp import web
    settings = json.loads(Path("/fixture.json").read_text())
    os.environ.update({
        "HERMES_HOME": "/tmp/human-hermes", "MATRIX_E2EE_MODE": "required",
        "MATRIX_REQUIRE_MENTION": "false", "MATRIX_AUTO_THREAD": "false",
        "MATRIX_DM_AUTO_THREAD": "false", "MATRIX_ALLOWED_USERS": settings["worker"]["user_id"],
    })
    from gateway.config import PlatformConfig
    from plugins.platforms.matrix.adapter import MatrixAdapter
    human = settings["human"]
    adapter = MatrixAdapter(PlatformConfig(enabled=True, token=human["access_token"], extra={
        "homeserver": settings["matrix"], "user_id": human["user_id"],
        "device_id": human["device_id"], "e2ee_mode": "required",
    }))
    events, calls = [], []

    async def receive(event):
        events.append({"room": event.source.chat_id, "text": event.text})

    adapter.set_message_handler(receive)
    assert await adapter.connect(), "human Matrix client failed to connect"

    async def status(_):
        return web.json_response({"events": events, "calls": calls})

    async def send(req):
        payload = await req.json()
        sent = await adapter.send(payload["room"], payload["text"])
        assert sent.success, sent.error
        return web.json_response({"event_id": sent.message_id})

    async def model(req):
        payload = await req.json()
        messages = payload.get("messages", [])
        users = "\n".join(str(m.get("content", "")) for m in messages if m.get("role") == "user")
        seed = re.search(r"UPGRADE_SEED:([a-z0-9]+)", users)
        nonce = re.findall(r"UPGRADE_RECALL:([a-z0-9]+)", users)
        text = ("RESUMED:" + seed[1] + ":" + nonce[-1]) if nonce and seed else (
            "SEEDED:" + seed[1] if seed else "MISSING_UPGRADE_HISTORY")
        calls.append({"reply": text, "history_found": bool(seed), "messages": len(messages)})
        common = {"id": "chatcmpl-upgrade", "created": int(time.time()), "model": payload.get("model", "upgrade-model")}
        if payload.get("stream"):
            response = web.StreamResponse(headers={"Content-Type": "text/event-stream"})
            await response.prepare(req)
            for delta, reason in (({"role": "assistant", "content": text}, None), ({}, "stop")):
                chunk = {**common, "object": "chat.completion.chunk", "choices": [{"index": 0, "delta": delta, "finish_reason": reason}]}
                await response.write(("data: " + json.dumps(chunk) + "\n\n").encode())
            await response.write(b"data: [DONE]\n\n")
            await response.write_eof()
            return response
        return web.json_response({**common, "object": "chat.completion", "choices": [{
            "index": 0, "message": {"role": "assistant", "content": text}, "finish_reason": "stop",
        }], "usage": {"prompt_tokens": 100, "completion_tokens": 10, "total_tokens": 110}})

    app = web.Application()
    app.router.add_get("/status", status)
    app.router.add_post("/send", send)
    app.router.add_post("/v1/chat/completions", model)
    runner = web.AppRunner(app)
    await runner.setup()
    await web.TCPSite(runner, "0.0.0.0", 8088).start()
    await asyncio.Event().wait()


def snapshot():
    import sqlite3
    import yaml
    root = Path(WORKSPACE)
    cfg = json.loads((root / "openclaw.json").read_text())
    native = yaml.safe_load((root / ".hermes/config.yaml").read_text())
    with sqlite3.connect(root / ".hermes/state.db") as db:
        sessions = db.execute("SELECT id FROM sessions WHERE source='matrix' ORDER BY id").fetchall()
        messages = db.execute("SELECT session_id, role, content FROM messages ORDER BY id").fetchall()
        assert db.execute("PRAGMA integrity_check").fetchone()[0] == "ok"
    with sqlite3.connect(root / ".hermes/platforms/matrix/store/crypto.db") as db:
        device = db.execute("SELECT device_id FROM crypto_account").fetchone()[0]
        inbound = db.execute("SELECT count(*) FROM crypto_megolm_inbound_session").fetchone()[0]
        assert db.execute("PRAGMA integrity_check").fetchone()[0] == "ok"
    print(json.dumps({"device": device, "sessions": sessions, "messages": messages,
        "inbound_keys": inbound, "model": cfg["agents"]["defaults"]["model"]["primary"],
        "native_setting": native.get("upgrade_marker"),
        "plugin": hashlib.sha256((root / ".hermes/plugins/upgrade-proof/__init__.py").read_bytes()).hexdigest(),
        "workspace": (root / "upgrade-workspace.txt").read_text()}))


async def decrypt():
    from hermes_worker.worker import Worker
    Worker._load_env_file(Path(WORKSPACE) / ".hermes/.env")
    from hermes_cli.plugins import discover_plugins
    discover_plugins()
    from gateway.config import load_gateway_config
    from hermes_matrix.overlay_adapter import MatrixAdapter
    config = load_gateway_config()
    matrix = next(v for k, v in config.platforms.items() if str(getattr(k, "value", k)) == "matrix")
    adapter = MatrixAdapter(matrix)
    assert await adapter.connect(), "restored Matrix client failed to connect"
    try:
        old = json.loads(os.environ["OLD_ENCRYPTED_EVENT"])
        event = await adapter._client.get_event(old["room"], old["event_id"])
        assert str(event.type) == "m.room.encrypted", event.type
        clear = await adapter._client.crypto.decrypt_megolm_event(event)
        assert old["text"] in clear.content.body, clear.content.body
        print("PASS: restored Megolm keys decrypt the pre-upgrade encrypted event")
    finally:
        await adapter.disconnect()


def run(args):
    prefix = "hermes-upgrade-" + uuid.uuid4().hex[:8]
    network = prefix + "-net"
    names = {name: prefix + "-" + name for name in ("matrix", "minio", "fixture", "old", "new")}
    evidence = Path(args.evidence).resolve()
    evidence.mkdir(parents=True, exist_ok=True)
    script = str(Path(__file__).resolve())
    created = []
    try:
        docker("network", "create", network)

        def start(name, image, *options, command=()):
            result = docker("run", "-d", "--name", names[name], "--network", network, *options, image, *command)
            created.append(names[name])
            return result

        start("matrix", args.infra_image, "-p", "127.0.0.1::6167",
              "-e", "AGENTTEAMS_MATRIX_DOMAIN=upgrade.test", "-e", "AGENTTEAMS_REGISTRATION_TOKEN=upgrade-test-only",
              "--entrypoint", "/opt/agentteams/scripts/init/start-tuwunel.sh")
        start("minio", args.infra_image, "-e", "AGENTTEAMS_MINIO_USER=upgrade-test",
              "-e", "AGENTTEAMS_MINIO_PASSWORD=upgrade-test-only", "--entrypoint", "/opt/agentteams/scripts/init/start-minio.sh")
        matrix_url = "http://127.0.0.1:" + docker("port", names["matrix"], "6167/tcp").rsplit(":", 1)[1]
        matrix = "http://" + names["matrix"] + ":6167"
        wait(lambda: request(matrix_url + "/_matrix/client/versions"), "Matrix startup")

        def api(path, data=None, token=None, method=None):
            return request(matrix_url + "/_matrix/client/v3/" + path, data, token, method)

        identities = {}
        for name in ("upgrade-worker", "upgrade-human"):
            identities[name] = api("register", {"username": name, "password": "upgrade-test-only",
                "auth": {"type": "m.login.registration_token", "token": "upgrade-test-only"}})
        worker, human = identities.values()
        rooms = []
        for encrypted in (False, True):
            room = api("createRoom", {"preset": "private_chat", "invite": [worker["user_id"]],
                "initial_state": [{"type": "m.room.encryption", "state_key": "", "content": {
                    "algorithm": "m.megolm.v1.aes-sha2"}}] if encrypted else []}, human["access_token"])["room_id"]
            rooms.append(room)

        with tempfile.TemporaryDirectory(prefix="hermes-upgrade-") as directory:
            state = Path(directory)
            (state / "fixture.json").write_text(json.dumps({"matrix": matrix, "worker": worker, "human": human}))
            start("fixture", args.old_image, "-p", "127.0.0.1::8088", "-v", script + ":/upgrade.py:ro",
                "-v", str(state / "fixture.json") + ":/fixture.json:ro", "--entrypoint", PYTHON,
                command=("/upgrade.py", "--fixture"))
            fixture_url = "http://127.0.0.1:" + docker("port", names["fixture"], "8088/tcp").rsplit(":", 1)[1]
            wait(lambda: request(fixture_url + "/status"), "encrypted human client")

            seed = state / "seed"
            (seed / ".hermes/plugins/upgrade-proof").mkdir(parents=True)
            (seed / ".hermes/plugins/upgrade-proof/__init__.py").write_text("def register(ctx):\n    pass\n")
            (seed / ".hermes/plugins/upgrade-proof/plugin.yaml").write_text("name: upgrade-proof\nversion: 1.0.0\ndescription: upgrade regression fixture\n")
            (seed / ".hermes/config.yaml").write_text("upgrade_marker: keep-native-setting\nmemory:\n  memory_enabled: false\nplugins:\n  enabled: [upgrade-proof]\n")
            (seed / "upgrade-workspace.txt").write_text("keep-workspace\n")
            (seed / "AGENTS.md").write_text("You are an upgrade test worker. Reply directly to messages. Do not call tools.\n")
            cfg = {"channels": {"matrix": {"homeserver": matrix, "userId": worker["user_id"],
                "accessToken": worker["access_token"], "deviceId": worker["device_id"], "encryption": True,
                "dm": {"policy": "allowlist", "allowFrom": [human["user_id"]]},
                "groupAllowFrom": [human["user_id"]], "requireMention": False,
                "autoThread": False}}, "models": {"providers": {"test": {
                "baseUrl": "http://" + names["fixture"] + ":8088/v1", "apiKey": "synthetic-model-key",
                "models": [{"id": "upgrade-model", "contextWindow": 131072, "input": ["text"]}]}}},
                "agents": {"defaults": {"model": {"primary": "test/upgrade-model"}}}}
            (seed / "openclaw.json").write_text(json.dumps(cfg))
            (seed / "credentials/matrix").mkdir(parents=True)
            (seed / "credentials/matrix/password").write_text("upgrade-test-only")
            mc_env = "MC_HOST_upgrade=http://upgrade-test:upgrade-test-only@" + names["minio"] + ":9000"
            docker("run", "--rm", "--network", network, "-e", mc_env, "-v", str(seed) + ":/seed:ro",
                "--entrypoint", "/bin/sh", args.old_image, "-c",
                "mc.bin mb --ignore-existing upgrade/agentteams-storage && mc.bin mirror /seed upgrade/agentteams-storage/agents/upgrade-worker --overwrite")

            def worker_start(name, image):
                return start(name, image, "-e", "HOME=" + WORKSPACE, "-e", "AGENTTEAMS_WORKER_NAME=upgrade-worker",
                    "-e", "AGENTTEAMS_FS_ENDPOINT=http://" + names["minio"] + ":9000",
                    "-e", "AGENTTEAMS_FS_ACCESS_KEY=upgrade-test", "-e", "AGENTTEAMS_FS_SECRET_KEY=upgrade-test-only",
                    "-e", "AGENTTEAMS_MATRIX_DEBUG=1",
                    "-e", "MATRIX_E2EE_MODE=required", "-e", "MATRIX_AUTO_THREAD=false", "-e", "MATRIX_DM_AUTO_THREAD=false")

            def capture(name):
                return json.loads(docker("exec", names[name], PYTHON, "/upgrade.py", "--snapshot"))

            def idle(name):
                query = "import sqlite3; c=sqlite3.connect('" + WORKSPACE + "/.hermes/state.db'); print(c.execute('SELECT count(*) FROM session_turn_leases').fetchone()[0])"
                wait(lambda: docker("exec", names[name], PYTHON, "-c", query) == "0", name + " completed turn cleanup")

            def send_and_wait(room, text, expected):
                sent = request(fixture_url + "/send", {"room": room, "text": text})
                wait(lambda: any(e["room"] == room and expected in e["text"] for e in request(fixture_url + "/status")["events"]), expected)
                return sent["event_id"]

            def ready(name):
                docker("cp", script, names[name] + ":/upgrade.py")
                started = docker("inspect", "--format", "{{.State.StartedAt}}", names[name])
                wait(lambda: "Matrix: initial sync complete" in docker("logs", "--since", started, names[name]),
                     name + " encrypted Matrix initial sync")

            worker_start("old", args.old_image)
            ready("old")
            secrets = [uuid.uuid4().hex, uuid.uuid4().hex]
            old_event = None
            for room, secret in zip(rooms, secrets):
                event = send_and_wait(room, "UPGRADE_SEED:" + secret, "SEEDED:" + secret)
                old_event = {"room": room, "event_id": event, "text": "UPGRADE_SEED:" + secret}
            idle("old")
            before = capture("old")
            assert len(before["sessions"]) == 2, before["sessions"]
            assert before["inbound_keys"] > 0
            print("PASS: v0.22.7 completed plain and encrypted conversations", flush=True)
            time.sleep(8)
            docker("stop", "-t", "20", names["old"])
            worker_start("new", args.new_image)
            ready("new")
            after = capture("new")
            print("Restored old state into the new image", flush=True)
            assert before["device"] == after["device"], "Matrix device changed on upgrade"
            assert before["sessions"] == after["sessions"], "session identities changed"
            assert all(row in after["messages"] for row in before["messages"]), "old transcript missing"
            for key in ("native_setting", "plugin", "workspace", "model"):
                assert before[key] == after[key], f"lost {key}"
            for room, secret in zip(rooms, secrets):
                nonce = uuid.uuid4().hex
                send_and_wait(room, "UPGRADE_RECALL:" + nonce, "RESUMED:" + secret + ":" + nonce)
            print("PASS: upgraded Worker continued both old sessions", flush=True)
            idle("new")
            middle = capture("new")
            time.sleep(8)
            docker("stop", "-t", "20", names["new"])
            docker("start", names["new"])
            ready("new")
            for room, secret in zip(rooms, secrets):
                nonce = uuid.uuid4().hex
                send_and_wait(room, "UPGRADE_RECALL:" + nonce, "RESUMED:" + secret + ":" + nonce)
            print("PASS: second restart continued both sessions", flush=True)
            idle("new")
            final = capture("new")
            assert final["device"] == before["device"], "second restart rotated the Matrix device"
            assert final["sessions"] == before["sessions"], "second restart changed session identities"
            assert all(row in final["messages"] for row in middle["messages"]), "second restart lost a completed turn"
            docker("stop", "-t", "20", names["new"])
            # A stopped container's workspace is copied into a separate read/decrypt probe.
            restored = state / "restored"
            docker("cp", names["new"] + ":" + WORKSPACE, str(restored))
            decryption = docker("run", "--rm", "--network", network, "-e", "HOME=" + WORKSPACE,
                "-e", "HERMES_HOME=" + WORKSPACE + "/.hermes", "-e", "OLD_ENCRYPTED_EVENT=" + json.dumps(old_event),
                "-v", str(restored) + ":" + WORKSPACE, "-v", script + ":/upgrade.py:ro",
                "--entrypoint", PYTHON, args.new_image, "/upgrade.py", "--decrypt")
            (evidence / "decryption.log").write_text(decryption)
            (evidence / "result.json").write_text(json.dumps({"old_image": args.old_image, "new_image": args.new_image,
                "pre_upgrade_encrypted_event": old_event["event_id"],
                "model": "deterministic OpenAI-compatible fixture", "before": before, "after": after, "final": final,
                "passed": ["cold MinIO restore", "native settings", "user plugin", "workspace", "session identity/transcript",
                    "Matrix device identity", "plain and encrypted continuation", "second restart", "old Megolm event decryption"]}, indent=2))
            print("PASS: old Worker -> cold v0.23.0 restore -> second restart, including encrypted history")
    finally:
        for name in reversed(created):
            logs = subprocess.run(["docker", "logs", name], capture_output=True, text=True)
            (evidence / (name + ".log")).write_text(logs.stdout + logs.stderr)
            subprocess.run(["docker", "rm", "-f", "-v", name], capture_output=True)
        subprocess.run(["docker", "network", "rm", network], capture_output=True)


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--fixture", action="store_true")
    parser.add_argument("--snapshot", action="store_true")
    parser.add_argument("--decrypt", action="store_true")
    parser.add_argument("--old-image", default="higress-registry.cn-hangzhou.cr.aliyuncs.com/agentteams/agentteams-hermes-worker:v1.2.6")
    parser.add_argument("--new-image")
    parser.add_argument("--infra-image", default="higress-registry.cn-hangzhou.cr.aliyuncs.com/agentteams/agentteams-embedded:v1.2.6")
    parser.add_argument("--evidence", default="upgrade-evidence")
    options = parser.parse_args()
    if options.fixture:
        asyncio.run(fixture())
    elif options.snapshot:
        snapshot()
    elif options.decrypt:
        asyncio.run(decrypt())
    else:
        assert options.new_image, "--new-image is required"
        run(options)
