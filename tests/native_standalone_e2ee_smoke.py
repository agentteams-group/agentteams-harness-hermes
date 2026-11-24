"""Run the registered cron sender against the installed native Hermes SDK."""
import asyncio
import importlib.util
import json
import os
import threading
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from pathlib import Path
from types import SimpleNamespace

from plugins.platforms.matrix import adapter as native


def main():
    requests = []

    class Handler(BaseHTTPRequestHandler):
        def do_PUT(self):
            requests.append((self.path, json.loads(self.rfile.read(int(self.headers["Content-Length"])))))
            data = b'{"event_id":"$sent"}'
            self.send_response(200)
            self.send_header("Content-Type", "application/json")
            self.send_header("Content-Length", str(len(data)))
            self.end_headers()
            self.wfile.write(data)

        def log_message(self, *args):
            pass

    server = ThreadingHTTPServer(("127.0.0.1", 0), Handler)
    thread = threading.Thread(target=server.serve_forever, daemon=True)
    thread.start()
    registration = {}
    path = Path(__file__).resolve().parents[1] / "plugins/platforms/matrix/adapter.py"
    spec = importlib.util.spec_from_file_location("candidate_matrix_plugin", path)
    plugin = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(plugin)
    plugin.register(SimpleNamespace(register_platform=lambda **kwargs: registration.update(kwargs)))
    sender = registration["standalone_sender_fn"]
    assert sender is not native._standalone_send
    homeserver = f"http://127.0.0.1:{server.server_port}"

    async def check():
        os.environ.pop("MATRIX_E2EE_MODE", None)
        os.environ.pop("MATRIX_ENCRYPTION", None)
        for settings in ({"e2ee_mode": "required"}, {"encryption": True}):
            config = SimpleNamespace(token="synthetic-token", extra={"homeserver": homeserver, **settings})
            result = await sender(config, "!room:test", "must stay private")
            assert not result.get("success") and result.get("error"), result
            assert requests == []
        os.environ["MATRIX_E2EE_MODE"] = "required"
        config = SimpleNamespace(token="synthetic-token", extra={"homeserver": homeserver})
        result = await sender(config, "!room:test", "env-required")
        assert not result.get("success") and result.get("error"), result
        assert requests == []
        os.environ.pop("MATRIX_E2EE_MODE")
        config.extra["e2ee_mode"] = "off"
        result = await sender(config, "!room:test", "plain control")
        assert result["success"] is True, result
        assert len(requests) == 1
        assert "/send/m.room.message/" in requests[0][0]
        assert requests[0][1]["body"] == "plain control"

    try:
        asyncio.run(check())
    finally:
        server.shutdown()
        server.server_close()
        thread.join()
    print("Native cron E2EE-required guard and plaintext wire control passed")


if __name__ == "__main__":
    main()
