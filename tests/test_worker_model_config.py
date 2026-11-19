"""Worker model authority must survive a missing or stale native directory."""
import shutil

import pytest
import yaml

from hermes_worker.bridge import bridge_openclaw_to_hermes


@pytest.mark.parametrize("model_input", [["text"], ["text", "image"]])
@pytest.mark.parametrize("gateway_url", ["https://worker-gateway.test", "https://worker-gateway.test/v1", "https://worker-gateway.test/v1/"])
def test_worker_model_rebuild_ignores_stale_compatibility_and_native_config(tmp_path, monkeypatch, model_input, gateway_url):
    runtime = tmp_path / "runtime/runtime.yaml"
    runtime.parent.mkdir()
    runtime.write_text(yaml.safe_dump({
        "member": {"runtime": "hermes", "role": "worker"},
        "desired": {"model": {"providerId": "agentteams-gateway", "model": "worker-model",
                               "gatewayUrl": gateway_url, "input": model_input}},
        "credentials": {"gatewayKeyEnv": "WORKER_BOUND_GATEWAY_KEY"},
    }))
    monkeypatch.setenv("WORKER_BOUND_GATEWAY_KEY", "worker-bound-test-key")
    native = tmp_path / ".hermes"
    native.mkdir()
    (native / "config.yaml").write_text(yaml.safe_dump({"model": {"default": "stale-native"},
        "auxiliary": {"vision": {"api_key": "old-vision-test-key", "model": "old-vision"}}}))
    (native / ".env").write_text("OPENAI_API_KEY=stale-native-test-key\n")
    compatibility = {"models": {"providers": {"old": {"baseUrl": "https://old-gateway.test/v1",
                       "apiKey": "old-test-key", "models": [{"id": "stale-compatibility"}]}}}}
    compatibility["env"] = {"OPENAI_API_KEY": "old-test-key", "HERMES_DEFAULT_MODEL": "old-env-model"}

    for attempt in range(2):
        if attempt:
            shutil.rmtree(native)
        bridge_openclaw_to_hermes(compatibility, native)
        model = yaml.safe_load((native / "config.yaml").read_text())["model"]
        assert model["default"] == "worker-model"
        assert model["base_url"] == "https://worker-gateway.test/v1"
        assert "old-vision-test-key" not in (native / "config.yaml").read_text()
        environment = (native / ".env").read_text()
        assert "worker-bound-test-key" in environment
        assert "old-test-key" not in environment and "stale-native-test-key" not in environment
        assert compatibility["models"]["providers"]["old"]["models"][0]["id"] == "stale-compatibility"


def test_worker_model_without_bound_credential_does_not_reuse_old_secret(tmp_path, monkeypatch):
    runtime = tmp_path / "runtime/runtime.yaml"
    runtime.parent.mkdir()
    runtime.write_text(yaml.safe_dump({
        "member": {"runtime": "hermes", "role": "worker"},
        "desired": {"model": {"model": "worker-model", "gatewayUrl": "https://worker-gateway.test/v1"}},
        "credentials": {"gatewayKeyEnv": "WORKER_BOUND_GATEWAY_KEY"},
    }))
    monkeypatch.delenv("WORKER_BOUND_GATEWAY_KEY", raising=False)
    compatibility = {"models": {"providers": {"old": {"apiKey": "old-test-key", "models": [{"id": "old"}]}}}}
    with pytest.raises(ValueError, match="credential"):
        bridge_openclaw_to_hermes(compatibility, tmp_path / ".hermes")
    assert not (tmp_path / ".hermes/.env").exists()
