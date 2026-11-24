"""Tests for the AgentTeams Matrix user-plugin bridge."""

from __future__ import annotations

import importlib.util
import asyncio
import sys
import types
from pathlib import Path


def _load_plugin_module():
    plugin_path = Path(__file__).resolve().parents[1] / "plugins" / "platforms" / "matrix" / "adapter.py"
    spec = importlib.util.spec_from_file_location("agentteams_matrix_plugin", plugin_path)
    assert spec is not None and spec.loader is not None
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


def test_register_swaps_bundled_adapter_and_delegates(monkeypatch) -> None:
    native_adapter_class = object()
    overlay_adapter_class = object()
    calls: list[tuple[str, object]] = []

    native = types.ModuleType("plugins.platforms.matrix.adapter")
    native.MatrixAdapter = native_adapter_class
    native._standalone_send = object()

    def native_register(ctx):
        calls.append(("register", native.MatrixAdapter))

    native.register = native_register

    overlay = types.ModuleType("hermes_matrix.overlay_adapter")
    overlay.MatrixAdapter = overlay_adapter_class

    monkeypatch.setitem(sys.modules, "plugins", types.ModuleType("plugins"))
    monkeypatch.setitem(sys.modules, "plugins.platforms", types.ModuleType("plugins.platforms"))
    monkeypatch.setitem(sys.modules, "plugins.platforms.matrix", types.ModuleType("plugins.platforms.matrix"))
    monkeypatch.setitem(sys.modules, "plugins.platforms.matrix.adapter", native)
    monkeypatch.setitem(sys.modules, "hermes_matrix", types.ModuleType("hermes_matrix"))
    monkeypatch.setitem(sys.modules, "hermes_matrix.overlay_adapter", overlay)

    plugin = _load_plugin_module()
    plugin.register(object())

    assert calls == [("register", overlay_adapter_class)]
    assert native.MatrixAdapter is overlay_adapter_class


def test_registered_standalone_sender_refuses_required_encryption(monkeypatch):
    from types import SimpleNamespace
    from unittest.mock import AsyncMock

    sender = AsyncMock(return_value={"success": True, "message_id": "$plain"})
    native = SimpleNamespace(
        MatrixAdapter=object(), _standalone_send=sender,
        _resolve_e2ee_mode=lambda extra: extra.get("e2ee_mode", "required"),
        send_error=lambda message: {"success": False, "error": message},
    )
    registrations = []
    native.register = lambda ctx: registrations.append(native._standalone_send)
    matrix = types.ModuleType("plugins.platforms.matrix")
    matrix.adapter = native
    overlay = types.ModuleType("hermes_matrix.overlay_adapter")
    overlay.MatrixAdapter = object()
    monkeypatch.setitem(sys.modules, "plugins", types.ModuleType("plugins"))
    monkeypatch.setitem(sys.modules, "plugins.platforms", types.ModuleType("plugins.platforms"))
    monkeypatch.setitem(sys.modules, "plugins.platforms.matrix", matrix)
    monkeypatch.setitem(sys.modules, "hermes_matrix.overlay_adapter", overlay)
    plugin = _load_plugin_module()
    plugin.register(object())
    assert native._standalone_send is sender

    async def scenario():
        guarded = registrations[0]
        for mode in ("required", None):
            result = await guarded(SimpleNamespace(extra={"e2ee_mode": mode} if mode else {}), "!room:test", "secret")
            assert result["success"] is False
            assert "encrypted gateway" in result["error"]
        sender.assert_not_awaited()
        result = await guarded(SimpleNamespace(extra={"e2ee_mode": "off"}), "!room:test", "plain", thread_id="thread")
        assert result["success"] is True
        sender.assert_awaited_once_with(SimpleNamespace(extra={"e2ee_mode": "off"}), "!room:test", "plain", thread_id="thread")

    asyncio.run(scenario())


def test_matrix_plugin_package_exports_register(monkeypatch):
    """Hermes loads directory plugins through __init__.py, not adapter.py."""
    import importlib.util
    import sys

    package = Path(__file__).resolve().parents[1] / "plugins" / "platforms" / "matrix"
    spec = importlib.util.spec_from_file_location(
        "agentteams_matrix_plugin_test", package / "__init__.py",
        submodule_search_locations=[str(package)],
    )
    assert spec is not None and spec.loader is not None
    module = importlib.util.module_from_spec(spec)
    monkeypatch.setitem(sys.modules, spec.name, module)
    spec.loader.exec_module(module)
    assert callable(module.register)
