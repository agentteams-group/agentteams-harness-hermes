"""AgentTeams Matrix platform plugin.

hermes-agent moved Matrix from ``gateway/platforms/matrix.py`` to the
bundled ``plugins/platforms/matrix`` plugin.  At container startup this
plugin is installed under the same path-derived key (``platforms/matrix``) and
manifest name, so Hermes' user-over-bundled precedence replaces the stock
registration.  Its register() then delegates to the bundled register() with
the AgentTeams adapter subclass swapped in.

Keeping the swap at register() time avoids copying the bundled plugin's large
registration metadata (dashboard card, env bridge, send-target grammar, setup
flow) into this overlay.
"""
from __future__ import annotations


def register(ctx) -> None:
    """Register the AgentTeams Matrix adapter under the stock platform name."""
    from plugins.platforms.matrix import adapter as native
    from hermes_matrix.overlay_adapter import MatrixAdapter

    native.MatrixAdapter = MatrixAdapter
    sender = native._standalone_send

    async def standalone_send(pconfig, chat_id, message, **kwargs):
        # The standalone HTTP sender has no crypto store. Never downgrade a
        # required-E2EE cron/notification to a plaintext m.room.message.
        if native._resolve_e2ee_mode(getattr(pconfig, "extra", {}) or {}) == "required":
            return native.send_error(
                "Matrix standalone delivery requires the running encrypted gateway when E2EE is required"
            )
        return await sender(pconfig, chat_id, message, **kwargs)

    native._standalone_send = standalone_send
    try:
        native.register(ctx)
    finally:
        native._standalone_send = sender
