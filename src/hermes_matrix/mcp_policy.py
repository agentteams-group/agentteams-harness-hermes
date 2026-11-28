"""Apply Core MCP policy through Hermes' native one-call approval transport."""
from functools import wraps
import os
from pathlib import Path

import yaml

from runtime_control import mcp_approval


def install():
    from hermes_matrix.mcp_transport_outcomes import install as install_transport_outcomes
    install_transport_outcomes()
    from hermes_matrix.mcp_outcomes import install as install_outcomes
    install_outcomes()
    from tools import mcp_tool_handlers as native
    original = native._trust_gate_check
    if getattr(original, '_agentteams_policy', False):
        return

    @wraps(original)
    def check(server_name, tool_name, booked=None, args=None):
        path = os.getenv('TEAMHARNESS_RUNTIME_CONFIG')
        runtime = yaml.safe_load(Path(path).read_text()) if path else {}
        mode = mcp_approval(runtime, server_name)
        if mode == 'ask':
            from tools.approval_prompt import request_elicitation_consent
            decision = request_elicitation_consent(
                server_name + '.' + tool_name + '\n' + native._format_trust_gate_arguments(args or {}),
                'AgentTeams MCP approval policy', surface='agentteams-mcp/' + server_name,
                title='MCP tool approval', remember_key=None)
            if decision != 'accept':
                return native.tool_error('MCP operation was not approved')
        return original(server_name, tool_name, booked, args)

    check._agentteams_policy = True
    native._trust_gate_check = check
