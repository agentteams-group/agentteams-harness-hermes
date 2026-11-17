"""Installed SDK MCP handshake; no model or Task execution is claimed."""
import asyncio
import os
from pathlib import Path
import tempfile

from mcp import ClientSession, StdioServerParameters
from mcp.client.stdio import stdio_client
import yaml

from hermes_worker.bridge import bridge_openclaw_to_hermes


async def check(workspace):
    runtime = workspace / "runtime/runtime.yaml"
    runtime.parent.mkdir()
    runtime.write_text(yaml.safe_dump({"member": {"runtime": "hermes", "role": "worker"}, "team": {"name": "team"}}))
    os.environ["AGENTTEAMS_PROCESS_ID"] = "fixture-start"
    home = workspace / ".hermes"
    os.environ["HERMES_HOME"] = str(home)
    bridge_openclaw_to_hermes({}, home)
    server = yaml.safe_load((home / "config.yaml").read_text())["mcp_servers"]["teamharness"]
    params = StdioServerParameters(**server)
    async with stdio_client(params) as (read, write), ClientSession(read, write) as session:
        await session.initialize()
        tools = await session.list_tools()
        taskflow = next(tool for tool in tools.tools if tool.name == "taskflow")
        assert "checkpoint_task" in taskflow.input_schema["properties"]["action"]["enum"]
    print("Hermes installed MCP SDK discovered native Task/checkpoint tools with projected scope (no model/Task execution)")
    return server


if __name__ == "__main__":
    with tempfile.TemporaryDirectory() as directory:
        server = asyncio.run(check(Path(directory)))
        from tools.mcp_tool_discovery import register_mcp_servers
        from tools.mcp_tool_lifecycle import shutdown_mcp_servers
        try:
            registered = register_mcp_servers({"teamharness": server})
            assert "mcp__teamharness__taskflow" in registered, registered
            print("Hermes native tool registry exposes TeamHarness taskflow (no model/Task execution)")
        finally:
            shutdown_mcp_servers()
