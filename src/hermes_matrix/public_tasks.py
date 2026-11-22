"""Bind the common Task CLI to Hermes' native request and tool lifecycles."""
from __future__ import annotations

from contextlib import contextmanager
from copy import copy
from contextvars import ContextVar
from dataclasses import replace
from functools import lru_cache, wraps
import importlib.util
import hashlib
import json
import os
from pathlib import Path
import sys
import threading

current_turn = ContextVar("agentteams_public_task_turn", default=None)
TASK_TOOL = "mcp__teamharness__task"
LEGACY_TASK_TOOLS = {"mcp__teamharness__" + name for name in ("taskflow", "projectflow", "filesync")}


@lru_cache(maxsize=None)
def common(name):
    path = Path(os.environ["AGENTTEAMS_PLUGIN_DIR"]) / "mcp/common" / (name + ".py")
    if str(path.parent) not in sys.path:
        sys.path.insert(0, str(path.parent))
    spec = importlib.util.spec_from_file_location("agentteams_hermes_" + name, path)
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


class TaskTurn:
    def __init__(self, event, prepared=None):
        meta = {"room_id": event.source.chat_id, "sender_id": event.source.user_id,
                "event_id": event.message_id}
        meta.update(getattr(event, "_task_protocol_meta", {}) or {})
        source_ids = getattr(event, "_agentteams_input_ids", [])
        if len(source_ids) > 1 and not meta.get("teamharness_coordination"):
            meta["matrix_input_sources"] = [{**meta, "event_id": identity} for identity in source_ids]
        # Coordination notifications already carry a verified Task scope.
        # Only a Human request supplies input references for creating a Task.
        sources = []
        if str(event.message_id).startswith("$") and not meta.get("teamharness_coordination"):
            sources = common("task_receipt").creation_sources(meta)
        self.context = dict(prepared or {})
        self.context.update(role=os.getenv("AGENTTEAMS_AGENT_ROLE", "worker"),
                            sources=sources, sourceEvent=event.message_id)
        self.event = event
        self.meta = meta
        self.pending = []
        self.previous_io = {}
        self.bound = False
        self.lock = threading.Lock()
        self.previous_tools = None
        self.receipts = {}
        self.phase_complete = False
        self.model_input_ids = []

    def task_journal(self, action, **fields):
        identity = getattr(self.event, "_agentteams_recovery", None)
        if not identity:
            return None
        from task_recovery_rpc import execute
        return execute({"runtime": "hermes", "device_delivery": True, "identity": identity,
                        "action": action, **fields})

    def trace_metadata(self):
        coordination = self.meta.get("teamharness_coordination") or {}
        claim = getattr(self.event, "_agentteams_recovery", None) or {}
        receipt = self.context.get("context") or {}
        task = receipt.get("task") or {}
        metadata = {**self.meta, "event_id": self.event.message_id if str(self.event.message_id).startswith("$") else "",
            "task_id": claim.get("task_id") or coordination.get("task_id") or task.get("id"),
            "team_id": claim.get("team_id"), "project_id": coordination.get("project_id") or task.get("projectId")}
        generation = claim.get("generation") if claim else receipt.get("generation")
        if isinstance(generation, int) and generation > 0:
            metadata["execution_generation"] = generation
        return metadata

    def bind(self, agent, task_id, messages):
        if self.bound:
            return
        self.bound = True
        schema = common("task_native").invoke({"operation": "schema", "role": self.context["role"]})
        self.previous_tools = (agent, agent.tools, agent.valid_tool_names)
        tools = [tool for tool in agent.tools if tool.get("function", {}).get("name") not in LEGACY_TASK_TOOLS | {TASK_TOOL}]
        if self.context.get("review") or self.context.get("completion") or self.context.get("planning"):
            tools = []
        tools.append({"type": "function", "function": {"name": TASK_TOOL,
            "description": schema["description"], "parameters": schema["inputSchema"]}})
        agent.tools = tools
        agent.valid_tool_names = {tool["function"]["name"] for tool in tools}
        receipt = self.context.get("context") or {}
        if self.context.get("execution"):
            from tools.terminal_tool import register_task_env_overrides, resolve_task_overrides
            if not task_id or task_id == "default":
                raise RuntimeError("Task execution requires an isolated native terminal identity")
            previous = dict(resolve_task_overrides(task_id) or {})
            self.previous_io[task_id] = previous
            register_task_env_overrides(task_id, {**previous, "cwd": receipt["outputDirectory"]})
        for message in messages:
            event_id = message.get("platform_message_id")
            if message.get("role") == "user" and event_id == self.event.message_id and isinstance(message.get("content"), str):
                marker = f"[Message {event_id}]\n"
                if not message["content"].startswith(marker):
                    message["content"] = marker + message["content"]
        if self.context["role"] == "manager":
            self.delivery_context(agent, messages)

    def delivery_context(self, agent, messages):
        """Persist SDK context first, then the plugin's compact delivery cursor."""
        key = hashlib.sha256(agent.session_id.encode()).hexdigest()
        path = Path(os.environ["HERMES_HOME"]) / "agentteams-context" / (key + ".json")
        middle = json.loads(path.read_text()) if path.exists() else {}
        delivery = common("task_native").invoke({"operation": "delivery_context",
            "room": self.event.source.chat_id, "middle": middle})
        if not delivery["facts"]:
            return
        text = "\n\n".join(delivery["facts"])
        messages.append({"role": "system", "content": text,
            "display_metadata": {"agentteams_delivery_context": delivery["acknowledgements"]}})
        agent._persist_session(messages)
        rows = agent._session_db.get_messages(agent.session_id, include_ancestors=True) if agent._session_db else []
        if not any(row.get("role") == "system" and row.get("content") == text for row in rows):
            raise RuntimeError("Sent delivery facts were not retained in native context")
        updated = common("task_native").invoke({"operation": "ack_delivery_context",
            "middle": middle, "acknowledgements": delivery["acknowledgements"]})
        path.parent.mkdir(parents=True, exist_ok=True)
        temporary = path.with_suffix(".tmp")
        temporary.write_text(json.dumps(updated))
        temporary.replace(path)

    def prompt(self):
        path = Path(os.environ["AGENTTEAMS_PLUGIN_DIR"]) / "skills/team/task-management/SKILL.md"
        pieces = [path.read_text(), "[Task input references]\n" + json.dumps(self.context["sources"])]
        if self.context.get("fact"):
            pieces.append(self.context["fact"] + "\n" + json.dumps(self.context.get("context"), default=str))
        if self.context.get("roster"):
            pieces.append("[Registered public team identities]\n" + json.dumps(self.context["roster"]))
        pieces.extend(self.receipts.values())
        return "\n\n".join(pieces)

    def invoke(self, arguments):
        with self.lock:
            outcome = common("task_native").invoke({"operation": "task", "arguments": arguments, "context": self.context})
            self.context.update(outcome.get("contextUpdate") or {})
            self.pending.extend(outcome.get("refreshedInputs") or [])
            self.phase_complete = self.phase_complete or outcome.get("phaseComplete") is True
            result = dict(outcome["result"])
            if outcome.get("fact") and outcome.get("receiptId"):
                self.receipts.setdefault(outcome["receiptId"], outcome["fact"])
            elif outcome.get("refreshedInputs"):
                self.receipts[json.dumps([source["eventId"] for source in outcome["refreshedInputs"]])] = outcome["fact"]
                return outcome["fact"]
            return json.dumps(result, ensure_ascii=False)

    def checkpoint_refreshed(self, agent, messages):
        if not self.pending:
            return
        pending, self.pending = self.pending, []
        for source in pending:
            if not any(row.get("role") == "user" and row.get("platform_message_id") == source["eventId"] for row in messages):
                messages.append({"role": "user", "platform_message_id": source["eventId"],
                    "content": f"[Message {source['eventId']}]\n" + source["body"]})
        agent._persist_session(messages)
        db = agent._session_db
        if db is None or getattr(agent, "_persist_disabled", False):
            raise RuntimeError("Native Human input persistence is unavailable")
        rows = db.get_messages(agent.session_id, include_ancestors=True)
        if any(not any(row.get("role") == "user" and row.get("platform_message_id") == source["eventId"]
                       and source["body"] in str(row.get("content", "")) for row in rows) for source in pending):
            raise RuntimeError("Refreshed Human input was not retained by the native writer")
        self.context.pop("inputRefreshPending", None)

    def close(self):
        if self.previous_tools:
            agent, tools, names = self.previous_tools
            agent.tools, agent.valid_tool_names = tools, names
        if self.previous_io:
            from tools.terminal_tool import register_task_env_overrides, clear_task_env_overrides
            for task_id, previous in self.previous_io.items():
                if previous:
                    register_task_env_overrides(task_id, previous)
                else:
                    clear_task_env_overrides(task_id)


@contextmanager
def task_turn(event, prepared=None):
    turn = TaskTurn(event, prepared)
    token = current_turn.set(turn)
    try:
        yield turn
    finally:
        turn.close()
        current_turn.reset(token)


def install():
    """Use pinned native call sites; persistence failures must not fail open."""
    from agent import turn_context, conversation_loop
    from run_agent import AIAgent
    import model_tools
    original = turn_context._collect_pre_llm_call_context
    if getattr(original, "_agentteams_public_tasks", False):
        return

    @wraps(original)
    def prepare(agent, **kwargs):
        turn = current_turn.get()
        if turn:
            turn.bind(agent, kwargs["effective_task_id"], kwargs["messages"])
        return original(agent, **kwargs)

    build = AIAgent._build_api_kwargs
    @wraps(build)
    def wire(agent, messages, *args, **kwargs):
        turn = current_turn.get()
        if turn:
            prompt = turn.prompt()
            messages = [dict(message) for message in messages]
            system = next((message for message in messages if message.get("role") == "system"), None)
            if system is None:
                messages.insert(0, {"role": "system", "content": prompt})
            elif prompt not in system.get("content", ""):
                system["content"] += "\n\n" + prompt
            if os.getenv("AGENTTEAMS_OTEL_ENABLED", "").lower() in {"true", "1"}:
                from opentelemetry import trace
                turn.model_input_ids = [source["eventId"] for source in turn.context["sources"]
                    if any(message.get("role") == "user" and f"[Message {source['eventId']}]" in str(message.get("content", ""))
                           for message in messages)]
                trace.get_current_span().set_attribute("agentteams.input.event_ids", turn.model_input_ids)
        if turn:
            turn.task_journal("admit_tools")
        return build(agent, messages, *args, **kwargs)

    def traced_provider(call):
        @wraps(call)
        def invoke(agent, *args, **kwargs):
            turn = current_turn.get()
            if not turn or os.getenv("AGENTTEAMS_OTEL_ENABLED", "").lower() not in {"true", "1"}:
                return call(agent, *args, **kwargs)
            import at_telemetry as telemetry
            token = telemetry._correlation.set({**telemetry._correlation.get(),
                "agentteams.input.event_ids": turn.model_input_ids})
            try:
                return call(agent, *args, **kwargs)
            finally:
                telemetry._correlation.reset(token)
        return invoke

    execute = model_tools.handle_function_call
    @wraps(execute)
    def tool(function_name, arguments, *args, **kwargs):
        turn = current_turn.get()
        if turn and function_name == TASK_TOOL:
            try:
                return turn.invoke(arguments)
            except common("task_native").NativeTaskValidationError as error:
                return json.dumps({"ok": False, "effect": "none", "error": "native_task_validation", "message": str(error)})
        return execute(function_name, arguments, *args, **kwargs)

    batch = AIAgent._execute_tool_calls
    @wraps(batch)
    def tools(agent, assistant_message, messages, *args, **kwargs):
        turn = current_turn.get()
        identity = getattr(turn.event, "_agentteams_recovery", None) if turn else None
        if identity:
            turn.task_journal("admit_tools")
            # The fixed public Task resolver is native-owned. Fence business
            # effects first; publishing Task completion must not revoke their lease.
            business = [call for call in assistant_message.tool_calls if call.function.name != TASK_TOOL]
            canonical = [call for call in assistant_message.tool_calls if call.function.name == TASK_TOOL]
            result = None
            if business:
                operation = turn.task_journal("begin_tools", calls=[{"id": call.id,
                    "name": call.function.name, "arguments": call.function.arguments} for call in business])
                selected = copy(assistant_message)
                selected.tool_calls = business
                offset = len(messages)
                result = batch(agent, selected, messages, *args, **kwargs)
                rows = [row for row in messages[offset:] if row.get("role") == "tool"]
                if (getattr(agent, "_incremental_persistence_failed", False)
                        or any(row.get("effect_disposition") == "unknown" for row in rows)):
                    raise RuntimeError("Native Task tool results require verification")
                turn.task_journal("finish_tools", operation_id=operation,
                    results=[{"id": row["tool_call_id"], "content": row["content"]} for row in rows])
            if canonical:
                selected = copy(assistant_message)
                selected.tool_calls = canonical
                result = batch(agent, selected, messages, *args, **kwargs)
        else:
            result = batch(agent, assistant_message, messages, *args, **kwargs)
        if turn:
            turn.checkpoint_refreshed(agent, messages)
        return result

    round_tools = conversation_loop.run_tool_round
    @wraps(round_tools)
    def finish_phase(agent, **kwargs):
        verdict = round_tools(agent, **kwargs)
        turn = current_turn.get()
        if (not turn or not turn.phase_complete or verdict.failed
                or getattr(agent, "_incremental_persistence_failed", False)):
            return verdict
        fact = "\n\n".join(turn.receipts.values())
        verdict.messages.append({"role": "system", "content": fact})
        agent._persist_session(verdict.messages)
        rows = agent._session_db.get_messages(agent.session_id, include_ancestors=True) if agent._session_db else []
        if not any(row.get("role") == "system" and row.get("content") == fact for row in rows):
            raise RuntimeError("Completed Task phase receipt is not durable")
        return replace(verdict, action="break", final_response=fact,
                       _turn_exit_reason="text_response(public_task_phase)")

    prepare._agentteams_public_tasks = True
    turn_context._collect_pre_llm_call_context = prepare
    AIAgent._build_api_kwargs = wire
    AIAgent._interruptible_api_call = traced_provider(AIAgent._interruptible_api_call)
    AIAgent._interruptible_streaming_api_call = traced_provider(AIAgent._interruptible_streaming_api_call)
    model_tools.handle_function_call = tool
    AIAgent._execute_tool_calls = tools
    conversation_loop.run_tool_round = finish_phase
