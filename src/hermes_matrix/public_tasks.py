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


def is_canonical_task_call(call):
    if call.function.name == TASK_TOOL:
        return True
    if call.function.name != "tool_call":
        return False
    from tools.tool_search import normalize_tool_call_entries
    try:
        arguments = json.loads(call.function.arguments)
    except (TypeError, ValueError):
        return False
    if not isinstance(arguments, dict):
        return False
    entries, error = normalize_tool_call_entries(arguments)
    return not error and len(entries) == 1 and entries[0]["name"] == TASK_TOOL


@lru_cache(maxsize=None)
def common(name):
    path = Path(os.environ["AGENTTEAMS_PLUGIN_DIR"]) / "mcp/common" / (name + ".py")
    if str(path.parent) not in sys.path:
        sys.path.insert(0, str(path.parent))
    spec = importlib.util.spec_from_file_location("agentteams_hermes_" + name, path)
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


def retain_confirmed_handoffs():
    from hermes_state import SessionDB
    from pathlib import Path
    folder = Path(os.environ["AGENTTEAMS_WORKER_HOME"]) / ".agentteams/native-handoffs"
    if not folder.is_dir():
        return
    items = common("task_native").invoke({"operation": "confirmed_handoffs", "runtime": "hermes"})["items"]
    if not items:
        return
    db = SessionDB()
    try:
        for item in items:
            session = item["target"]["session"]
            if not db.get_session(session):
                continue
            existing = {row["content"] for row in db.get_messages(session) if row["role"] == "system"}
            for fact in item["facts"]:
                if fact not in existing:
                    db.append_message(session, "system", content=fact)
                    existing.add(fact)
            (folder / (item["key"] + ".json")).unlink(missing_ok=True)
    finally:
        db.close()


class TaskTurn:
    @property
    def has_task_progress(self):
        return self.progress is not None and bool(self.meta.get("teamharness_coordination") or
                                                  (self.context.get("context") or {}).get("task"))

    def __init__(self, event, prepared=None, progress=None):
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
                            sources=sources, sourceEvent=event.message_id,
                            actor=meta.get("agent_matrix_user_id", ""))
        identity = getattr(event, "_agentteams_recovery", {}) or {}
        if identity.get("session_id"):
            self.context["handoffTarget"] = {"runtime": "hermes", "session": identity["session_id"]}
        if progress:
            self.context["runId"] = progress.run_id
        self.event = event
        self.meta = meta
        self.pending = []
        self.previous_io = {}
        self.bound = False
        self.lock = threading.Lock()
        self.previous_tools = None
        self.progress, self.previous_callbacks = progress, {}
        self.receipts = {}
        self.phase_complete = False
        self.display_text = "NO_REPLY"
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
        if self.progress:
            for name, callback in [('tool_start_callback', self.progress.tool_started),
                                   ('tool_complete_callback', self.progress.tool_completed),
                                   ('reasoning_callback', self.progress.reasoning)]:
                previous = getattr(agent, name, None)
                self.previous_callbacks[name] = (agent, previous)
                def composed(*args, _previous=previous, _callback=callback, **kwargs):
                    if _previous:
                        _previous(*args, **kwargs)
                    _callback(*args, **kwargs)
                setattr(agent, name, composed)
            if self.has_task_progress:
                self.previous_callbacks['interim_assistant_callback'] = (agent, getattr(agent, 'interim_assistant_callback', None))
                def task_commentary(text, *, already_streamed=False):
                    self.progress.reasoning(text + "\n")
                agent.interim_assistant_callback = task_commentary
            previous = getattr(agent, 'tool_progress_callback', None)
            self.previous_callbacks['tool_progress_callback'] = (agent, previous)
            def native_progress(event_type, tool_name=None, preview=None, args=None, **kwargs):
                if event_type in {'tool.started', 'tool.completed'}:
                    return
                if event_type == '_thinking' or tool_name == '_thinking':
                    if preview:
                        self.progress.reasoning(preview)
                    return
                if previous:
                    return previous(event_type, tool_name=tool_name, preview=preview, args=args, **kwargs)
            agent.tool_progress_callback = native_progress
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
            pieces.append(self.context["fact"] + "\n" + json.dumps(
                common("task_context").model_context(self.context.get("context") or {}), default=str))
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
            if outcome.get("phaseComplete"):
                self.display_text = outcome.get("displayText", "NO_REPLY")
            result = common("task_native").model_result(outcome)
            if outcome.get("fact") and outcome.get("receiptId"):
                self.receipts.setdefault(outcome["receiptId"], outcome["fact"])
                for receipt in outcome.get("extraReceipts", []):
                    self.receipts.setdefault(receipt["receiptId"], receipt["fact"])
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
        for name, (agent, callback) in self.previous_callbacks.items():
            setattr(agent, name, callback)
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
def task_turn(event, prepared=None, progress=None):
    turn = TaskTurn(event, prepared, progress)
    token = current_turn.set(turn)
    try:
        yield turn
    finally:
        turn.close()
        current_turn.reset(token)


def install():
    """Use pinned native call sites; persistence failures must not fail open."""
    from agent import turn_context, conversation_loop
    from agent.turn_facade import TurnFacadeMixin
    from run_agent import AIAgent
    import model_tools
    original = turn_context._collect_pre_llm_call_context
    if getattr(original, "_agentteams_public_tasks", False):
        return

    from gateway.run_turn import GatewayTurnMixin
    native_heartbeat = GatewayTurnMixin._run_agent_notify_long_running
    @wraps(native_heartbeat)
    async def task_heartbeat(runner, *args, **kwargs):
        turn = current_turn.get()
        if turn and turn.has_task_progress:
            return
        return await native_heartbeat(runner, *args, **kwargs)

    native_conversation = TurnFacadeMixin.run_conversation
    @wraps(native_conversation)
    def task_conversation(agent, *args, **kwargs):
        try:
            return native_conversation(agent, *args, **kwargs)
        except BaseException:
            turn = current_turn.get()
            if turn and turn.progress:
                turn.progress.failed = True
            raise

    title_session = turn_context._maybe_title_session_at_turn_start
    @wraps(title_session)
    def task_title(agent, messages, title_user_message=None):
        turn = current_turn.get()
        scope = turn and (turn.context.get("execution") or turn.context.get("planning")
                          or turn.context.get("review") or turn.context.get("completion"))
        if not scope:
            return title_session(agent, messages, title_user_message)
        # The Task room already owns its name; native titles are globally unique.

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
            canonical = [call for call in assistant_message.tool_calls if is_canonical_task_call(call)]
            business = [call for call in assistant_message.tool_calls if call not in canonical]
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
        return replace(verdict, action="break", final_response=turn.display_text,
                       _turn_exit_reason="text_response(public_task_phase)")

    prepare._agentteams_public_tasks = True
    turn_context._collect_pre_llm_call_context = prepare
    turn_context._maybe_title_session_at_turn_start = task_title
    GatewayTurnMixin._run_agent_notify_long_running = task_heartbeat
    TurnFacadeMixin.run_conversation = task_conversation
    AIAgent._build_api_kwargs = wire
    AIAgent._interruptible_api_call = traced_provider(AIAgent._interruptible_api_call)
    AIAgent._interruptible_streaming_api_call = traced_provider(AIAgent._interruptible_streaming_api_call)
    model_tools.handle_function_call = tool
    AIAgent._execute_tool_calls = tools
    conversation_loop.run_tool_round = finish_phase
