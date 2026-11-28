"""Native SDK tracing adapters owned by this Harness."""
import inspect
import logging
import os
import time
from at_trace_transport import incoming, _delivery
import wrapt
from opentelemetry import trace
import at_telemetry as telemetry
from at_runtime_adapters import bind, enrich_task, start_turn, register_hook, _adapter_errors

PACKAGES = ("hermes-agent", "openai")
INSTRUMENT_OPENAI = True


def hermes_turn(module):
    def run(wrapped, instance, args, kwargs):
        metadata = {
            "room_id": getattr(instance, "_chat_id", ""),
            "event_id": kwargs.get("persist_user_platform_id", ""),
        }
        # Read the runtime's current verified frame; native session IDs are opaque.
        try:
            from hermes_matrix.public_tasks import current_turn
        except ImportError:
            current = None
        else:
            current = current_turn.get()
        if current is not None:
            metadata.update(current.trace_metadata())
        fields = telemetry.turn_fields(str(getattr(instance, "session_id", "")), metadata=metadata)
        if kwargs.get("task_id"):
            fields["agentteams.runtime.task_id"] = str(kwargs["task_id"])
        if getattr(instance, "_parent_session_id", None):
            fields["agentteams.runtime.parent_session_id"] = str(instance._parent_session_id)
        from at_trace_transport import incoming
        fields.update(incoming({"com.agentteams.trace": metadata.get("otel_carrier", {})}))
        fields = enrich_task(fields, os.getenv("AGENTTEAMS_WORKER_HOME") or os.getenv("HOME"))
        with bind(fields):
            with trace.use_span(start_turn(fields, {"message": args[0] if args else kwargs.get("user_message"),
                                                    "history": kwargs.get("conversation_history")}), end_on_exit=True) as span:
                from at_trace_transport import remember_reply
                remember_reply(span, fields)
                try:
                    result = wrapped(*args, **kwargs)
                    telemetry.set_io(span, output=result)
                    telemetry.outcome(span, result)
                    return result
                except BaseException as exc:
                    telemetry.outcome(span, error=exc)
                    raise

    wrapt.wrap_function_wrapper(module, "TurnFacadeMixin.run_conversation", run)


def hermes_tools(module):
    # Both sequential and parallel execution use this boundary, including policy
    # blocks and inline tools. Keep an explicit compatibility check for upgrades.
    signature = inspect.signature(module._run_agent_tool_execution_middleware)
    required = {"function_name", "function_args", "tool_call_id", "execute"}
    if not required.issubset(signature.parameters):
        message = "Unsupported Hermes tool execution signature; update telemetry adapter"
        _adapter_errors.append(message)
        logging.getLogger("agentteams.telemetry").error(message)
        return

    def execute(wrapped, instance, args, kwargs):
        name = kwargs["function_name"]
        with telemetry.operation(f"tool {name}", attributes={
            "langfuse.observation.type": "tool", "gen_ai.operation.name": "execute_tool",
            "gen_ai.tool.name": name, "gen_ai.tool.call.id": kwargs.get("tool_call_id", ""),
        }) as span:
            telemetry.set_io(span, input=kwargs.get("function_args"))
            try:
                result = wrapped(*args, **kwargs)
                telemetry.set_io(span, output=result.result)
                span.set_attribute("agentteams.tool.blocked", result.blocked)
                span.set_attribute("agentteams.tool.dispatched", result.dispatched)
                span.set_attribute("agentteams.tool.middleware", str(telemetry.capture(result.middleware_trace)))
                telemetry.outcome(span, result.result)
                if result.blocked:
                    telemetry.outcome(span, error=RuntimeError("tool blocked by policy"))
                return result
            except BaseException as exc:
                telemetry.outcome(span, error=exc)
                raise

    wrapt.wrap_function_wrapper(module, "_run_agent_tool_execution_middleware", execute)


def openai_content(module):
    # OpenAI instrumentation's prompt capture varies across SDK/provider paths.
    # Supply the actual SDK request to its generation span at on_start instead
    # of emitting a second generation (which would double-count usage).
    def create(wrapped, instance, args, kwargs):
        token = telemetry._model_request.set(kwargs)
        try:
            return wrapped(*args, **kwargs)
        finally:
            telemetry._model_request.reset(token)

    async def async_create(wrapped, instance, args, kwargs):
        token = telemetry._model_request.set(kwargs)
        try:
            return await wrapped(*args, **kwargs)
        finally:
            telemetry._model_request.reset(token)

    wrapt.wrap_function_wrapper(module, "Completions.create", create)
    wrapt.wrap_function_wrapper(module, "AsyncCompletions.create", async_create)


def hermes_gateway(module):
    async def receive(wrapped, instance, args, kwargs):
        event = args[0] if args else kwargs["event"]
        raw = event.raw_message
        content = getattr(raw, "content", raw.get("content", raw) if isinstance(raw, dict) else {})
        fields = {**telemetry._correlation.get(), **incoming(content, room_id=event.source.chat_id, event_id=event.message_id)}
        fields["agentteams.message.received_at_ns"] = time.time_ns()
        token = telemetry._correlation.set(fields)
        delivery_token = _delivery.set({})
        try:
            # Native gateway creates its tasks here, copying ContextVars.
            return await wrapped(*args, **kwargs)
        finally:
            _delivery.reset(delivery_token)
            telemetry._correlation.reset(token)

    wrapt.wrap_function_wrapper(module, "BasePlatformAdapter.handle_message", receive)


def install():
    register_hook(hermes_gateway, "gateway.platforms.base")
    register_hook(hermes_turn, "agent.turn_facade")
    register_hook(hermes_tools, "agent.tool_executor")
    register_hook(openai_content, "openai.resources.chat.completions")
