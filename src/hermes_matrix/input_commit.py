"""Fail-closed persistence boundary for the pinned Hermes turn prologue.

The SDK's plugin pre_llm_call hook times out fail-open and runs before its
normal turn persistence. Wrap that call site directly, outside plugin dispatch,
so native input readback and common CAS finish before hooks or compaction.
"""
from functools import wraps
import inspect

from task_recovery import Recovery, RecoveryConflict, enabled
from worker_matrix_state import WorkerMatrix, matrix_event_key


def commit_input(agent, messages, conversation_history):
    if not enabled() or getattr(agent, "platform", None) != "matrix":
        return
    event_id = getattr(agent, "_persist_user_message_platform_id", None)
    if not isinstance(event_id, str) or not event_id.startswith("$"):
        return  # Startup/internal work has no Matrix source to acknowledge.
    room = getattr(agent, "_chat_id", None)
    index = getattr(agent, "_persist_user_message_idx", None)
    if (not isinstance(room, str) or not room.startswith("!") or not isinstance(index, int)
            or not 0 <= index < len(messages)):
        raise RecoveryConflict("Native input has no current Matrix source row")
    user = messages[index]
    if user.get("role") != "user" or user.get("platform_message_id") != event_id:
        raise RecoveryConflict("Native input source differs from the admitted Matrix input")
    matrix = WorkerMatrix("hermes")
    key = matrix_event_key(room, event_id)
    if matrix.input(key) is None:
        raise RecoveryConflict("Required Matrix input is not durable")
    db = getattr(agent, "_session_db", None)
    if db is None or getattr(agent, "_persist_disabled", False):
        raise RecoveryConflict("Native persistence is unavailable for this Matrix input")
    # Use the SDK writer and its row markers, not a parallel transcript. The
    # writer can swallow a storage exception; actual row readback is mandatory.
    agent._ensure_db_session()
    agent._persist_session(messages, conversation_history)
    session_id = agent.session_id
    session = db.get_session(session_id)
    rows = db.get_messages(session_id, include_ancestors=True)
    row_id = user.get("_row_id")
    if (not session or session.get("end_reason") is not None or not isinstance(row_id, int)
            or not any(row.get("id") == row_id and row.get("role") == "user"
                and row.get("platform_message_id") == event_id and row.get("content") for row in rows)):
        raise RecoveryConflict("Actual SDK database does not retain the current Matrix input row")
    digest = Recovery.digest({"session_id": session_id, "messages": rows})
    snapshot = matrix.native_input(key)
    if not matrix.native_current(snapshot, session_id, digest):
        matrix.native_committed(key, snapshot, session_id, digest)


def install_native_input_guard():
    if not enabled():
        return
    from agent import turn_context
    original = turn_context._collect_pre_llm_call_context
    if getattr(original, "_agentteams_input_guard", False):
        return
    if not {"agent", "messages", "conversation_history"} <= inspect.signature(original).parameters.keys():
        raise RuntimeError("Pinned Hermes native input persistence boundary is unavailable")

    @wraps(original)
    def guarded(agent, **kwargs):
        commit_input(agent, kwargs["messages"], kwargs["conversation_history"])
        return original(agent, **kwargs)

    guarded._agentteams_input_guard = True
    turn_context._collect_pre_llm_call_context = guarded
