"""Pinned Hermes SDK + local model/Task fixtures; no real Task certification."""
import json
import os
from pathlib import Path
import tempfile
import threading
from concurrent.futures import ThreadPoolExecutor
from types import SimpleNamespace
from unittest import TestCase, main
from unittest.mock import patch, Mock
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer

from hermes_state import SessionDB
from run_agent import AIAgent
from tools.terminal_tool import resolve_task_overrides
from hermes_matrix.public_tasks import TASK_TOOL, common, install, task_turn


class NativePublicTaskSmoke(TestCase):
    def setUp(self):
        self.tmp = tempfile.TemporaryDirectory()
        self.addCleanup(self.tmp.cleanup)
        self.root = Path(self.tmp.name)
        self.env = patch.dict(os.environ, {"HERMES_HOME": str(self.root / ".hermes"),
            "AGENTTEAMS_AGENT_ROLE": "worker", "AGENTTEAMS_OTEL_ENABLED": "false"})
        self.env.start()
        self.addCleanup(self.env.stop)
        self.db = SessionDB(self.root / "own-native-state.db")
        self.addCleanup(self.db.close)
        self.event = SimpleNamespace(message_id="$source", source=SimpleNamespace(chat_id="!fixture:test", user_id="@human:test"))
        self.directory = self.root / "tasks/own"
        self.output = self.directory / "workspace"
        self.output.mkdir(parents=True)
        self.result = {"task": {"id": "own", "mainTaskId": "main", "projectId": "", "status": "in_progress",
                               "assignmentId": "dispatch", "assignedTo": "worker"},
                       "taskDirectory": str(self.directory), "outputDirectory": str(self.output),
                       "scratchDirectory": str(self.output / ".scratch")}
        self.submission = {"task": {"id": "own", "status": "submitted", "submissionId": "saved-submission"},
                           "notification": {"status": "queued", "recipient": "leader", "eventId": "notice"}}
        self.bridge = common("task_native")
        self.call = patch.object(self.bridge.task_cli, "run", side_effect=lambda args:
            self.result if args["action"] == "begin" else self.submission)
        self.call.start()
        self.addCleanup(self.call.stop)
        self.prepared = self.bridge.invoke({"operation": "prepare_event", "event": {"kind": "task_assigned",
            "task_id": "own", "main_task_id": "main", "project_id": "", "delivery_id": "dispatch"}})
        install()

    def agent(self, url="http://127.0.0.1:1/v1"):
        agent = AIAgent(model="public-task-fixture", api_key="synthetic", provider="custom", base_url=url,
            platform="matrix", chat_id="!fixture:test", session_id="native-public-task", session_db=self.db,
            skip_memory=True, skip_context_files=True, skip_background_review=True,
            enabled_toolsets=[], quiet_mode=True, max_iterations=3)
        agent._cached_system_prompt = "Own synthetic SDK fixture."
        agent._ensure_db_session()
        self.db.set_session_title(agent.session_id, "Own native SDK fixture")
        return agent

    def test_actual_native_model_wire_and_tool_receipt_are_bound(self):
        requests = []
        case = self
        class Model(BaseHTTPRequestHandler):
            def log_message(self, *args):
                pass
            def do_POST(self):
                request = json.loads(self.rfile.read(int(self.headers["Content-Length"])))
                is_task = any("Task input references" in str(row.get("content", "")) for row in request.get("messages", []))
                if is_task:
                    requests.append(request)
                has_result = any(row.get("role") == "tool" and "saved-submission" in str(row.get("content")) for row in request.get("messages", []))
                message = {"role": "assistant", "content": "NATIVE_PUBLIC_TASK_OK"}
                finish = "stop"
                if is_task and not has_result:
                    identity = "foreign" if getattr(case, "invalid_first", False) and len(requests) == 1 else "own"
                    message = {"role": "assistant", "content": None, "tool_calls": [{"id": "call-task-" + str(len(requests)), "type": "function",
                        "function": {"name": TASK_TOOL, "arguments": json.dumps({"action": "submit", "taskId": identity, "summary": "fixture"})}}]}
                    finish = "tool_calls"
                if request.get("stream"):
                    delta = dict(message)
                    if "tool_calls" in delta:
                        delta["tool_calls"][0]["index"] = 0
                    chunks = [{"id": "fixture", "object": "chat.completion.chunk", "created": 1,
                        "model": "public-task-fixture", "choices": [{"index": 0, "delta": delta, "finish_reason": None}]},
                        {"id": "fixture", "object": "chat.completion.chunk", "created": 1,
                         "model": "public-task-fixture", "choices": [{"index": 0, "delta": {}, "finish_reason": finish}],
                         "usage": {"prompt_tokens": 1, "completion_tokens": 1, "total_tokens": 2}}]
                    body = ("".join("data: " + json.dumps(row) + "\n\n" for row in chunks) + "data: [DONE]\n\n").encode()
                    content_type = "text/event-stream"
                else:
                    body = json.dumps({"id": "fixture", "object": "chat.completion", "created": 1,
                        "model": "public-task-fixture", "choices": [{"index": 0, "message": message, "finish_reason": finish}],
                        "usage": {"prompt_tokens": 1, "completion_tokens": 1, "total_tokens": 2}}).encode()
                    content_type = "application/json"
                self.send_response(200)
                self.send_header("Content-Type", content_type)
                self.send_header("Content-Length", str(len(body)))
                self.end_headers()
                self.wfile.write(body)
        server = ThreadingHTTPServer(("127.0.0.1", 0), Model)
        threading.Thread(target=server.serve_forever, daemon=True).start()
        self.addCleanup(server.server_close)
        self.addCleanup(server.shutdown)
        agent = self.agent(f"http://127.0.0.1:{server.server_port}/v1")
        with task_turn(self.event, self.prepared):
            result = agent.run_conversation("Perform this own synthetic task", persist_user_platform_id="$source")
        self.assertEqual(result["final_response"], "NO_REPLY")
        self.assertTrue(result["completed"])
        self.assertFalse(result["interrupted"])
        self.assertEqual(len(requests), 2 if getattr(self, "invalid_first", False) else 1)
        self.assertIn(TASK_TOOL, [tool["function"]["name"] for tool in requests[0]["tools"]])
        self.assertTrue(any(row.get("role") == "system" and "Delivery and acceptance are not yet confirmed" in str(row.get("content"))
                            for row in self.db.get_messages(agent.session_id)))
        self.assertTrue(any(row.get("role") == "tool" and "saved-submission" in str(row.get("content"))
                            for row in self.db.get_messages(agent.session_id)))

    def test_actual_model_and_task_trace_keep_verified_identity_without_content(self):
        from opentelemetry.sdk.trace.export import SimpleSpanProcessor
        from opentelemetry.sdk.trace.export.in_memory_span_exporter import InMemorySpanExporter
        import at_telemetry as telemetry
        exporter = InMemorySpanExporter()
        os.environ.update(AGENTTEAMS_OTEL_ENABLED="true", AGENTTEAMS_OTEL_RUNTIME="hermes", AGENTTEAMS_OTEL_CAPTURE_CONTENT="false")
        provider = telemetry.configure(local_only=True)
        provider.add_span_processor(SimpleSpanProcessor(exporter))
        self.event._task_protocol_meta = {"teamharness_coordination": {"id": "c" * 32, "kind": "task_assigned", "task_id": "own"}}
        self.prepared["context"]["generation"] = 3
        self.test_actual_native_model_wire_and_tool_receipt_are_bound()
        spans = exporter.get_finished_spans()
        root = next(span for span in spans if span.name == "worker.turn")
        model = next(span for span in spans if span.attributes.get("gen_ai.operation.name") == "chat" or span.name == "openai.chat")
        tool = next(span for span in spans if span.attributes.get("gen_ai.tool.name") == TASK_TOOL)
        for span in (root, model, tool):
            self.assertEqual(span.attributes["agentteams.task.id"], "own")
            self.assertEqual(span.attributes["agentteams.event.id"], "$source")
            self.assertEqual(span.attributes["agentteams.coordination.id"], "c" * 32)
            self.assertEqual(span.attributes["agentteams.execution.generation"], 3)
        # Assignment notifications carry a Task identity, but are not Human input sources.
        self.assertEqual(tuple(model.attributes["agentteams.input.event_ids"]), ())
        self.assertEqual(model.attributes["gen_ai.usage.input_tokens"], 1)
        self.assertEqual(model.attributes["gen_ai.usage.output_tokens"], 1)
        self.assertEqual(model.parent.span_id, root.context.span_id)
        self.assertFalse(any("Perform this own synthetic task" in str(span.attributes) for span in spans))

    def test_task_business_tools_use_public_journal_and_unknown_checkpoint_blocks_model(self):
        import model_tools
        from openai.types.chat import ChatCompletionMessage
        from hermes_matrix.public_tasks import TaskTurn
        from task_operation import admit, begin, finish, result_bytes
        from task_recovery import RecoveryConflict
        checkpoint, actions = {}, []
        def journal(action, **fields):
            nonlocal checkpoint
            actions.append(action)
            if action == "admit_tools":
                return admit(checkpoint)
            if action == "begin_tools":
                operation, checkpoint = begin(checkpoint, ["team", "own"], fields["calls"])
                return operation
            data = result_bytes(checkpoint, fields["operation_id"], fields["results"])
            checkpoint = finish(checkpoint, fields["operation_id"], {"key": "fixture/public-result", "size": len(data)})
            return checkpoint
        self.event._agentteams_recovery = {"task_id": "own", "team_id": "team", "generation": 1}
        agent = self.agent()
        agent.tools = model_tools.get_tool_definitions(enabled_toolsets=["file"])
        agent.valid_tool_names = {tool["function"]["name"] for tool in agent.tools}
        messages = []
        assistant = ChatCompletionMessage(role="assistant", content=None, tool_calls=[{"id": "native-write", "type": "function",
            "function": {"name": "write_file", "arguments": json.dumps({"path": "journal.txt", "content": "own journal fixture"})}}])
        with task_turn(self.event, self.prepared) as turn, patch.object(turn, "task_journal", side_effect=journal):
            turn.bind(agent, "journal-native", messages)
            agent._execute_tool_calls(assistant, messages, "journal-native", 0)
            self.assertEqual((self.output / "journal.txt").read_text(), "own journal fixture")
            self.assertEqual(actions, ["admit_tools", "begin_tools", "finish_tools"])
            self.assertEqual(checkpoint["operation"]["state"], "verified")
            checkpoint["operation"]["state"] = "unknown"
            with self.assertRaises(RecoveryConflict):
                agent._build_api_kwargs([{"role": "user", "content": "Do not infer over unconfirmed effects"}])

    def test_canonical_task_is_not_written_to_business_journal(self):
        from hermes_matrix.public_tasks import TaskTurn
        actions = []
        self.event._agentteams_recovery = {"task_id": "own", "team_id": "team", "generation": 1}
        with patch.object(TaskTurn, "task_journal", side_effect=lambda action, **fields: actions.append(action)):
            self.test_actual_native_model_wire_and_tool_receipt_are_bound()
        self.assertTrue(actions)
        self.assertEqual(set(actions), {"admit_tools"})

    def test_deferred_task_uses_same_canonical_authority(self):
        from hermes_matrix.public_tasks import is_canonical_task_call
        from openai.types.chat import ChatCompletionMessage

        calls = ChatCompletionMessage(role="assistant", content=None, tool_calls=[
            {"id": "deferred-" + name, "type": "function", "function": {
                "name": "tool_call", "arguments": json.dumps({"calls": [{
                    "name": name, "arguments": arguments,
                }]})
            }}
            for name, arguments in [
                (TASK_TOOL, {"action": "submit", "taskId": "own", "summary": "Done"}),
                ("read_file", {"path": "output.txt"}),
            ]
        ]).tool_calls
        self.assertTrue(is_canonical_task_call(calls[0]))
        self.assertFalse(is_canonical_task_call(calls[1]))

    def test_ordinary_native_canonical_task_does_not_double_journal(self):
        from hermes_matrix.message_execution import ordinary_turn, install_ordinary_guard
        install_ordinary_guard()
        matrix = SimpleNamespace(message_begin_tools=Mock(), message_finish_tools=Mock(),
            message_current=Mock(return_value={"checkpoint_digest": "source-current"}))
        ordinary = SimpleNamespace(matrix=matrix, identity={"event_key": "source"},
            session=SimpleNamespace(session_id="native-public-task"))
        token = ordinary_turn.set(ordinary)
        try:
            self.test_actual_native_model_wire_and_tool_receipt_are_bound()
        finally:
            ordinary_turn.reset(token)
        matrix.message_begin_tools.assert_not_called()
        matrix.message_finish_tools.assert_not_called()
        self.assertEqual(ordinary.checkpoint_digest, "source-current")

    def test_known_validation_refusal_can_be_corrected_in_the_same_native_turn(self):
        self.invalid_first = True
        self.test_actual_native_model_wire_and_tool_receipt_are_bound()
        rows = self.db.get_messages("native-public-task")
        self.assertTrue(any(row["role"] == "tool" and '"effect": "none"' in str(row.get("content")) for row in rows))

    def test_actual_native_writer_retains_refreshed_user_row_before_releasing_gate(self):
        agent = self.agent()
        agent._ensure_db_session()
        with task_turn(self.event, self.prepared) as turn:
            turn.pending = [{"eventId": "$late", "roomId": "!fixture:test", "userId": "@human:test", "body": "Corrected real input"}]
            turn.context["inputRefreshPending"] = True
            turn.checkpoint_refreshed(agent, [])
            self.assertNotIn("inputRefreshPending", turn.context)
        rows = self.db.get_messages(agent.session_id)
        self.assertTrue(any(row["role"] == "user" and row["platform_message_id"] == "$late" for row in rows))

    def test_native_terminal_override_is_visible_and_removed_after_scope(self):
        agent = self.agent()
        with task_turn(self.event, self.prepared) as turn:
            turn.bind(agent, "isolated-terminal-task", [])
            self.assertEqual(resolve_task_overrides("isolated-terminal-task")["cwd"], str(self.output))
        self.assertEqual(resolve_task_overrides("isolated-terminal-task"), {})

    def test_sent_delivery_uses_native_context_before_durable_ack_and_survives_replay(self):
        with patch.dict(os.environ, {"AGENTTEAMS_AGENT_ROLE": "manager"}):
            agent = self.agent()
            agent._ensure_db_session()
            delivery = {"eventId": "delivery-one", "status": "completed", "roomId": "!fixture:test",
                        "taskId": "main", "reportMessageId": "$report", "report": "Delivered app.py with 12 tests", "artifacts": []}
            with patch.object(self.bridge, "controller_json", return_value={"deliveries": [delivery]}):
                with task_turn(self.event) as turn:
                    messages = [{"role": "user", "content": "What was delivered?", "platform_message_id": "$source"}]
                    turn.bind(agent, "own-delivery-question", messages)
                replay = self.db.get_messages_as_conversation(agent.session_id)
                self.assertTrue(any(row["role"] == "system" and "12 tests" in row["content"] for row in replay))
                count = len(self.db.get_messages(agent.session_id))
                with task_turn(self.event) as turn:
                    turn.bind(agent, "own-delivery-question", replay)
                self.assertEqual(len(self.db.get_messages(agent.session_id)), count)

                agent.session_id = "never-written-native-context"
                agent._persist_session = Mock()
                agent._ensure_db_session()
                with task_turn(self.event) as turn:
                    with self.assertRaisesRegex(RuntimeError, "not retained"):
                        turn.bind(agent, "blocked-context", [])
                import hashlib
                key = hashlib.sha256(agent.session_id.encode()).hexdigest()
                self.assertFalse((Path(os.environ["HERMES_HOME"]) / "agentteams-context" / (key + ".json")).exists())

    def test_native_file_and_terminal_defaults_remain_isolated_for_two_tasks(self):
        from tools.file_tools import write_file_tool
        from tools.terminal_tool import terminal_tool
        barrier = threading.Barrier(2)
        def execute(identity):
            output = self.root / "tasks" / identity / "workspace"
            output.mkdir(parents=True)
            prepared = {"execution": identity, "context": {"outputDirectory": str(output)}}
            agent = SimpleNamespace(tools=[], valid_tool_names=set())
            native = "native-file-" + identity
            with task_turn(self.event, prepared) as turn:
                turn.bind(agent, native, [])
                barrier.wait(timeout=5)
                result = write_file_tool("result.txt", identity, task_id=native)
                self.assertTrue((output / "result.txt").exists(), str(result))
                command = terminal_tool("pwd", task_id=native, timeout=10)
                self.assertIn(str(output), command)
            return output
        with ThreadPoolExecutor(max_workers=2) as executor:
            outputs = list(executor.map(execute, ["first", "second"]))
        self.assertEqual((outputs[0] / "result.txt").read_text(), "first")
        self.assertEqual((outputs[1] / "result.txt").read_text(), "second")


if __name__ == "__main__":
    main()
