"""Actual SDK writer/turn ordering; Controller and model endpoint are fixtures."""
import json
import os
from pathlib import Path
import tempfile
import threading
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from unittest import TestCase, main
from unittest.mock import Mock, patch

from agent import turn_context, turn_context_compaction
from agent.message_metadata import append_message
from hermes_state import SessionDB
from run_agent import AIAgent
from task_recovery import RecoveryConflict, RecoveryUnavailable
from hermes_matrix.input_commit import commit_input, install_native_input_guard


class InputCommitSmoke(TestCase):
    def setUp(self):
        self.tmp = tempfile.TemporaryDirectory()
        self.addCleanup(self.tmp.cleanup)
        self.env = patch.dict(os.environ, {"HERMES_HOME": self.tmp.name,
            "AGENTTEAMS_CONTROLLER_URL": "http://authority.invalid", "AGENTTEAMS_PROCESS_ID": "actual-process",
            "AGENTTEAMS_AGENT_ROLE": "worker"})
        self.env.start()
        self.addCleanup(self.env.stop)
        self.db = SessionDB(Path(self.tmp.name) / "state.db")
        self.addCleanup(self.db.close)
        self.matrix = Mock()
        self.matrix.input.return_value = {"room_id": "!room:test", "event_id": "$source"}
        self.matrix.native_current.return_value = False
        factory = patch("hermes_matrix.input_commit.WorkerMatrix", return_value=self.matrix)
        self.factory = factory.start()
        self.addCleanup(factory.stop)

    def agent(self, url="http://127.0.0.1:1/v1"):
        agent = AIAgent(model="input-fixture", api_key="synthetic", base_url=url, provider="custom",
            platform="matrix", chat_id="!room:test", session_id="native-source", session_db=self.db,
            skip_memory=True, skip_context_files=True, skip_background_review=True,
            enabled_toolsets=[], quiet_mode=True, max_iterations=1)
        agent._cached_system_prompt = "Reply briefly."
        agent._persist_user_message_platform_id = "$source"
        agent._persist_user_message_idx = 0
        agent._persist_user_message_override = None
        return agent

    def messages(self):
        messages = []
        append_message(messages, {"role": "user", "content": "Persist this actual input", "platform_message_id": "$source"})
        return messages

    def test_native_row_readback_and_repeated_flush_do_not_duplicate_input(self):
        agent, messages = self.agent(), self.messages()
        commit_input(agent, messages, [])
        self.assertIsInstance(messages[0]["_row_id"], int)
        rows = self.db.get_messages(agent.session_id)
        self.assertEqual(len(rows), 1)
        self.assertEqual(rows[0]["id"], messages[0]["_row_id"])
        args = self.matrix.native_committed.call_args.args
        self.assertEqual(args[2], agent.session_id)
        self.assertEqual(len(args[3]), 64)
        self.matrix.native_current.return_value = True
        commit_input(agent, messages, [])
        self.assertEqual(len(self.db.get_messages(agent.session_id)), 1)
        self.assertEqual(self.matrix.native_committed.call_count, 1)

    def test_missing_input_disabled_writer_and_silently_failed_save_cannot_commit(self):
        for failure in ["missing", "disabled", "save", "read", "fact-read", "fact-write", "foreign"]:
            with self.subTest(failure=failure):
                agent, messages = self.agent(), self.messages()
                agent.session_id += "-" + failure
                self.matrix.reset_mock()
                self.matrix.input.return_value = None if failure == "missing" else {}
                agent._persist_disabled = failure == "disabled"
                if failure == "save":
                    # The SDK writer can log and swallow an exception. A no-op
                    # writer must not inherit proof from some older user row.
                    agent._persist_session = Mock()
                if failure == "foreign":
                    messages[0]["platform_message_id"] = "$other"
                operation = patch.object(self.db, "get_messages", side_effect=OSError("read failed")) if failure == "read" else patch.dict({}, {})
                self.matrix.native_input.side_effect = RecoveryUnavailable("fact read failed") if failure == "fact-read" else None
                self.matrix.native_committed.side_effect = RecoveryUnavailable("fact write failed") if failure == "fact-write" else None
                with operation, self.assertRaises((RecoveryConflict, RecoveryUnavailable, OSError)):
                    commit_input(agent, messages, [])
                if failure != "fact-write":
                    self.matrix.native_committed.assert_not_called()

    def test_old_same_event_row_does_not_prove_a_failed_current_native_save(self):
        agent, old = self.agent(), self.messages()
        commit_input(agent, old, [])
        self.matrix.reset_mock()
        agent._persist_session = Mock()
        with self.assertRaises(RecoveryConflict):
            commit_input(agent, self.messages(), [])
        self.matrix.native_committed.assert_not_called()

    def test_startup_without_source_never_fabricates_input_facts(self):
        agent = self.agent()
        agent._persist_user_message_platform_id = "recovery:team/T:2"
        commit_input(agent, self.messages(), [])
        self.factory.assert_not_called()

    def test_real_turn_commits_before_compaction_and_model_and_fact_failure_stops_both(self):
        order, requests, blocked_requests = [], [], []
        requests_by_port = {}
        class Model(BaseHTTPRequestHandler):
            def log_message(self, *args):
                pass
            def do_POST(self):
                payload = json.loads(self.rfile.read(int(self.headers["Content-Length"])))
                if self.path == "/api/show" and set(payload) == {"name"}:
                    # Native local-provider capability lookup carries only the
                    # model name. It is not a request to infer on user input.
                    self.send_response(404)
                    self.end_headers()
                    return
                requests_by_port[self.server.server_port].append(payload)
                order.append("model")
                if payload.get("stream"):
                    chunks = [{"id": "fixture", "object": "chat.completion.chunk", "created": 1,
                        "model": "input-fixture", "choices": [{"index": 0,
                            "delta": {"content": "NATIVE_GUARD_OK"}, "finish_reason": None}]},
                        {"id": "fixture", "object": "chat.completion.chunk", "created": 1,
                            "model": "input-fixture", "choices": [{"index": 0, "delta": {}, "finish_reason": "stop"}]}]
                    body = ("".join("data: " + json.dumps(chunk) + "\n\n" for chunk in chunks) + "data: [DONE]\n\n").encode()
                    self.send_response(200)
                    self.send_header("Content-Type", "text/event-stream")
                    self.send_header("Content-Length", str(len(body)))
                    self.end_headers()
                    self.wfile.write(body)
                    return
                body = json.dumps({"id": "fixture", "object": "chat.completion", "created": 1,
                    "model": "input-fixture", "choices": [{"index": 0, "message": {
                        "role": "assistant", "content": "NATIVE_GUARD_OK"}, "finish_reason": "stop"}],
                    "usage": {"prompt_tokens": 1, "completion_tokens": 1, "total_tokens": 2}}).encode()
                self.send_response(200)
                self.send_header("Content-Type", "application/json")
                self.send_header("Content-Length", str(len(body)))
                self.end_headers()
                self.wfile.write(body)
        server = ThreadingHTTPServer(("127.0.0.1", 0), Model)
        blocked_server = ThreadingHTTPServer(("127.0.0.1", 0), Model)
        requests_by_port.update({server.server_port: requests, blocked_server.server_port: blocked_requests})
        threading.Thread(target=server.serve_forever, daemon=True).start()
        threading.Thread(target=blocked_server.serve_forever, daemon=True).start()
        self.addCleanup(server.server_close)
        self.addCleanup(server.shutdown)
        self.addCleanup(blocked_server.server_close)
        self.addCleanup(blocked_server.shutdown)
        native_compaction = turn_context_compaction.run_turn_start_compaction
        def compact(*args, **kwargs):
            order.append("compaction")
            return native_compaction(*args, **kwargs)
        def fact(*args):
            self.assertTrue(any(row.get("platform_message_id") == "$source"
                for row in self.db.get_messages(args[2])))
            order.append("fact")
        self.matrix.native_committed.side_effect = fact
        original = turn_context._collect_pre_llm_call_context
        with patch.object(turn_context, "_collect_pre_llm_call_context", original), \
                patch.object(turn_context_compaction, "run_turn_start_compaction", side_effect=compact):
            install_native_input_guard()
            installed = turn_context._collect_pre_llm_call_context
            install_native_input_guard()
            self.assertIs(turn_context._collect_pre_llm_call_context, installed)
            agent = self.agent(f"http://127.0.0.1:{server.server_port}/v1")
            result = agent.run_conversation("Persist this actual input", persist_user_platform_id="$source")
            self.assertEqual(result["final_response"], "NATIVE_GUARD_OK")
            self.assertLess(order.index("fact"), order.index("compaction"))
            self.assertLess(order.index("fact"), order.index("model"), repr(requests))
            order.clear()
            self.matrix.native_committed.side_effect = RecoveryUnavailable("current start/CAS unavailable")
            # A successful turn can still title its session asynchronously.
            # A separate endpoint proves that the failed turn sends nothing,
            # including auxiliary calls, without counting that earlier work.
            blocked = self.agent(f"http://127.0.0.1:{blocked_server.server_port}/v1")
            blocked.session_id = "blocked-native"
            try:
                result = blocked.run_conversation("Do not infer before persistence", persist_user_platform_id="$source")
            except RecoveryUnavailable:
                result = {"failed": True}
            self.assertFalse(blocked_requests)
            self.assertNotIn("compaction", order)
            self.assertNotEqual(result.get("final_response"), "NATIVE_GUARD_OK")


if __name__ == "__main__":
    main()
