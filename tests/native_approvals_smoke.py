"""Run inside the pinned Hermes image: real queue, waiter, decisions and receipts."""
import asyncio
import json
import subprocess
import unittest
from types import SimpleNamespace
from unittest.mock import patch

from hermes_matrix.approvals import MatrixApprovals
from tools import approval as native
from tools import approval_context


class NativeApprovalSmoke(unittest.IsolatedAsyncioTestCase):
    async def test_native_approve_and_deny(self):
        for action, expected in [("approve", "once"), ("deny", "deny")]:
            sent = []
            ready = asyncio.Event()

            async def send(room, text, data):
                sent.append((room, dict(data)))
                if data["status"] == "pending":
                    ready.set()
                return "$event"

            transport = MatrixApprovals(send)
            loop = asyncio.get_running_loop()
            session = "matrix:smoke:" + action

            def notify(data):
                prompt = SimpleNamespace(
                    request_id=data["request_id"], session_key=session, chat_id="!room",
                    choices=["once", "deny"], command="printf native-proof", description="test gate",
                    text="Native command approval", metadata={"requester_user_id": "@owner"},
                )
                result = asyncio.run_coroutine_threadsafe(transport.prompt(prompt), loop).result(10)
                assert result.success, result

            with patch.object(approval_context, "_get_approval_timeout", return_value=5):
                wait = asyncio.create_task(asyncio.to_thread(native._await_gateway_decision,
                    session, notify, {"command": "printf native-proof", "description": "test gate", "pattern_keys": ["smoke"]}))
                await asyncio.wait_for(ready.wait(), 10)
                data = sent[0][1]
                body = f"/agentteams-approval {action} {data['request_id']} {data['context_id']}"
                self.assertFalse(wait.done())
                await transport.answer("!other", "@owner", body)
                await transport.answer("!room", "@intruder", body)
                await transport.answer("!room", "@owner", body.replace(data["context_id"], "stale"))
                self.assertFalse(wait.done())
                await transport.answer("!room", "@owner", "@worker " + body)
                result = await asyncio.wait_for(wait, 10)
                self.assertEqual(result["choice"], expected)
                proof = subprocess.check_output(["sh", "-c", "printf native-proof"], text=True) if result["choice"] == "once" else ""
                self.assertEqual(proof, "native-proof" if action == "approve" else "")
                for _ in range(10):
                    if len(sent) == 2:
                        break
                    await asyncio.sleep(0)
                self.assertEqual(sent[-1][1]["status"], "approved" if action == "approve" else "rejected")
                await transport.answer("!room", "@owner", body)
                self.assertFalse(transport.pending)
                print(json.dumps({"runtime": "hermes", "decision": action, "native_choice": result["choice"], "proof": proof}))

    async def test_native_timeout(self):
        sent = []
        ready = asyncio.Event()
        async def send(room, text, data):
            sent.append(dict(data))
            ready.set()
            return "$timeout"
        transport = MatrixApprovals(send)
        loop = asyncio.get_running_loop()
        def notify(data):
            prompt = SimpleNamespace(request_id=data["request_id"], session_key="timeout", chat_id="!room",
                choices=["once", "deny"], command="printf timeout", description="", text="test", metadata={})
            asyncio.run_coroutine_threadsafe(transport.prompt(prompt), loop).result(10)
        with patch.object(approval_context, "_get_approval_timeout", return_value=0.05):
            result = await asyncio.to_thread(native._await_gateway_decision, "timeout", notify,
                {"command": "printf timeout", "pattern_keys": ["smoke-timeout"]})
        self.assertFalse(result["resolved"])
        for _ in range(10):
            if len(sent) == 2:
                break
            await asyncio.sleep(0)
        self.assertEqual(sent[-1]["status"], "expired")


if __name__ == "__main__":
    unittest.main()
