"""Send a verified saved reply through Hermes' current Matrix account."""
import asyncio

from mautrix.types import EventType, PaginationDirection, RoomID
from task_recovery import RecoveryConflict
from worker_matrix_delivery import publish_saved


async def deliver(client, room, checkpoint, text, content, bind):
    room = RoomID(room)

    async def whoami():
        account = await client.whoami()
        if account.user_id != client.mxid:
            raise RecoveryConflict("Matrix access token belongs to another Worker account")
        return account.user_id, account.device_id

    async def find(txn, body, sender):
        token = None
        seen = set()
        while token not in seen:
            seen.add(token)
            page = await client.get_messages(room, PaginationDirection.BACKWARD, from_token=token,
                limit=100, filter_json={"senders": [sender], "types": ["m.room.message", "m.room.encrypted"]})
            for event in page.events:
                if event.room_id != room or event.sender != sender:
                    continue
                if event.type == EventType.ROOM_ENCRYPTED:
                    if not client.crypto:
                        continue
                    event = await client.crypto.decrypt_megolm_event(event)
                raw = event.serialize()
                msg = raw.get("content", {})
                marker = msg.get("org.agentteams.reply_txn")
                if marker is None and not checkpoint.get("reply_attempt"):
                    marker = (raw.get("unsigned") or {}).get("transaction_id")
                if (raw.get("room_id") == room and raw.get("sender") == sender and raw.get("type") == "m.room.message"
                        and msg.get("msgtype") == "m.text" and marker == txn and msg.get("body") == body):
                    return raw["event_id"]
            if not page.end or not page.events:
                break
            token = page.end
        return None

    async def send(txn):
        return await client.send_message_event(room, EventType.ROOM_MESSAGE, {**content,
            "body": text, "org.agentteams.reply_txn": txn}, txn_id=txn)

    return await publish_saved(checkpoint, text, whoami, bind, find, send)


async def task_reply(adapter, recovery, task_id, team, room, saved, text):
    execution = saved["execution"]
    async def bind(cp):
        fresh = await asyncio.to_thread(recovery.checkpoint, task_id, team, cp)
        check_binding(execution, cp, fresh)
    return await deliver(adapter._client, room, execution["checkpoint"], text,
        {"msgtype": "m.text", "org.agentteams.progress_only": True}, bind)


def check_binding(execution, cp, fresh):
    actual = fresh["execution"]
    if (actual["state"] != "active" or actual["checkpoint"].get("reply_attempt") != cp["reply_attempt"]
            or actual["checkpoint"].get("reply_txn") != cp["reply_txn"]
            or any(actual.get(key) != execution.get(key) for key in ("instance_id", "process_id", "generation"))):
        raise RecoveryConflict("Saved reply moved to another execution before its device bound")
