"""Use Hermes' native transport and inbound policy to prepare retained history."""
import asyncio
from collections import deque
from copy import copy

from mautrix.types import BaseMessageEventContentFuncs, EventType, PaginationDirection, RoomID
from task_recovery import RecoveryConflict
from worker_matrix_history import backfill_round
from worker_matrix_state import matrix_event_key

from hermes_matrix.sync_state import Admission, _admission


async def recover_history(sync):
    joined = None

    async def fetch(room, token):
        nonlocal joined
        if joined is None:
            joined = set(await sync.client.get_joined_rooms())
        if room not in joined or not await sync.adapter._is_allowed_matrix_room_event(room):
            raise RecoveryConflict("History room is outside current membership or policy")
        page = await sync.client.get_messages(RoomID(room), PaginationDirection.BACKWARD, from_token=token)
        if page.start != token:
            raise RecoveryConflict("Matrix history response has a different starting token")
        return page.events, page.end

    async def prepare(room, events, until):
        inputs = []
        for native in events:
            if str(native.room_id) != room:
                raise RecoveryConflict("Matrix history response changed rooms")
            event_id = str(native.event_id)
            saved = await asyncio.to_thread(sync.matrix.input, matrix_event_key(room, event_id))
            if saved:
                if saved["room_id"] != room or saved["event_id"] != event_id:
                    raise RecoveryConflict("Retained history input does not match its source")
                inputs.append({k: saved[k] for k in ("room_id", "event_id", "input", "intent", "work") if k in saved})
                if event_id == until:
                    break
                continue
            if event_id == until:
                raise RecoveryConflict("Known history anchor is missing from the shared store")
            if native.type == EventType.ROOM_ENCRYPTED:
                if not sync.client.crypto:
                    raise RecoveryConflict("Matrix history decryption is unavailable")
                native = await sync.client.crypto.decrypt_megolm_event(native)
                if str(native.room_id) != room or str(native.event_id) != event_id:
                    raise RecoveryConflict("Decrypted history input changed identity")
            if native.type != EventType.ROOM_MESSAGE and str(native.type) != "org.agentteams.coordination":
                continue
            if isinstance(native.content, BaseMessageEventContentFuncs):
                native.content.trim_reply_fallback()  # Same normalization as mautrix Syncer.dispatch_event.
            item = await classify(sync.adapter, native)
            if item:
                inputs.append(item)
        return inputs

    return await backfill_round(sync.matrix, fetch, prepare)


async def classify(adapter, event):
    # The native deduplicator is an in-memory delivery cache. History admission
    # has its own immutable event identity and must not alter live delivery.
    policy = copy(adapter)
    policy._processed_events = deque(maxlen=adapter._processed_events.maxlen)
    policy._processed_events_set = set()
    policy._parked_voices = type(adapter._parked_voices)()
    policy._threads = copy(adapter._threads)
    async def keep_participation(thread_id):
        # Looking up old input must not mark the Worker as having replied.
        pass
    policy._threads.mark_async = keep_participation
    admission = Admission()
    scope = _admission.set(admission)
    try:
        task = asyncio.create_task(policy._on_room_message(event))
    finally:
        _admission.reset(scope)
    try:
        await admission.ready.wait()
        if admission.error:
            raise admission.error
        return admission.item
    finally:
        # Stop exactly where native policy accepted the input, before its
        # execution, context buffering, Task claim or approval callback.
        task.cancel()
        await asyncio.gather(task, return_exceptions=True)
