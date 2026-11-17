"""Commit admitted timeline inputs in Matrix order before releasing native work."""
import asyncio
import logging
from contextvars import ContextVar

from mautrix.client.syncer import SyncStream
from mautrix.types import EventType
from task_recovery import RecoveryConflict
from worker_matrix_state import WorkerMatrix, matrix_event_key


_decoding = ContextVar("hermes_sync_decoding", default=None)
_admission = ContextVar("hermes_sync_admission", default=None)
_TIMELINE = SyncStream.JOINED_ROOM | SyncStream.TIMELINE


class Admission:
    """The native policy pauses at its existing acceptance point until the batch commits."""

    def __init__(self):
        self.ready = asyncio.Event()
        self.release = asyncio.Event()
        self.item = self.error = None

    async def retain(self, matrix, room, event_id, source, intent, task_id):
        self.item = await asyncio.to_thread(matrix.prepare_input, room, event_id, source, intent, task_id=task_id)
        self.ready.set()
        await self.release.wait()
        return matrix_event_key(room, event_id)


def current_admission():
    return _admission.get()


def forget_failed_input(adapter, event_id):
    adapter._processed_events_set.discard(event_id)
    if event_id in adapter._processed_events:
        adapter._processed_events.remove(event_id)


class NativeSync:
    def __init__(self, adapter, client):
        self.adapter, self.client = adapter, client
        self.matrix = WorkerMatrix("hermes", device_delivery=True)
        self.response = self.consumer = self.error = None
        self.initialized = False
        self.dispatch_event = self.client.dispatch_event

    def install(self):
        native_sync = self.client.sync

        async def sync(*args, **kwargs):
            self.error = self.response = None
            try:
                snapshot = await asyncio.to_thread(self.matrix.load)
                self.consumer = self.matrix.consumer(snapshot, account=str(self.client.mxid),
                    device=str(self.client.device_id or ""), format="mautrix/sync-v3")
                kwargs["since"] = self.consumer["token"] or None
                self.response = await native_sync(*args, **kwargs)
                return self.response
            except Exception as error:
                self.error = error
                raise

        def capture(event, source):
            # mautrix redispatches the clear event from its background decryptor.
            # Capture any decoded type, including reactions, under the raw ID.
            batch = _decoding.get()
            if batch is not None and event is not None and source == _TIMELINE and event.type != EventType.ROOM_ENCRYPTED:
                slot = batch.get((str(event.room_id), str(event.event_id)))
                if slot is not None:
                    if not slot.done():
                        slot.set_result((event, source))
                    return []
            return self.dispatch_event(event, source)

        self.client.sync = sync
        self.client.dispatch_event = capture

    async def absorb(self, data, dispatch):
        if data is not self.response or self.consumer is None:
            raise RecoveryConflict("Sync response has no matching shared cursor")
        rooms = data.get("rooms", {}).get("join", {})
        futures = {(room, raw["event_id"]): asyncio.get_running_loop().create_future()
            for room, entry in rooms.items() for raw in entry.get("timeline", {}).get("events", [])
            if raw.get("type") in {"m.room.message", "m.room.encrypted", "m.reaction"}}
        held = []
        committed = False
        try:
            # Let the SDK process state, invites and queued encryption keys.
            # Withhold its early put_next_batch and capture decoded timeline work.
            decoding = _decoding.set(futures)
            try:
                await dispatch({key: value for key, value in data.items() if key != "next_batch"})
                if futures:
                    await asyncio.wait_for(asyncio.gather(*futures.values()), timeout=30)
            finally:
                _decoding.reset(decoding)

            snapshot = await asyncio.to_thread(self.matrix.load)
            timelines = []
            deferred = []
            for room, entry in rooms.items():
                if "timeline" not in entry:
                    continue
                timeline = entry["timeline"]
                inputs = []
                for raw in timeline.get("events", []):
                    slot = futures.get((room, raw["event_id"]))
                    if slot is None:
                        continue
                    event, source = slot.result()
                    if event.type != EventType.ROOM_MESSAGE:
                        deferred.append((event, source))
                        continue
                    key = matrix_event_key(room, str(event.event_id))
                    prior = await asyncio.to_thread(self.matrix.input, key)
                    if prior and (key in snapshot["state"]["pending"] or prior.get("execution") or prior.get("native_commit")):
                        # Already indexed work is recovered from shared state. A
                        # put-before-CAS orphan alone is not evidence of admission.
                        inputs.append({k: prior[k] for k in ("room_id", "event_id", "input", "intent", "work") if k in prior})
                        continue
                    admission = Admission()
                    scope = _admission.set(admission)
                    try:
                        tasks = self.dispatch_event(event, source)
                        if not tasks:
                            raise RecoveryConflict("Native message handler unavailable")
                        dispatched = asyncio.gather(*tasks)
                        held.append((str(event.event_id), room, admission, dispatched))
                        # A plugin middleware can consume an event before our
                        # callback. Its dispatcher then finishes without admission.
                        dispatched.add_done_callback(lambda _, a=admission: a.ready.set())
                        await admission.ready.wait()
                        if admission.error:
                            raise admission.error
                        if admission.item:
                            inputs.append(admission.item)
                    finally:
                        _admission.reset(scope)
                timelines.append({"room_id": room, "inputs": inputs, "limited": bool(timeline.get("limited")),
                    "prev_batch": timeline.get("prev_batch")})
            saved = await asyncio.to_thread(self.matrix.sync, snapshot, self.consumer, data.get("next_batch"), timelines)
            committed = True
            for event_id, room, admission, dispatched in held:
                if saved["state"]["rooms"].get(room, {}).get("gaps") and admission.item:
                    # The immutable input remains pending for recovery after pagination.
                    dispatched.cancel()
                    await asyncio.gather(dispatched, return_exceptions=True)
                    forget_failed_input(self.adapter, event_id)
                else:
                    admission.release.set()
                    await dispatched
                    if admission.error:
                        raise admission.error
            for event, source in deferred:
                await asyncio.gather(*self.dispatch_event(event, source))
            await self.client.sync_store.put_next_batch(data["next_batch"])
            self.initialized = True
            if any(room.get("gaps") for room in saved["state"]["rooms"].values()):
                from hermes_matrix.sync_history import recover_history
                for room_id, failure in (await recover_history(self)).items():
                    logging.getLogger(__name__).warning("Matrix history pending in %s: %s", room_id, failure)
            return data["next_batch"]
        except BaseException as error:
            self.error = error
            for event_id, _, _, dispatched in held:
                if not dispatched.done():
                    dispatched.cancel()
                if not committed:
                    forget_failed_input(self.adapter, event_id)
            await asyncio.gather(*(done for _, _, _, done in held), return_exceptions=True)
            raise
