"""Native persistence facts must come from the SDK database, not its routing ID."""
from types import SimpleNamespace
from unittest.mock import Mock

from hermes_matrix.session_cache import session_fingerprint
from test_overlay_image_downgrade import _load_overlay_with_fake_gateway


def fixture():
    db = Mock()
    db.get_compression_tip.return_value = "native-session"
    db.get_session.return_value = {"end_reason": None}
    db.get_messages.return_value = [{"id": 1, "role": "user", "content": "persisted input"}]
    store = Mock()
    store._db_for_key.return_value = db
    entry = SimpleNamespace(session_key="route", session_id="native-session", suspended=False, expiry_finalized=False)
    return store, db, entry


def test_native_fingerprint_changes_with_persisted_content_and_compression_tip():
    store, db, entry = fixture()
    first = session_fingerprint(store, entry)
    assert first and len(first) == 64
    db.get_messages.return_value.append({"id": 2, "role": "assistant", "content": "completed one"})
    second = session_fingerprint(store, entry)
    assert second and second != first
    db.get_compression_tip.return_value = "compression-child"
    assert session_fingerprint(store, entry) != second
    db.get_messages.assert_called_with("compression-child", include_ancestors=True)


def test_missing_empty_closed_and_suspended_native_sessions_are_not_usable():
    for case in ["missing-db", "missing-row", "empty", "system-only", "closed", "suspended", "expired", "read-error"]:
        store, db, entry = fixture()
        if case == "missing-db":
            store._db_for_key.return_value = None
        elif case == "missing-row":
            db.get_session.return_value = None
        elif case == "empty":
            db.get_messages.return_value = []
        elif case == "system-only":
            db.get_messages.return_value = [{"role": "system", "content": "metadata"}]
        elif case == "closed":
            db.get_session.return_value = {"end_reason": "reset"}
        elif case == "suspended":
            entry.suspended = True
        elif case == "expired":
            entry.expiry_finalized = True
        else:
            db.get_messages.side_effect = OSError("read failed")
        assert session_fingerprint(store, entry) is None, case


def test_native_admission_and_final_delivery_preserve_execution_fences(monkeypatch):
    module, _ = _load_overlay_with_fake_gateway(monkeypatch)
    identity = {"process_id": "process", "instance_id": "actual/start", "generation": 4}
    task = {"status": "in_progress", "execution": {**identity, "runtime": "hermes", "state": "active"}}
    assert module.MatrixAdapter._owns_recovery(task, identity, "process")
    task["execution"]["state"] = "released"
    assert not module.MatrixAdapter._owns_recovery(task, identity, "process")
    assert module.MatrixAdapter._owns_recovery(task, identity, "process", delivery=True)
    task["status"] = "completed"
    assert not module.MatrixAdapter._owns_recovery(task, identity, "process")
    assert module.MatrixAdapter._owns_recovery(task, identity, "process", delivery=True)
    task["status"] = "in_progress"
    task["execution"]["state"] = "needs_verification"
    assert not module.MatrixAdapter._owns_recovery(task, identity, "process", delivery=True)
    task["execution"]["state"] = "active"
    for field in ("process_id", "instance_id", "generation", "runtime"):
        changed = {**task, "execution": {**task["execution"], field: "old"}}
        assert not module.MatrixAdapter._owns_recovery(changed, identity, "process", delivery=True)
    task["status"] = "cancelled"
    assert not module.MatrixAdapter._owns_recovery(task, identity, "process", delivery=True)
