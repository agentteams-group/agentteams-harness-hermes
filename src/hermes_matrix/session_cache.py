"""Hermes-native persistence facts for the shared Worker session descriptor."""
import hashlib
import json


def session_fingerprint(store, entry):
    if entry is None or entry.suspended or entry.expiry_finalized:
        return None
    try:
        db = store._db_for_key(entry.session_key)
        if db is None:
            return None
        tip = db.get_compression_tip(entry.session_id) or entry.session_id
        row = db.get_session(tip)
        if not row or row.get("end_reason") is not None:
            return None
        # Read the native rows without load_transcript's alternation repair.
        # Follow compression lineage, retaining the SDK's insertion order.
        rows = db.get_messages(tip, include_ancestors=True)
        if not any(row.get("role") == "user" and row.get("content") for row in rows):
            return None
        data = json.dumps({"tip": tip, "messages": rows}, sort_keys=True,
                          separators=(",", ":"), ensure_ascii=False).encode()
        return hashlib.sha256(data).hexdigest()
    except Exception:
        # An unreadable native cache cannot become shared execution authority.
        return None
