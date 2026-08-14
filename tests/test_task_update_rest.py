"""REST-layer tests for PATCH /api/tasks/{id} -- covers a bug class the
storage-layer tests can't see (storage.update_task's own None="leave
unchanged" convention is correct for its Python callers; the bug lived in how
app.py maps a REST JSON body's null/absent-key distinction onto that).

Runs against an isolated Starlette TestClient + temp DB, not the live server.

    python tests/test_task_update_rest.py
"""
from __future__ import annotations

import sys
import tempfile
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))
import storage  # noqa: E402


def _fresh_db() -> Path:
    tmp = Path(tempfile.mkstemp(suffix=".sqlite3")[1])
    storage.DB_PATH = tmp
    storage.init_db()
    return tmp


def _client():
    from starlette.testclient import TestClient
    import app as app_module
    return TestClient(app_module.app)


def test_explicit_null_rejected_for_non_tristate_fields() -> None:
    _fresh_db()
    client = _client()
    t = client.post("/api/tasks", json={"title": "x", "created_by": "claude"}).json()
    before_updated_at = t["updated_at"]

    for field in ("status", "description", "assignee", "priority"):
        res = client.patch(f"/api/tasks/{t['id']}", json={field: None})
        assert res.status_code == 400, f"{field}: null should be rejected, got {res.status_code}"
        assert field in res.json()["error"]

    # None of those rejected calls should have mutated the row -- especially
    # not updated_at, which would silently reorder the activity-sorted board
    # for a request that changed nothing (the exact bug OpenClaw caught live:
    # priority:null returned 200, left priority at 'normal', and still bumped
    # updated_at + emitted task_updated).
    after = client.get(f"/api/tasks/{t['id']}").json()
    assert after["updated_at"] == before_updated_at, "rejected update must not bump updated_at"
    assert after["priority"] == "normal"
    print("PASS: explicit null rejected (400) for status/description/assignee/priority, no side effects")


def test_due_date_null_still_allowed_as_the_one_real_tri_state() -> None:
    _fresh_db()
    client = _client()
    t = client.post("/api/tasks", json={"title": "x", "created_by": "claude", "due_date": "2026-05-01"}).json()
    res = client.patch(f"/api/tasks/{t['id']}", json={"due_date": None})
    assert res.status_code == 200, res.text
    assert res.json()["due_date"] is None
    print("PASS: due_date:null still works as the documented clear-semantic (only due_date has one)")


def test_empty_body_rejected() -> None:
    _fresh_db()
    client = _client()
    t = client.post("/api/tasks", json={"title": "x", "created_by": "claude"}).json()
    before_updated_at = t["updated_at"]

    res = client.patch(f"/api/tasks/{t['id']}", json={})
    assert res.status_code == 400, f"expected 400 for an empty body, got {res.status_code}"

    res2 = client.patch(f"/api/tasks/{t['id']}", json={"not_a_real_field": "x"})
    assert res2.status_code == 400, "a body with only unrecognized keys should also be rejected"

    after = client.get(f"/api/tasks/{t['id']}").json()
    assert after["updated_at"] == before_updated_at, "a no-op PATCH must not bump updated_at"
    print("PASS: empty/no-op PATCH bodies rejected (400), no phantom updated_at bump")


def test_due_date_error_message_is_stable_public_text() -> None:
    _fresh_db()
    client = _client()
    res = client.post("/api/tasks", json={"title": "x", "created_by": "claude", "due_date": "2026-02-30"})
    assert res.status_code == 400
    msg = res.json()["error"]
    assert "2026-02-30" in msg and "YYYY-MM-DD" in msg
    # Not leaking Python's internal exception wording (e.g. "unconverted data
    # remains" / "does not match format") -- just the stable public message.
    assert "invalid literal" not in msg and "unconverted" not in msg
    print("PASS: due_date validation error is a stable public message, not parser internals")


if __name__ == "__main__":
    test_explicit_null_rejected_for_non_tristate_fields()
    test_due_date_null_still_allowed_as_the_one_real_tri_state()
    test_empty_body_rejected()
    test_due_date_error_message_is_stable_public_text()
    print("All REST task-update tests passed.")
