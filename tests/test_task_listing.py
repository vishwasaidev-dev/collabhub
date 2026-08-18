"""Tests for list_tasks()'s default-scope + lightweight-comments behavior,
and the dashboard "Mark done" flow's REST route (task_complete attribution).

No pytest dependency for the storage-layer tests -- plain asserts, runnable
directly:
    python tests/test_task_listing.py
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


def test_list_tasks_defaults_to_non_done() -> None:
    _fresh_db()
    open_task = storage.create_task(title="still open", created_by="claude")
    done_task = storage.create_task(title="finished", created_by="claude")
    storage.update_task(done_task["id"], status="done")

    default = storage.list_tasks()
    ids = {t["id"] for t in default}
    assert open_task["id"] in ids, "an open task must appear in the default (unfiltered) call"
    assert done_task["id"] not in ids, "a done task must NOT appear in the default call"

    everything = storage.list_tasks(status="all")
    ids_all = {t["id"] for t in everything}
    assert {open_task["id"], done_task["id"]} <= ids_all, "status='all' must include done tasks too"

    only_done = storage.list_tasks(status="done")
    assert {t["id"] for t in only_done} == {done_task["id"]}, "an exact status filter still works as before"
    print("PASS: list_tasks() defaults to non-done, status='all'/exact status both still work")


def test_list_tasks_rejects_invalid_status() -> None:
    _fresh_db()
    try:
        storage.list_tasks(status="not-a-real-status")
        raise AssertionError("expected ValueError for an invalid status")
    except ValueError:
        pass
    print("PASS: list_tasks() raises on a garbage status instead of silently returning nothing")


def test_list_tasks_summarizes_comments_but_keeps_checklist_and_attachments() -> None:
    """The actual fix for the reported bug: a handful of tasks with sizeable
    comment threads must not turn an ordinary list_tasks() call into a
    response too large for a calling agent's own tool budget -- while
    checklist_items/checklist_progress must stay byte-for-byte identical to
    get_task()'s shape (a real, tested invariant from tranche 4; only
    comments gets a lighter shape here, nothing else)."""
    _fresh_db()
    t = storage.create_task(title="x", created_by="claude")
    storage.add_checklist_item(t["id"], "item one")
    big_text = "x" * 5000
    storage.add_comment(t["id"], "claude", big_text)
    storage.add_comment(t["id"], "openclaw", "short reply")

    via_get = storage.get_task(t["id"])
    via_list = next(x for x in storage.list_tasks() if x["id"] == t["id"])

    assert len(via_get["comments"]) == 2, "get_task must still return the full comment thread"
    assert "comments" not in via_list, "list_tasks must not carry the full comment thread"
    assert via_list["comment_count"] == 2, "list_tasks must report an accurate comment_count instead"
    assert via_list["checklist_items"] == via_get["checklist_items"], (
        "checklist_items must stay consistent between list_tasks and get_task"
    )
    assert via_list["checklist_progress"] == via_get["checklist_progress"]
    print("PASS: list_tasks summarizes comments as a count, keeps checklist/attachments in full")


def test_full_state_keeps_full_comments_for_the_dashboard() -> None:
    """full_state() (what the dashboard's board actually renders from) is
    deliberately untouched by the list_tasks change above -- board cards
    render comments inline, not on demand."""
    _fresh_db()
    t = storage.create_task(title="x", created_by="claude")
    storage.add_comment(t["id"], "claude", "a comment")

    via_state = next(x for x in storage.full_state()["tasks"] if x["id"] == t["id"])
    assert len(via_state["comments"]) == 1
    assert via_state["comments"][0]["text"] == "a comment"
    print("PASS: full_state() still hydrates full comments for the dashboard")


def test_rest_tasks_list_rejects_invalid_status() -> None:
    _fresh_db()
    client = _client()
    res = client.get("/api/tasks", params={"status": "not-a-real-status"})
    assert res.status_code == 400
    assert "status" in res.json()["error"]
    print("PASS: GET /api/tasks 400s on an invalid status instead of a bare empty list")


def test_dashboard_complete_route_records_who_closed_it_even_with_no_note() -> None:
    """The dashboard's 'Mark done' button lets the closing note be blank
    (unlike the MCP complete_task tool, where an agent always supplies a
    real summary) -- this is the fix that keeps that still attributable."""
    _fresh_db()
    client = _client()
    t = client.post("/api/tasks", json={"title": "x", "created_by": "claude"}).json()

    res = client.post(f"/api/tasks/{t['id']}/complete", json={"author": "vishwas", "summary": ""})
    assert res.status_code == 200
    task = res.json()
    assert task["status"] == "done"
    assert len(task["comments"]) == 1, "a blank note must still leave an attributed comment behind"
    assert "vishwas" in task["comments"][0]["text"]
    print("PASS: closing with a blank note still records who closed it")


def test_dashboard_complete_route_uses_the_given_note_when_present() -> None:
    _fresh_db()
    client = _client()
    t = client.post("/api/tasks", json={"title": "x", "created_by": "claude"}).json()

    res = client.post(f"/api/tasks/{t['id']}/complete", json={"author": "vishwas", "summary": "shipped it"})
    assert res.status_code == 200
    task = res.json()
    assert task["comments"][0]["text"] == "shipped it"
    assert task["comments"][0]["author"] == "vishwas"
    print("PASS: a real closing note is used verbatim, not overridden by the default")


if __name__ == "__main__":
    test_list_tasks_defaults_to_non_done()
    test_list_tasks_rejects_invalid_status()
    test_list_tasks_summarizes_comments_but_keeps_checklist_and_attachments()
    test_full_state_keeps_full_comments_for_the_dashboard()
    test_rest_tasks_list_rejects_invalid_status()
    test_dashboard_complete_route_records_who_closed_it_even_with_no_note()
    test_dashboard_complete_route_uses_the_given_note_when_present()
