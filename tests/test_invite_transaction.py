"""Storage-layer test for start_chat_session's atomic invite-link creation.

OpenClaw's review (tranche 2, 2026-08-14) correctly pointed out that the
"invalid invite_task_id rolls back the whole session insert" claim was only
ever exercised indirectly (via link_task_session's separate existence
checks) -- never proven against start_chat_session itself, because the only
session available to test against live was already active (#4) and ending it
to test would have disrupted the real collaboration. This runs against a
disposable temp DB instead, so it can actually exercise both paths:

  1. An invalid invite_task_id must leave behind NEITHER a chat_sessions row
     NOR a task_chat_sessions row -- the whole insert rolls back together.
  2. A valid invite_task_id must create BOTH rows, atomically, in one commit.

No pytest dependency -- plain asserts, runnable directly:
    python tests/test_invite_transaction.py
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


def test_invalid_invite_task_id_rolls_back_session_insert() -> None:
    _fresh_db()
    sessions_before = len(storage.list_chat_sessions(limit=100))
    try:
        storage.start_chat_session(started_by="test", invite_task_id=99999)
        raise AssertionError("expected NotFoundError for a nonexistent invite_task_id")
    except storage.NotFoundError:
        pass

    sessions_after = len(storage.list_chat_sessions(limit=100))
    assert sessions_after == sessions_before, (
        f"session row leaked despite the invalid invite_task_id: "
        f"{sessions_before} before, {sessions_after} after"
    )

    with storage._connect() as conn:
        row = conn.execute("SELECT COUNT(*) AS c FROM task_chat_sessions").fetchone()
        assert row["c"] == 0, "a relation row leaked despite the session insert failing"
    print("PASS: invalid invite_task_id leaves neither a session nor a relation row")


def test_valid_invite_task_id_creates_both_rows_atomically() -> None:
    _fresh_db()
    task = storage.create_task(title="invite target", created_by="test")

    sess = storage.start_chat_session(started_by="test", invite_task_id=task["id"])
    assert sess["invite_task_id"] == task["id"]

    with storage._connect() as conn:
        sess_row = conn.execute("SELECT * FROM chat_sessions WHERE id=?", (sess["id"],)).fetchone()
        assert sess_row is not None, "session row was not created"

        link_row = conn.execute(
            "SELECT * FROM task_chat_sessions WHERE task_id=? AND session_id=? AND relation_type='invite'",
            (task["id"], sess["id"]),
        ).fetchone()
        assert link_row is not None, "invite relation row was not created alongside the session"

    task_after = storage.get_task(task["id"])
    assert any(s["session_id"] == sess["id"] and s["relation_type"] == "invite" for s in task_after["chat_sessions"]), (
        "get_task's chat_sessions summary doesn't reflect the auto-created invite link"
    )
    print("PASS: valid invite_task_id creates both the session and the invite relation row in one commit")


if __name__ == "__main__":
    test_invalid_invite_task_id_rolls_back_session_insert()
    test_valid_invite_task_id_creates_both_rows_atomically()
    print("All invite-transaction tests passed.")
