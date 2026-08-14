"""Tests for the search index (tranche 3): same-transaction sync and
reindex-on-change.

OpenClaw's pre-build review (2026-08-14) specifically asked for tests
proving (1) a task's description edit is reflected in search results and
the stale content is no longer findable, and (2) that the search-index
write and the canonical mutation share one transaction -- an injected
failure in the index write must roll back the source row too, not leave
a task/comment/note/message that search can never find (silent drift).

No pytest dependency -- plain asserts, runnable directly:
    python tests/test_search_index.py
"""
from __future__ import annotations

import sqlite3
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


def test_backfill_indexes_preexisting_data() -> None:
    _fresh_db()
    task = storage.create_task(title="preexisting task", description="alpha content", created_by="test")
    # Simulate search_index having been created fresh (already true here,
    # since _fresh_db() calls init_db() once) -- re-running init_db() must
    # NOT wipe the index (search_index_existed guard), and a from-scratch
    # rebuild must still find everything that's there.
    storage.init_db()
    assert storage.search("alpha")["results"], "backfill/rebuild lost pre-existing data"
    print("PASS: backfill/rebuild covers pre-existing data")


def test_reindex_on_description_change() -> None:
    _fresh_db()
    task = storage.create_task(title="reindex test", description="original wording", created_by="test")
    before = storage.search("original")
    assert any(r["source_type"] == "task" and r["source_id"] == task["id"] for r in before["results"]), (
        "task not indexed on create"
    )

    storage.update_task(task["id"], description="updated wording entirely")

    after_old = storage.search("original")
    assert not any(r["source_type"] == "task" and r["source_id"] == task["id"] for r in after_old["results"]), (
        "stale description content is still searchable after the edit -- reindex-on-update is broken"
    )
    after_new = storage.search("updated")
    assert any(r["source_type"] == "task" and r["source_id"] == task["id"] for r in after_new["results"]), (
        "new description content is not searchable after the edit"
    )
    print("PASS: task description edit is reflected in search (old content gone, new content found)")


def test_index_write_failure_rolls_back_the_canonical_mutation() -> None:
    _fresh_db()
    tasks_before = len(storage.list_tasks())

    # Force the index write to fail by dropping search_index -- the INSERT
    # inside _index_for_search will raise sqlite3.OperationalError. Since
    # create_task and _index_for_search run inside the SAME `with _connect()`
    # block/transaction, that exception must roll back the task INSERT too.
    with storage._connect() as conn:
        conn.execute("DROP TABLE search_index")

    try:
        storage.create_task(title="should not survive", description="x", created_by="test")
        raise AssertionError("expected an exception when the index write fails")
    except sqlite3.OperationalError:
        pass

    tasks_after = len(storage.list_tasks())
    assert tasks_after == tasks_before, (
        f"task row leaked despite the index write failing: {tasks_before} before, {tasks_after} after "
        "-- source and index are no longer in one transaction"
    )
    print("PASS: a failed index write rolls back the canonical mutation too (same transaction)")


def test_comment_and_note_and_chat_message_are_indexed() -> None:
    _fresh_db()
    task = storage.create_task(title="parent task", description="", created_by="test")
    storage.add_comment(task["id"], "test", "distinctcommentword")
    storage.post_note("test", "distinctnoteword")
    sess = storage.start_chat_session(started_by="test")
    storage.send_chat_message(sess["id"], "test", "distinctchatword")

    assert storage.search("distinctcommentword")["results"][0]["source_type"] == "comment"
    assert storage.search("distinctnoteword")["results"][0]["source_type"] == "note"
    assert storage.search("distinctchatword")["results"][0]["source_type"] == "chat_message"
    print("PASS: comments, notes, and chat messages are all indexed on creation")


if __name__ == "__main__":
    test_backfill_indexes_preexisting_data()
    test_reindex_on_description_change()
    test_index_write_failure_rolls_back_the_canonical_mutation()
    test_comment_and_note_and_chat_message_are_indexed()
    print("All search-index tests passed.")
