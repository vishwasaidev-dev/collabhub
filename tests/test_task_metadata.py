"""Tests for task metadata (tranche 4): priority, due_date tri-state
semantics, and checklist items.

No pytest dependency -- plain asserts, runnable directly:
    python tests/test_task_metadata.py
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


def test_priority_validation() -> None:
    _fresh_db()
    t = storage.create_task(title="x", priority="high")
    assert t["priority"] == "high"
    try:
        storage.create_task(title="x", priority="critical")
        raise AssertionError("expected ValueError for invalid priority")
    except ValueError:
        pass
    print("PASS: priority validated on create")


def test_due_date_validation_rejects_invalid_calendar_dates() -> None:
    _fresh_db()
    try:
        storage.create_task(title="x", due_date="2026-02-30")  # Feb has no 30th
        raise AssertionError("expected ValueError for an invalid calendar date")
    except ValueError:
        pass
    # A real leap day is valid; the same day in a non-leap year is not.
    t = storage.create_task(title="x", due_date="2024-02-29")
    assert t["due_date"] == "2024-02-29"
    try:
        storage.create_task(title="y", due_date="2026-02-29")  # 2026 is not a leap year
        raise AssertionError("expected ValueError for Feb 29 in a non-leap year")
    except ValueError:
        pass
    print("PASS: due_date validated with a real calendar parser, not a regex")


def test_due_date_tri_state_omit_vs_null_vs_set() -> None:
    _fresh_db()
    t = storage.create_task(title="x", due_date="2026-03-15")
    assert t["due_date"] == "2026-03-15"

    # Omitted entirely -> unchanged
    t2 = storage.update_task(t["id"], description="changed")
    assert t2["due_date"] == "2026-03-15", "omitting due_date must leave it unchanged"

    # Explicit None -> cleared
    t3 = storage.update_task(t["id"], due_date=None)
    assert t3["due_date"] is None, "explicit due_date=None must clear it"

    # Explicit value -> set
    t4 = storage.update_task(t["id"], due_date="2026-04-01")
    assert t4["due_date"] == "2026-04-01"
    print("PASS: due_date tri-state (omitted/explicit-null/set) all behave correctly")


def test_checklist_crud_and_membership() -> None:
    _fresh_db()
    t = storage.create_task(title="x")
    other = storage.create_task(title="y")

    item = storage.add_checklist_item(t["id"], "  trim me  ")
    assert item["text"] == "trim me" and item["done"] is False

    try:
        storage.add_checklist_item(t["id"], "   ")
        raise AssertionError("expected ValueError for empty text")
    except ValueError:
        pass

    try:
        storage.add_checklist_item(999999, "x")
        raise AssertionError("expected NotFoundError for a nonexistent task")
    except storage.NotFoundError:
        pass

    done_item = storage.set_checklist_item_done(t["id"], item["id"], True)
    assert done_item["done"] is True

    try:
        storage.set_checklist_item_done(t["id"], item["id"], 1)  # not a real bool
        raise AssertionError("expected ValueError for a non-bool done value")
    except ValueError:
        pass

    try:
        storage.set_checklist_item_done(other["id"], item["id"], True)  # wrong task
        raise AssertionError("expected NotFoundError for a mismatched task/item pair")
    except storage.NotFoundError:
        pass

    assert storage.delete_checklist_item(t["id"], item["id"]) is True
    assert storage.delete_checklist_item(t["id"], item["id"]) is False, "delete must be idempotent"
    print("PASS: checklist CRUD, validation, task-membership, and idempotent delete all correct")


def test_checklist_max_items_boundary() -> None:
    _fresh_db()
    t = storage.create_task(title="x")
    for i in range(storage.CHECKLIST_MAX_ITEMS_PER_TASK):
        storage.add_checklist_item(t["id"], f"item {i}")
    try:
        storage.add_checklist_item(t["id"], "one too many")
        raise AssertionError("expected ValueError at the max-items boundary")
    except ValueError:
        pass
    print("PASS: max-items-per-task boundary enforced")


def test_checklist_activity_bumps_task_updated_at() -> None:
    _fresh_db()
    t = storage.create_task(title="x")
    before = t["updated_at"]
    import time
    time.sleep(1.1)  # _now() has 1-second resolution
    storage.add_checklist_item(t["id"], "an item")
    after = storage.get_task(t["id"])["updated_at"]
    assert after > before, "checklist activity should bump the task's updated_at (board is activity-sorted)"
    print("PASS: checklist activity bumps task.updated_at")


def test_migration_from_legacy_schema_backfills_priority_and_due_date() -> None:
    """Simulates a DB that predates tranche 4 -- a `tasks` table with no
    priority/due_date columns at all -- and confirms init_db()'s
    _ensure_column migration adds them without losing existing rows, with
    priority backfilling to the CHECK-enforced default."""
    import sqlite3

    tmp = Path(tempfile.mkstemp(suffix=".sqlite3")[1])
    conn = sqlite3.connect(tmp)
    conn.execute(
        """
        CREATE TABLE tasks (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            title TEXT NOT NULL,
            description TEXT NOT NULL DEFAULT '',
            status TEXT NOT NULL DEFAULT 'open',
            assignee TEXT,
            created_by TEXT NOT NULL DEFAULT '',
            tags TEXT NOT NULL DEFAULT '',
            created_at TEXT NOT NULL,
            updated_at TEXT NOT NULL
        )
        """
    )
    conn.execute(
        "INSERT INTO tasks (title, created_by, created_at, updated_at) VALUES (?, ?, ?, ?)",
        ("pre-existing legacy task", "claude", "2020-01-01T00:00:00+00:00", "2020-01-01T00:00:00+00:00"),
    )
    conn.commit()
    conn.close()

    storage.DB_PATH = tmp
    storage.init_db()  # must not raise, must not touch the existing row's identity

    legacy = storage.list_tasks()
    assert len(legacy) == 1, "the pre-existing row must survive the migration"
    assert legacy[0]["title"] == "pre-existing legacy task"
    assert legacy[0]["priority"] == "normal", "existing rows must backfill to the column default"
    assert legacy[0]["due_date"] is None
    assert legacy[0]["checklist_items"] == []

    # And the migrated table must still actually enforce the CHECK -- not
    # just carry the column with no constraint.
    try:
        storage.create_task(title="x", priority="not-a-real-priority")
        raise AssertionError("expected ValueError on a migrated table too")
    except ValueError:
        pass

    # Running init_db() again (e.g. a process restart against the now-migrated
    # DB) must be a no-op, not a second ALTER TABLE attempt.
    storage.init_db()
    print("PASS: legacy schema migrates cleanly, backfills to defaults, keeps existing rows, restart-safe")


def test_checklist_items_cascade_delete_with_their_task() -> None:
    """delete_task() does not explicitly DELETE FROM task_checklist_items --
    it relies entirely on the schema's ON DELETE CASCADE foreign key, which
    only actually fires if PRAGMA foreign_keys=ON is truly active on the
    deleting connection. Verified directly against the table, not just
    through the public API that would also look clean if rows were merely
    orphaned rather than removed."""
    _fresh_db()
    t = storage.create_task(title="x")
    other = storage.create_task(title="y")  # a sibling row that must NOT be touched
    for i in range(5):
        storage.add_checklist_item(t["id"], f"item {i}")
    storage.add_checklist_item(other["id"], "unrelated item")

    with storage._connect() as conn:
        count_before = conn.execute(
            "SELECT COUNT(*) AS c FROM task_checklist_items WHERE task_id=?", (t["id"],)
        ).fetchone()["c"]
    assert count_before == 5

    assert storage.delete_task(t["id"]) is True

    with storage._connect() as conn:
        orphaned = conn.execute(
            "SELECT COUNT(*) AS c FROM task_checklist_items WHERE task_id=?", (t["id"],)
        ).fetchone()["c"]
        other_count = conn.execute(
            "SELECT COUNT(*) AS c FROM task_checklist_items WHERE task_id=?", (other["id"],)
        ).fetchone()["c"]
    assert orphaned == 0, "ON DELETE CASCADE must remove the deleted task's checklist items, not just orphan them"
    assert other_count == 1, "a sibling task's checklist items must be untouched"
    print("PASS: checklist items cascade-delete with their task; unrelated tasks unaffected")


def test_concurrent_checklist_append_no_lost_writes() -> None:
    """20 threads each add one checklist item to the SAME task concurrently
    (separate connections, WAL mode). Since there is deliberately no
    `position` column to race on (id order is used instead, per the
    contract), the only correctness property that matters here is that
    every write actually lands -- no lost updates, no duplicate ids, no
    exceptions swallowed by a thread."""
    import threading

    _fresh_db()
    t = storage.create_task(title="x")
    n = 20
    errors: list[Exception] = []
    lock = threading.Lock()

    def worker(i: int) -> None:
        try:
            storage.add_checklist_item(t["id"], f"concurrent item {i}")
        except Exception as exc:  # noqa: BLE001 -- captured for the assertion below, not swallowed
            with lock:
                errors.append(exc)

    threads = [threading.Thread(target=worker, args=(i,)) for i in range(n)]
    for th in threads:
        th.start()
    for th in threads:
        th.join()

    assert not errors, f"concurrent add_checklist_item calls raised: {errors}"
    final = storage.get_task(t["id"])
    assert final["checklist_progress"]["total"] == n, (
        f"expected all {n} concurrent appends to land, got {final['checklist_progress']['total']}"
    )
    ids = [item["id"] for item in final["checklist_items"]]
    assert len(ids) == len(set(ids)), "expected no duplicate ids under concurrent AUTOINCREMENT inserts"
    print(f"PASS: {n} concurrent checklist appends to the same task all landed, no lost writes or duplicate ids")


def test_checklist_hydration_is_consistent_across_task_shapes() -> None:
    """The exact gap OpenClaw's review flagged: initial load (list_tasks/
    full_state) must show the same checklist_items/progress shape as a
    post-event get_task fetch would, not a poorer one."""
    _fresh_db()
    t = storage.create_task(title="x")
    storage.add_checklist_item(t["id"], "item one")
    storage.set_checklist_item_done(t["id"], storage.add_checklist_item(t["id"], "item two")["id"], True)

    via_get = storage.get_task(t["id"])
    via_list = next(x for x in storage.list_tasks() if x["id"] == t["id"])
    via_state = next(x for x in storage.full_state()["tasks"] if x["id"] == t["id"])

    for label, shape in [("get_task", via_get), ("list_tasks", via_list), ("full_state", via_state)]:
        assert shape["checklist_progress"] == {"done": 1, "total": 2}, f"{label} progress mismatch: {shape['checklist_progress']}"
        assert len(shape["checklist_items"]) == 2, f"{label} items count mismatch"
    print("PASS: checklist hydration is consistent across get_task/list_tasks/full_state")


if __name__ == "__main__":
    test_priority_validation()
    test_due_date_validation_rejects_invalid_calendar_dates()
    test_due_date_tri_state_omit_vs_null_vs_set()
    test_checklist_crud_and_membership()
    test_checklist_max_items_boundary()
    test_checklist_activity_bumps_task_updated_at()
    test_migration_from_legacy_schema_backfills_priority_and_due_date()
    test_checklist_items_cascade_delete_with_their_task()
    test_concurrent_checklist_append_no_lost_writes()
    test_checklist_hydration_is_consistent_across_task_shapes()
    print("All task-metadata tests passed.")
