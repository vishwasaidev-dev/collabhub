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
    test_checklist_hydration_is_consistent_across_task_shapes()
    print("All task-metadata tests passed.")
