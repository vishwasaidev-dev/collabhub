"""SQLite-backed storage for CollabHub: shared tasks + notes.

Single-file DB, opened per-call (sqlite3 handles this fine at our scale --
two low-frequency agent clients, not a real traffic load). WAL mode so the
dashboard's polling reads never block a concurrent write from either agent.

Everything that mutates state also appends a row to `events` -- a single
append-only log that catch_up()/wait_for_events()/the SSE and WebSocket
streams all read from. One log, multiple taps, instead of a bespoke
notification path per feature.
"""
from __future__ import annotations

import json
import re
import sqlite3
from datetime import date, datetime, timezone
from pathlib import Path
from typing import Any

DB_PATH = Path(__file__).resolve().parent / "collabhub.sqlite3"

VALID_STATUSES = ("open", "claimed", "in_progress", "blocked", "done")

# --- Task metadata (tranche 4) constants ------------------------------------
VALID_PRIORITIES = ("low", "normal", "high", "urgent")
CHECKLIST_MAX_ITEM_LEN = 300
CHECKLIST_MAX_ITEMS_PER_TASK = 50
# Sentinel distinguishing "due_date not mentioned in this call -- leave
# unchanged" from "due_date explicitly passed as null -- clear it". A bare
# `None` default can't carry this distinction (OpenClaw's review, tranche 4:
# omission and explicit-null are different operations and must stay
# distinguishable through both REST and MCP).
_OMITTED = object()


def _validate_due_date(value: str) -> None:
    """A due date is a calendar commitment, not an instant -- validate with a
    real date parser (rejects e.g. 2026-02-30), not a regex that would accept
    syntactically-shaped nonsense (OpenClaw's review)."""
    try:
        date.fromisoformat(value)
    except ValueError:
        # A stable, public-facing message -- not Python's internal parser
        # wording, which can change across versions and isn't meant for API
        # consumers (OpenClaw's review, non-blocking but easy to close now).
        raise ValueError(f"due_date {value!r} is not a valid YYYY-MM-DD calendar date") from None


class NotFoundError(Exception):
    """A referenced resource (task, chat session, ...) doesn't exist -- kept
    distinct from ValueError (malformed input) so REST handlers can map this
    to 404 instead of 400 (OpenClaw's review, tranche 2 final pass: a bad
    relation_type is a validation error, but a nonexistent task_id/session_id
    is a resource-not-found error, and conflating both into ValueError->400
    was wrong)."""


# --- Search (tranche 3) constants -------------------------------------------
SEARCH_TYPES = ("task", "comment", "note", "chat_message")
_SEARCH_TYPE_CODE = {"task": 1, "comment": 2, "note": 3, "chat_message": 4}
SEARCH_MAX_QUERY_LEN = 500
SEARCH_MAX_TOKENS = 20
SEARCH_MAX_LIMIT = 100
# Non-printable sentinels for snippet() delimiters -- NOT literal HTML tags,
# since the text around a match is raw user content and could itself contain
# a stored <script> or literal "<mark>"-looking string. Escape the whole
# snippet first, then swap these (which survive HTML-escaping unchanged)
# for real <mark>/</mark> -- only our own insertions become tags
# (OpenClaw's review, tranche 3 pre-build hardening).
SNIPPET_START = "\x01"
SNIPPET_END = "\x02"

# Normalized token, not arbitrary text (OpenClaw's review, tranche 2,
# 2026-08-14) -- keeps the join table's relation_type from ever becoming a
# dumping ground for free-form/HTML-sized strings.
RELATION_TYPE_RE = re.compile(r"^[a-z][a-z0-9_]{0,31}$")


def _now() -> str:
    return datetime.now(timezone.utc).isoformat(timespec="seconds")


def _connect() -> sqlite3.Connection:
    conn = sqlite3.connect(DB_PATH, timeout=10)
    conn.execute("PRAGMA journal_mode=WAL")
    conn.execute("PRAGMA foreign_keys=ON")
    conn.row_factory = sqlite3.Row
    return conn


def _ensure_column(conn: sqlite3.Connection, table: str, column: str, ddl: str) -> None:
    """Idempotent migration helper -- adds `column` to `table` if an older DB
    predates it. ALTER TABLE ADD COLUMN is safe/cheap in SQLite even on a
    populated table."""
    cols = {row["name"] for row in conn.execute(f"PRAGMA table_info({table})").fetchall()}
    if column not in cols:
        conn.execute(f"ALTER TABLE {table} ADD COLUMN {ddl}")


def init_db() -> None:
    with _connect() as conn:
        conn.execute(
            """
            CREATE TABLE IF NOT EXISTS tasks (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                title TEXT NOT NULL,
                description TEXT NOT NULL DEFAULT '',
                status TEXT NOT NULL DEFAULT 'open',
                assignee TEXT,
                created_by TEXT NOT NULL DEFAULT '',
                tags TEXT NOT NULL DEFAULT '',
                priority TEXT NOT NULL DEFAULT 'normal'
                    CHECK(priority IN ('low','normal','high','urgent')),
                due_date TEXT,
                created_at TEXT NOT NULL,
                updated_at TEXT NOT NULL
            )
            """
        )
        # Migration for a DB that predates priority/due_date (mirrors the
        # invite_task_id precedent below) -- existing rows backfill to the
        # column default ('normal'), enforced by both this CHECK and
        # application-level validation (OpenClaw's review: belt and
        # suspenders, not either alone).
        _ensure_column(conn, "tasks", "priority",
                        "priority TEXT NOT NULL DEFAULT 'normal' CHECK(priority IN ('low','normal','high','urgent'))")
        _ensure_column(conn, "tasks", "due_date", "due_date TEXT")
        conn.execute(
            """
            CREATE TABLE IF NOT EXISTS comments (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                task_id INTEGER NOT NULL REFERENCES tasks(id) ON DELETE CASCADE,
                author TEXT NOT NULL DEFAULT '',
                text TEXT NOT NULL,
                created_at TEXT NOT NULL
            )
            """
        )
        conn.execute(
            """
            CREATE TABLE IF NOT EXISTS notes (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                author TEXT NOT NULL DEFAULT '',
                text TEXT NOT NULL,
                created_at TEXT NOT NULL
            )
            """
        )
        conn.execute(
            """
            CREATE TABLE IF NOT EXISTS chat_sessions (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                status TEXT NOT NULL DEFAULT 'active',
                started_by TEXT NOT NULL DEFAULT '',
                started_at TEXT NOT NULL,
                ended_at TEXT
            )
            """
        )
        # invite_task_id: optional link back to the task (if any) that invited the
        # other agent into this session. Lets end_chat_session auto-close it instead
        # of leaving a stale "join session #N" task open forever (state-drift bug
        # OpenClaw flagged 2026-08-14).
        _ensure_column(conn, "chat_sessions", "invite_task_id", "invite_task_id INTEGER")
        conn.execute(
            """
            CREATE TABLE IF NOT EXISTS chat_messages (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                session_id INTEGER NOT NULL REFERENCES chat_sessions(id) ON DELETE CASCADE,
                author TEXT NOT NULL DEFAULT '',
                text TEXT NOT NULL,
                created_at TEXT NOT NULL
            )
            """
        )
        conn.execute(
            """
            CREATE TABLE IF NOT EXISTS events (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                type TEXT NOT NULL,
                payload TEXT NOT NULL,
                created_at TEXT NOT NULL
            )
            """
        )
        conn.execute(
            """
            CREATE TABLE IF NOT EXISTS agent_cursors (
                agent TEXT PRIMARY KEY,
                last_event_id INTEGER NOT NULL DEFAULT 0,
                updated_at TEXT NOT NULL
            )
            """
        )
        conn.execute(
            """
            CREATE TABLE IF NOT EXISTS agent_presence (
                agent TEXT PRIMARY KEY,
                last_seen TEXT NOT NULL,
                last_action TEXT NOT NULL DEFAULT ''
            )
            """
        )
        # Many-to-many task<->chat-session linkage (tranche 2, 2026-08-14) --
        # a single tasks.chat_session_id FK was the wrong shape: one task can
        # be discussed across several sessions, one session can cover several
        # tasks (OpenClaw's review). No ON DELETE clause -- deletion isn't a
        # supported operation on tasks/sessions today, so SQLite's default
        # NO ACTION (effectively RESTRICT: a delete that would orphan a row
        # here fails) is the deliberately conservative choice.
        conn.execute(
            """
            CREATE TABLE IF NOT EXISTS task_chat_sessions (
                task_id INTEGER NOT NULL REFERENCES tasks(id),
                session_id INTEGER NOT NULL REFERENCES chat_sessions(id),
                relation_type TEXT NOT NULL,
                linked_by TEXT NOT NULL DEFAULT '',
                created_at TEXT NOT NULL,
                UNIQUE(task_id, session_id, relation_type)
            )
            """
        )
        conn.execute("CREATE INDEX IF NOT EXISTS idx_tcs_task ON task_chat_sessions(task_id)")
        conn.execute("CREATE INDEX IF NOT EXISTS idx_tcs_session ON task_chat_sessions(session_id)")

        # Checklist items (tranche 4). Deliberately no `position` column --
        # computing MAX(position)+1 then inserting is a genuine TOCTOU race
        # across two concurrent writers (the read and the write aren't one
        # atomic step), and a UNIQUE(task_id, position) constraint would just
        # turn that race into an occasional failed write instead of a silent
        # duplicate. Since this tranche has no reorder feature, `id` order
        # (inherently race-free -- SQLite's own PK allocation) is both
        # simpler and actually correct; add position later only alongside a
        # real reorder feature that needs it (OpenClaw's review).
        # ON DELETE CASCADE here (unlike task_chat_sessions' deliberate NO
        # ACTION) because a checklist item has no independent existence once
        # its task is gone.
        conn.execute(
            """
            CREATE TABLE IF NOT EXISTS task_checklist_items (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                task_id INTEGER NOT NULL REFERENCES tasks(id) ON DELETE CASCADE,
                text TEXT NOT NULL,
                done INTEGER NOT NULL DEFAULT 0 CHECK(done IN (0,1)),
                created_at TEXT NOT NULL,
                updated_at TEXT NOT NULL
            )
            """
        )
        conn.execute("CREATE INDEX IF NOT EXISTS idx_checklist_task ON task_checklist_items(task_id)")

        # Unified full-text search index (tranche 3). `content` is the only
        # tokenized/searchable column; everything else is UNINDEXED metadata
        # carried alongside each row so a search hit can be displayed and
        # linked back to its source without a second query per result.
        # rowid is a stable composite key (type_code*10_000_000_000 +
        # source_id) so a row can be deleted+reinserted on update without
        # colliding across the four separate id spaces (task/comment/note/
        # chat_message each have their own AUTOINCREMENT sequence).
        search_index_existed = conn.execute(
            "SELECT name FROM sqlite_master WHERE type='table' AND name='search_index'"
        ).fetchone() is not None
        conn.execute(
            """
            CREATE VIRTUAL TABLE IF NOT EXISTS search_index USING fts5(
                content,
                source_type UNINDEXED,
                source_id UNINDEXED,
                parent_id UNINDEXED,
                author UNINDEXED,
                created_at UNINDEXED
            )
            """
        )
        if not search_index_existed:
            # First time this table has existed on this DB -- backfill every
            # row that already exists so search covers pre-existing data
            # immediately, not just writes from this point forward
            # (OpenClaw's review, tranche 3).
            _rebuild_search_index(conn)


# ---------------------------------------------------------------------------
# Event log + presence
# ---------------------------------------------------------------------------

def _emit_event(conn: sqlite3.Connection, type_: str, payload: dict[str, Any]) -> int:
    cur = conn.execute(
        "INSERT INTO events (type, payload, created_at) VALUES (?, ?, ?)",
        (type_, json.dumps(payload), _now()),
    )
    return cur.lastrowid


def _row_to_event(row: sqlite3.Row) -> dict[str, Any]:
    e = dict(row)
    e["payload"] = json.loads(e["payload"])
    return e


def get_events_since(since: int) -> list[dict]:
    with _connect() as conn:
        rows = conn.execute("SELECT * FROM events WHERE id>? ORDER BY id ASC", (since,)).fetchall()
        return [_row_to_event(r) for r in rows]


def latest_event_id() -> int:
    with _connect() as conn:
        row = conn.execute("SELECT MAX(id) AS m FROM events").fetchone()
        return row["m"] or 0


def _touch_presence(conn: sqlite3.Connection, agent: str, action: str) -> None:
    if not agent:
        return
    conn.execute(
        "INSERT INTO agent_presence (agent, last_seen, last_action) VALUES (?, ?, ?) "
        "ON CONFLICT(agent) DO UPDATE SET last_seen=excluded.last_seen, last_action=excluded.last_action",
        (agent, _now(), action),
    )


def touch_presence(agent: str, action: str = "") -> None:
    """Public entry point for callers (e.g. app.py routes) that don't already
    hold a connection from one of the functions below."""
    if not agent:
        return
    with _connect() as conn:
        _touch_presence(conn, agent, action)


def list_presence() -> list[dict]:
    with _connect() as conn:
        rows = conn.execute("SELECT * FROM agent_presence ORDER BY last_seen DESC").fetchall()
        return [dict(r) for r in rows]


def catch_up(agent: str, after_cursor: int | None = None) -> dict:
    """One-call digest: every event since `agent`'s last ACKED cursor (or an
    explicit `after_cursor` override), their own open tasks, and the active
    chat session.

    Non-destructive by design (OpenClaw's catch, 2026-08-14): reading does NOT
    advance the durable cursor -- if a response is lost in transit, the events
    it described must still be re-deliverable. Call ack_events(agent, cursor)
    once the caller has durably processed a batch. Repeat calls with no ack in
    between return the same events (at-least-once, not at-most-once)."""
    if not agent:
        raise ValueError("agent is required")
    with _connect() as conn:
        if after_cursor is None:
            cur_row = conn.execute(
                "SELECT last_event_id FROM agent_cursors WHERE agent=?", (agent,)
            ).fetchone()
            since = cur_row["last_event_id"] if cur_row else 0
        else:
            since = after_cursor
        rows = conn.execute("SELECT * FROM events WHERE id>? ORDER BY id ASC", (since,)).fetchall()
        events = [_row_to_event(r) for r in rows]
        next_cursor = events[-1]["id"] if events else since

        _touch_presence(conn, agent, "catch_up")

        my_tasks_rows = conn.execute(
            "SELECT * FROM tasks WHERE assignee=? AND status!='done' ORDER BY updated_at DESC",
            (agent,),
        ).fetchall()
        my_tasks = [_task_row_to_dict(r) for r in my_tasks_rows]
        _hydrate_checklists(conn, my_tasks)
        active_chat = conn.execute(
            "SELECT * FROM chat_sessions WHERE status='active' ORDER BY id DESC LIMIT 1"
        ).fetchone()

        return {
            "agent": agent,
            "since": since,
            "next_cursor": next_cursor,
            "unread_count": len(events),
            "events": events,
            "my_open_tasks": my_tasks,
            "active_chat_session": dict(active_chat) if active_chat else None,
        }


def ack_events(agent: str, cursor: int) -> dict:
    """Durably advance `agent`'s cursor to `cursor` -- monotonic (never regresses,
    even if an out-of-order/duplicate ack arrives late). Separate from catch_up
    so a lost response never silently drops events."""
    if not agent:
        raise ValueError("agent is required")
    with _connect() as conn:
        row = conn.execute("SELECT last_event_id FROM agent_cursors WHERE agent=?", (agent,)).fetchone()
        current = row["last_event_id"] if row else 0
        new_cursor = max(current, cursor)
        conn.execute(
            "INSERT INTO agent_cursors (agent, last_event_id, updated_at) VALUES (?, ?, ?) "
            "ON CONFLICT(agent) DO UPDATE SET last_event_id=excluded.last_event_id, updated_at=excluded.updated_at",
            (agent, new_cursor, _now()),
        )
        return {"agent": agent, "cursor": new_cursor}


# ---------------------------------------------------------------------------
# Tasks
# ---------------------------------------------------------------------------

def _task_row_to_dict(row: sqlite3.Row) -> dict[str, Any]:
    d = dict(row)
    d["tags"] = [t for t in d["tags"].split(",") if t] if d.get("tags") else []
    return d


def _checklist_item_to_dict(row: sqlite3.Row) -> dict[str, Any]:
    d = dict(row)
    d["done"] = bool(d["done"])
    return d


def _hydrate_checklists(conn: sqlite3.Connection, tasks: list[dict]) -> None:
    """Batched (no N+1) checklist attachment -- mutates each task dict in
    place, adding checklist_items + checklist_progress. Called from every
    task-list shape (list_tasks, full_state, catch_up), not just get_task --
    otherwise the dashboard's initial load (from /api/state) would show
    tasks without checklist data until each one happened to get a live
    event and trigger an individual refetch (OpenClaw's review, tranche 4)."""
    if not tasks:
        return
    task_ids = [t["id"] for t in tasks]
    placeholders = ",".join("?" * len(task_ids))
    rows = conn.execute(
        f"SELECT * FROM task_checklist_items WHERE task_id IN ({placeholders}) ORDER BY task_id, id ASC",
        task_ids,
    ).fetchall()
    items_by_task: dict[int, list[dict]] = {}
    for r in rows:
        items_by_task.setdefault(r["task_id"], []).append(_checklist_item_to_dict(r))
    for t in tasks:
        items = items_by_task.get(t["id"], [])
        t["checklist_items"] = items
        t["checklist_progress"] = {"done": sum(1 for i in items if i["done"]), "total": len(items)}


def create_task(
    title: str, description: str = "", created_by: str = "", tags: list[str] | None = None,
    priority: str = "normal", due_date: str | None = None,
) -> dict:
    if priority not in VALID_PRIORITIES:
        raise ValueError(f"invalid priority {priority!r}; must be one of {VALID_PRIORITIES}")
    if due_date is not None:
        _validate_due_date(due_date)
    now = _now()
    tags_str = ",".join(tags or [])
    with _connect() as conn:
        cur = conn.execute(
            "INSERT INTO tasks (title, description, status, created_by, tags, priority, due_date, created_at, updated_at) "
            "VALUES (?, ?, 'open', ?, ?, ?, ?, ?, ?)",
            (title, description, created_by, tags_str, priority, due_date, now, now),
        )
        task_id = cur.lastrowid
        row = conn.execute("SELECT * FROM tasks WHERE id=?", (task_id,)).fetchone()
        task = _task_row_to_dict(row)
        task["checklist_items"] = []
        task["checklist_progress"] = {"done": 0, "total": 0}
        _emit_event(conn, "task_created", {"task_id": task_id, "title": title, "created_by": created_by})
        _touch_presence(conn, created_by, "create_task")
        _index_for_search(conn, "task", task_id, None, _task_search_content(title, description, tags_str), created_by, now)
        return task


def list_tasks(status: str | None = None, assignee: str | None = None) -> list[dict]:
    q = "SELECT * FROM tasks"
    clauses, params = [], []
    if status:
        clauses.append("status=?")
        params.append(status)
    if assignee:
        clauses.append("assignee=?")
        params.append(assignee)
    if clauses:
        q += " WHERE " + " AND ".join(clauses)
    q += " ORDER BY updated_at DESC"
    with _connect() as conn:
        rows = conn.execute(q, params).fetchall()
        tasks = [_task_row_to_dict(r) for r in rows]
        _hydrate_checklists(conn, tasks)
        return tasks


def get_task(task_id: int) -> dict | None:
    with _connect() as conn:
        row = conn.execute("SELECT * FROM tasks WHERE id=?", (task_id,)).fetchone()
        if not row:
            return None
        task = _task_row_to_dict(row)
        comment_rows = conn.execute(
            "SELECT * FROM comments WHERE task_id=? ORDER BY created_at ASC", (task_id,)
        ).fetchall()
        task["comments"] = [dict(r) for r in comment_rows]
        # Non-recursive summaries only -- a session here doesn't itself embed
        # its messages/other linked tasks (OpenClaw's review: avoid recursive
        # full objects).
        task["chat_sessions"] = _task_chat_session_summaries(conn, task_id)
        _hydrate_checklists(conn, [task])
        return task


def update_task(
    task_id: int,
    status: str | None = None,
    description: str | None = None,
    assignee: str | None = None,
    priority: str | None = None,
    due_date: str | None = _OMITTED,
) -> dict | None:
    if status is not None and status not in VALID_STATUSES:
        raise ValueError(f"invalid status {status!r}; must be one of {VALID_STATUSES}")
    if priority is not None and priority not in VALID_PRIORITIES:
        raise ValueError(f"invalid priority {priority!r}; must be one of {VALID_PRIORITIES}")
    if due_date is not _OMITTED and due_date is not None:
        _validate_due_date(due_date)
    with _connect() as conn:
        row = conn.execute("SELECT * FROM tasks WHERE id=?", (task_id,)).fetchone()
        if not row:
            return None
        fields, params = [], []
        if status is not None:
            fields.append("status=?")
            params.append(status)
        if description is not None:
            fields.append("description=?")
            params.append(description)
        if assignee is not None:
            fields.append("assignee=?")
            params.append(assignee)
        if priority is not None:
            fields.append("priority=?")
            params.append(priority)
        if due_date is not _OMITTED:
            # due_date itself may be None here -- that's the explicit-clear
            # case (NULL), distinct from _OMITTED ("don't touch this
            # column", the default when the param isn't passed at all).
            fields.append("due_date=?")
            params.append(due_date)
        fields.append("updated_at=?")
        params.append(_now())
        params.append(task_id)
        conn.execute(f"UPDATE tasks SET {', '.join(fields)} WHERE id=?", params)
        row = conn.execute("SELECT * FROM tasks WHERE id=?", (task_id,)).fetchone()
        task = _task_row_to_dict(row)
        _hydrate_checklists(conn, [task])
        _emit_event(conn, "task_updated", {"task_id": task_id, "status": task["status"], "assignee": task.get("assignee")})
        if assignee:
            _touch_presence(conn, assignee, "update_task")
        # Reindex unconditionally on any update, not just a description
        # change -- cheap at this scale, and simpler than tracking exactly
        # which fields affect the indexed content (OpenClaw's review).
        _index_for_search(
            conn, "task", task_id, None,
            _task_search_content(task["title"], task["description"], row["tags"]),
            task.get("created_by", ""), task["updated_at"],
        )
        return task


def add_checklist_item(task_id: int, text: str) -> dict:
    text = (text or "").strip()
    if not text:
        raise ValueError("checklist item text must not be empty")
    if len(text) > CHECKLIST_MAX_ITEM_LEN:
        raise ValueError(f"checklist item text must be <= {CHECKLIST_MAX_ITEM_LEN} characters")
    with _connect() as conn:
        if not conn.execute("SELECT id FROM tasks WHERE id=?", (task_id,)).fetchone():
            raise NotFoundError(f"task {task_id} not found")
        count = conn.execute(
            "SELECT COUNT(*) AS c FROM task_checklist_items WHERE task_id=?", (task_id,)
        ).fetchone()["c"]
        if count >= CHECKLIST_MAX_ITEMS_PER_TASK:
            raise ValueError(f"task {task_id} already has the maximum of {CHECKLIST_MAX_ITEMS_PER_TASK} checklist items")
        now = _now()
        cur = conn.execute(
            "INSERT INTO task_checklist_items (task_id, text, done, created_at, updated_at) VALUES (?, ?, 0, ?, ?)",
            (task_id, text, now, now),
        )
        item_id = cur.lastrowid
        row = conn.execute("SELECT * FROM task_checklist_items WHERE id=?", (item_id,)).fetchone()
        item = _checklist_item_to_dict(row)
        # Checklist activity counts as task activity -- the board is sorted
        # by updated_at, and this is exactly the kind of change that should
        # surface a task near the top (OpenClaw's review: explicit yes here).
        conn.execute("UPDATE tasks SET updated_at=? WHERE id=?", (now, task_id))
        _emit_event(conn, "checklist_item_added", {"task_id": task_id, "item_id": item_id, "text": text})
        return item


def set_checklist_item_done(task_id: int, item_id: int, done: bool) -> dict:
    """`done` must be an actual bool -- a JSON `1`/`"true"` is rejected, not
    coerced, per OpenClaw's review (strict JSON bool behavior). Validates
    item_id actually belongs to task_id, distinct from item_id simply not
    existing at all -- both are 404s at the REST layer, but this keeps the
    check explicit rather than accidentally operating on the wrong task's
    item because two ids happened to both exist somewhere in the table."""
    if not isinstance(done, bool):
        raise ValueError("done must be a boolean")
    with _connect() as conn:
        row = conn.execute("SELECT * FROM task_checklist_items WHERE id=?", (item_id,)).fetchone()
        if not row or row["task_id"] != task_id:
            raise NotFoundError(f"checklist item {item_id} not found in task {task_id}")
        now = _now()
        conn.execute(
            "UPDATE task_checklist_items SET done=?, updated_at=? WHERE id=?",
            (1 if done else 0, now, item_id),
        )
        conn.execute("UPDATE tasks SET updated_at=? WHERE id=?", (now, task_id))
        row = conn.execute("SELECT * FROM task_checklist_items WHERE id=?", (item_id,)).fetchone()
        item = _checklist_item_to_dict(row)
        _emit_event(conn, "checklist_item_updated", {"task_id": task_id, "item_id": item_id, "done": item["done"]})
        return item


def delete_checklist_item(task_id: int, item_id: int) -> bool:
    """Idempotent and task-scoped: an item that's already gone, or that
    never belonged to this task_id, both just return False -- no error.
    From a caller scoped to a specific task, both cases mean the same thing
    ("there's nothing to delete here"), so there's no need to distinguish
    them (OpenClaw's review asked for delete semantics to be defined)."""
    with _connect() as conn:
        row = conn.execute(
            "SELECT * FROM task_checklist_items WHERE id=? AND task_id=?", (item_id, task_id)
        ).fetchone()
        if not row:
            return False
        now = _now()
        conn.execute("DELETE FROM task_checklist_items WHERE id=?", (item_id,))
        conn.execute("UPDATE tasks SET updated_at=? WHERE id=?", (now, task_id))
        _emit_event(conn, "checklist_item_removed", {"task_id": task_id, "item_id": item_id})
        return True


def add_comment(task_id: int, author: str, text: str) -> dict | None:
    with _connect() as conn:
        exists = conn.execute("SELECT id FROM tasks WHERE id=?", (task_id,)).fetchone()
        if not exists:
            return None
        now = _now()
        cur = conn.execute(
            "INSERT INTO comments (task_id, author, text, created_at) VALUES (?, ?, ?, ?)",
            (task_id, author, text, now),
        )
        conn.execute("UPDATE tasks SET updated_at=? WHERE id=?", (now, task_id))
        row = conn.execute("SELECT * FROM comments WHERE id=?", (cur.lastrowid,)).fetchone()
        comment = dict(row)
        _emit_event(conn, "task_commented", {"task_id": task_id, "comment_id": comment["id"], "author": author, "text": text})
        _touch_presence(conn, author, "comment_task")
        _index_for_search(conn, "comment", comment["id"], task_id, text, author, now)
        return comment


# ---------------------------------------------------------------------------
# Notes
# ---------------------------------------------------------------------------

def post_note(author: str, text: str) -> dict:
    with _connect() as conn:
        now = _now()
        cur = conn.execute(
            "INSERT INTO notes (author, text, created_at) VALUES (?, ?, ?)", (author, text, now)
        )
        row = conn.execute("SELECT * FROM notes WHERE id=?", (cur.lastrowid,)).fetchone()
        note = dict(row)
        _emit_event(conn, "note_posted", {"note_id": note["id"], "author": author, "text": text})
        _touch_presence(conn, author, "post_note")
        _index_for_search(conn, "note", note["id"], None, text, author, now)
        return note


def list_notes(limit: int = 50) -> list[dict]:
    with _connect() as conn:
        rows = conn.execute(
            "SELECT * FROM notes ORDER BY created_at DESC LIMIT ?", (limit,)
        ).fetchall()
        return [dict(r) for r in rows]


def get_note(note_id: int) -> dict | None:
    """Exact-id lookup -- list_notes only returns the newest slice, so a
    search hit on an older note needs its own fetch path rather than
    guessing at a large-enough limit (OpenClaw's review, tranche 3)."""
    with _connect() as conn:
        row = conn.execute("SELECT * FROM notes WHERE id=?", (note_id,)).fetchone()
        return dict(row) if row else None


# ---------------------------------------------------------------------------
# Task <-> chat session linkage (tranche 2)
# ---------------------------------------------------------------------------

def _link_task_session(conn: sqlite3.Connection, task_id: int, session_id: int, relation_type: str, linked_by: str = "") -> dict:
    """Insert a link row (idempotent -- re-linking the same (task, session,
    relation_type) triple is a no-op, not an error, since a caller retrying
    after a dropped response shouldn't get a spurious failure). Assumes the
    caller has already validated task_id/session_id exist and relation_type
    is well-formed; internal helper shared by start_chat_session's auto-invite
    link and the public link_task_session."""
    now = _now()
    try:
        conn.execute(
            "INSERT INTO task_chat_sessions (task_id, session_id, relation_type, linked_by, created_at) "
            "VALUES (?, ?, ?, ?, ?)",
            (task_id, session_id, relation_type, linked_by, now),
        )
        _emit_event(conn, "task_session_linked", {
            "task_id": task_id, "session_id": session_id, "relation_type": relation_type, "linked_by": linked_by,
        })
    except sqlite3.IntegrityError:
        pass  # already linked
    row = conn.execute(
        "SELECT * FROM task_chat_sessions WHERE task_id=? AND session_id=? AND relation_type=?",
        (task_id, session_id, relation_type),
    ).fetchone()
    return dict(row)


def link_task_session(task_id: int, session_id: int, relation_type: str, linked_by: str = "") -> dict:
    """Public entry point (validates existence + relation_type) -- without
    this, the many-to-many model can't actually be used for anything beyond
    the auto-created invite link (OpenClaw's review)."""
    if not RELATION_TYPE_RE.match(relation_type or ""):
        raise ValueError(f"relation_type must match {RELATION_TYPE_RE.pattern!r}")
    with _connect() as conn:
        if not conn.execute("SELECT id FROM tasks WHERE id=?", (task_id,)).fetchone():
            raise NotFoundError(f"task {task_id} not found")
        if not conn.execute("SELECT id FROM chat_sessions WHERE id=?", (session_id,)).fetchone():
            raise NotFoundError(f"chat session {session_id} not found")
        return _link_task_session(conn, task_id, session_id, relation_type, linked_by)


def unlink_task_session(task_id: int, session_id: int, relation_type: str) -> bool:
    """Returns True if a link was actually removed. The 'invite' relation is
    lifecycle-owned by the session (drives end_chat_session's auto-close) and
    deliberately cannot be unlinked through this general path -- OpenClaw's
    review flagged that unlinking it without also clearing
    chat_sessions.invite_task_id would leave the two out of sync; there's no
    product need yet to clear an invite link, so the simple safe choice is to
    just refuse it outright rather than build the transactional clear-both
    operation for a case that doesn't exist yet."""
    if relation_type == "invite":
        raise ValueError("cannot unlink the 'invite' relation directly -- it is lifecycle-owned by the session")
    with _connect() as conn:
        cur = conn.execute(
            "DELETE FROM task_chat_sessions WHERE task_id=? AND session_id=? AND relation_type=?",
            (task_id, session_id, relation_type),
        )
        removed = cur.rowcount > 0
        if removed:
            _emit_event(conn, "task_session_unlinked", {
                "task_id": task_id, "session_id": session_id, "relation_type": relation_type,
            })
        return removed


def _task_chat_session_summaries(conn: sqlite3.Connection, task_id: int) -> list[dict]:
    rows = conn.execute(
        "SELECT tcs.session_id, tcs.relation_type, cs.status, cs.started_at, cs.ended_at "
        "FROM task_chat_sessions tcs JOIN chat_sessions cs ON cs.id = tcs.session_id "
        "WHERE tcs.task_id=? ORDER BY tcs.created_at DESC",
        (task_id,),
    ).fetchall()
    return [dict(r) for r in rows]


def _session_linked_tasks_summaries(conn: sqlite3.Connection, session_id: int) -> list[dict]:
    rows = conn.execute(
        "SELECT tcs.task_id, tcs.relation_type, t.title, t.status "
        "FROM task_chat_sessions tcs JOIN tasks t ON t.id = tcs.task_id "
        "WHERE tcs.session_id=? ORDER BY tcs.created_at DESC",
        (session_id,),
    ).fetchall()
    return [dict(r) for r in rows]


def _session_duration_seconds(session: dict) -> int | None:
    """None while active -- a live/ongoing duration is the dashboard's job to
    compute client-side against wall-clock time, not something to bake into a
    server response that's stale the instant it's sent (OpenClaw's review:
    avoid an ambiguous 'duration' -- name it explicitly and define it)."""
    if not session.get("ended_at"):
        return None
    try:
        start = datetime.fromisoformat(session["started_at"])
        end = datetime.fromisoformat(session["ended_at"])
        return int((end - start).total_seconds())
    except (ValueError, TypeError):
        return None


# ---------------------------------------------------------------------------
# Live chat
# ---------------------------------------------------------------------------

def start_chat_session(started_by: str = "", invite_task_id: int | None = None) -> dict:
    """Start a live chat session, or return the already-active one if there is one
    (only one session is active at a time -- both agents share it). `invite_task_id`,
    if given, is a task to auto-complete when this session ends (see
    end_chat_session) -- and, atomically with session creation, gets an
    'invite' row in task_chat_sessions. A nonexistent invite_task_id raises
    instead of creating a dangling session; since this whole function runs
    inside one `with _connect()` block, raising here rolls back the session
    insert too (OpenClaw's review: link failure must roll back session
    creation, not leave an orphan)."""
    with _connect() as conn:
        existing = conn.execute(
            "SELECT * FROM chat_sessions WHERE status='active' ORDER BY id DESC LIMIT 1"
        ).fetchone()
        if existing:
            return dict(existing)
        if invite_task_id is not None and not conn.execute(
            "SELECT id FROM tasks WHERE id=?", (invite_task_id,)
        ).fetchone():
            raise NotFoundError(f"invite_task_id {invite_task_id} not found")
        now = _now()
        cur = conn.execute(
            "INSERT INTO chat_sessions (status, started_by, started_at, invite_task_id) "
            "VALUES ('active', ?, ?, ?)",
            (started_by, now, invite_task_id),
        )
        row = conn.execute("SELECT * FROM chat_sessions WHERE id=?", (cur.lastrowid,)).fetchone()
        sess = dict(row)
        _emit_event(conn, "chat_session_started", {"session_id": sess["id"], "started_by": started_by})
        _touch_presence(conn, started_by, "start_chat_session")
        if invite_task_id is not None:
            _link_task_session(conn, invite_task_id, sess["id"], "invite", started_by)
        return sess


def end_chat_session(session_id: int) -> dict | None:
    with _connect() as conn:
        row = conn.execute("SELECT * FROM chat_sessions WHERE id=?", (session_id,)).fetchone()
        if not row:
            return None
        conn.execute(
            "UPDATE chat_sessions SET status='ended', ended_at=? WHERE id=?", (_now(), session_id)
        )
        row = conn.execute("SELECT * FROM chat_sessions WHERE id=?", (session_id,)).fetchone()
        sess = dict(row)
        _emit_event(conn, "chat_session_ended", {"session_id": session_id})

        # Lifecycle linkage: auto-close the invite task this session was started
        # from, if it's still open. Fixes the exact drift bug where "join session
        # #N" tasks sat open forever after the session they invited to had ended.
        invite_task_id = sess.get("invite_task_id")
        if invite_task_id:
            task = conn.execute("SELECT * FROM tasks WHERE id=?", (invite_task_id,)).fetchone()
            if task and task["status"] != "done":
                now2 = _now()
                conn.execute(
                    "UPDATE tasks SET status='done', updated_at=? WHERE id=?", (now2, invite_task_id)
                )
                conn.execute(
                    "INSERT INTO comments (task_id, author, text, created_at) VALUES (?, 'system', ?, ?)",
                    (invite_task_id, "Auto-closed: linked chat session ended.", now2),
                )
                _emit_event(conn, "task_updated", {"task_id": invite_task_id, "status": "done", "auto": True})
        return sess


def get_active_chat_session() -> dict | None:
    with _connect() as conn:
        row = conn.execute(
            "SELECT * FROM chat_sessions WHERE status='active' ORDER BY id DESC LIMIT 1"
        ).fetchone()
        if not row:
            return None
        sess = dict(row)
        sess["duration_seconds"] = _session_duration_seconds(sess)
        sess["linked_tasks"] = _session_linked_tasks_summaries(conn, sess["id"])
        return sess


def get_latest_chat_session() -> dict | None:
    """Most recent session regardless of status, so a viewer (e.g. the dashboard)
    can still show the transcript of one that just ended."""
    with _connect() as conn:
        row = conn.execute("SELECT * FROM chat_sessions ORDER BY id DESC LIMIT 1").fetchone()
        if not row:
            return None
        sess = dict(row)
        sess["duration_seconds"] = _session_duration_seconds(sess)
        sess["linked_tasks"] = _session_linked_tasks_summaries(conn, sess["id"])
        return sess


def list_chat_sessions(limit: int = 20) -> list[dict]:
    """Legacy uncursored listing -- kept for existing callers (the MCP tool);
    list_chat_sessions_paginated below is the real tranche-2 history API."""
    with _connect() as conn:
        rows = conn.execute(
            "SELECT * FROM chat_sessions ORDER BY id DESC LIMIT ?", (limit,)
        ).fetchall()
        return [dict(r) for r in rows]


def list_chat_sessions_paginated(cursor: int | None = None, limit: int = 20) -> dict:
    """Cursor-paginated session index -- offset pagination silently skips or
    duplicates rows when new sessions are created between page fetches;
    cursoring on a strictly-decreasing id doesn't (OpenClaw's review).

    `limit` is a hard maximum on `sessions` -- always. The active session (if
    any) is returned separately as `pinned_active`, not folded into
    `sessions` -- OpenClaw's explicit call on review: silently returning
    limit+1 rows when a session happens to be active is surprising,
    state-dependent API behavior even if documented; a caller that wants it
    combined can concat the two arrays itself. `pinned_active` is only
    populated on page 1 (cursor is None) and is always excluded from the
    normal cursor query, so it can never appear twice across pages -- it's
    always the highest id anyway (only one session is ever active, and a new
    one can't start while another is active), so this exclusion is exact, not
    a heuristic."""
    if cursor is not None and cursor < 0:
        raise ValueError("cursor must be >= 0")
    limit = max(1, min(limit, 100))
    with _connect() as conn:
        pinned_active = None
        if cursor is None:
            row = conn.execute(
                "SELECT * FROM chat_sessions WHERE status='active' ORDER BY id DESC LIMIT 1"
            ).fetchone()
            pinned_active = dict(row) if row else None

        q = "SELECT * FROM chat_sessions WHERE 1=1"
        params: list[Any] = []
        if cursor is not None:
            q += " AND id < ?"
            params.append(cursor)
        if pinned_active:
            q += " AND id != ?"
            params.append(pinned_active["id"])
        q += " ORDER BY id DESC LIMIT ?"
        params.append(limit + 1)  # fetch one extra to detect has_more without a second COUNT query
        rows = conn.execute(q, params).fetchall()
        has_more = len(rows) > limit
        page = [dict(r) for r in rows[:limit]]

        all_sessions = ([pinned_active] if pinned_active else []) + page
        ids = [s["id"] for s in all_sessions]
        counts: dict[int, int] = {}
        linked_map: dict[int, list[dict]] = {}
        if ids:
            placeholders = ",".join("?" * len(ids))
            for r in conn.execute(
                f"SELECT session_id, COUNT(*) AS c FROM chat_messages WHERE session_id IN ({placeholders}) GROUP BY session_id",
                ids,
            ).fetchall():
                counts[r["session_id"]] = r["c"]
            # Single batched query for all sessions on this page, not one
            # query per session (OpenClaw's review: no N+1 request explosion).
            for r in conn.execute(
                f"SELECT tcs.session_id, tcs.task_id, tcs.relation_type, t.title, t.status "
                f"FROM task_chat_sessions tcs JOIN tasks t ON t.id = tcs.task_id "
                f"WHERE tcs.session_id IN ({placeholders})",
                ids,
            ).fetchall():
                linked_map.setdefault(r["session_id"], []).append({
                    "task_id": r["task_id"], "relation_type": r["relation_type"],
                    "title": r["title"], "status": r["status"],
                })

        for s in all_sessions:
            s["message_count"] = counts.get(s["id"], 0)
            s["duration_seconds"] = _session_duration_seconds(s)
            s["linked_tasks"] = linked_map.get(s["id"], [])

        next_cursor = page[-1]["id"] if page and has_more else None
        return {
            "pinned_active": pinned_active,
            "sessions": page,
            "next_cursor": next_cursor,
            "has_more": has_more,
        }


def get_chat_session(session_id: int) -> dict | None:
    """Single-session detail -- status/duration/linked_tasks/message_count,
    same shape as one row from list_chat_sessions_paginated. Used by the
    dashboard's transcript drawer header and to refresh in place when a
    session's status changes (e.g. on chat_session_ended) without needing
    the heavier full export."""
    with _connect() as conn:
        row = conn.execute("SELECT * FROM chat_sessions WHERE id=?", (session_id,)).fetchone()
        if not row:
            return None
        sess = dict(row)
        sess["duration_seconds"] = _session_duration_seconds(sess)
        sess["linked_tasks"] = _session_linked_tasks_summaries(conn, session_id)
        sess["message_count"] = conn.execute(
            "SELECT COUNT(*) AS c FROM chat_messages WHERE session_id=?", (session_id,)
        ).fetchone()["c"]
        return sess


def get_chat_session_messages(
    session_id: int, after_id: int = 0, limit: int = 50, before_id: int | None = None,
) -> dict | None:
    """Paginated transcript fetch. Returns None if the session doesn't exist
    (caller maps that to a 404). Ascending id order is correct and safe to
    paginate forward even while an active session is still appending -- new
    messages only ever get higher ids, so a page already fetched never
    shifts.

    `before_id`, if given, switches to descending pagination (fetching the
    page just older than `before_id`) instead of the default forward/
    ascending mode -- mutually exclusive with `after_id`, used to page
    further back from an around-message window (tranche 3, see
    get_chat_session_messages_around) without walking from message 1."""
    if before_id is not None:
        if before_id < 0:
            raise ValueError("before_id must be >= 0")
        limit = max(1, min(limit, 200))
        with _connect() as conn:
            if not conn.execute("SELECT id FROM chat_sessions WHERE id=?", (session_id,)).fetchone():
                return None
            rows = conn.execute(
                "SELECT * FROM chat_messages WHERE session_id=? AND id<? ORDER BY id DESC LIMIT ?",
                (session_id, before_id, limit + 1),
            ).fetchall()
            has_more = len(rows) > limit
            messages = [dict(r) for r in reversed(rows[:limit])]  # re-ascend for consistent rendering order
            next_before_id = messages[0]["id"] if messages and has_more else None
            return {"messages": messages, "next_before_id": next_before_id, "has_more": has_more}

    if after_id < 0:
        raise ValueError("after_id must be >= 0")
    limit = max(1, min(limit, 200))
    with _connect() as conn:
        if not conn.execute("SELECT id FROM chat_sessions WHERE id=?", (session_id,)).fetchone():
            return None
        rows = conn.execute(
            "SELECT * FROM chat_messages WHERE session_id=? AND id>? ORDER BY id ASC LIMIT ?",
            (session_id, after_id, limit + 1),
        ).fetchall()
        has_more = len(rows) > limit
        messages = [dict(r) for r in rows[:limit]]
        next_after_id = messages[-1]["id"] if messages and has_more else None
        return {"messages": messages, "next_after_id": next_after_id, "has_more": has_more}


def get_chat_session_messages_around(
    session_id: int, message_id: int, before: int = 25, after: int = 25,
) -> dict | None:
    """Bounded window centered on `message_id` -- lets a search hit (or any
    other direct reference to one message) open a transcript AT that message
    without paging through everything before it. Returns None if the session
    doesn't exist; raises NotFoundError if message_id doesn't belong to that
    session (mismatched/malformed message references must be distinguishable
    from a missing session -- OpenClaw's review, tranche 3).

    before/after are validated, not silently clamped, on the low end -- a
    negative window size is malformed input (400), not a value to coerce to
    0 (OpenClaw's live black-box catch: before=-1 was returning 200 with the
    negative silently treated as zero)."""
    if before < 0 or after < 0:
        raise ValueError("before/after must be >= 0")
    if before > 100 or after > 100:
        raise ValueError("before/after must be <= 100")
    with _connect() as conn:
        if not conn.execute("SELECT id FROM chat_sessions WHERE id=?", (session_id,)).fetchone():
            return None
        if not conn.execute(
            "SELECT id FROM chat_messages WHERE id=? AND session_id=?", (message_id, session_id)
        ).fetchone():
            raise NotFoundError(f"message {message_id} not found in session {session_id}")

        before_rows = conn.execute(
            "SELECT * FROM chat_messages WHERE session_id=? AND id<? ORDER BY id DESC LIMIT ?",
            (session_id, message_id, before),
        ).fetchall()
        after_rows = conn.execute(
            "SELECT * FROM chat_messages WHERE session_id=? AND id>=? ORDER BY id ASC LIMIT ?",
            (session_id, message_id, after + 1),
        ).fetchall()

        before_msgs = [dict(r) for r in reversed(before_rows)]
        after_msgs = [dict(r) for r in after_rows]
        all_msgs = before_msgs + after_msgs

        return {
            "messages": all_msgs,
            "target_message_id": message_id,
            "earliest_loaded_id": all_msgs[0]["id"] if all_msgs else None,
            "latest_loaded_id": all_msgs[-1]["id"] if all_msgs else None,
            # Exact has_more signals, not heuristics: before/after fetched
            # exactly `before`/`after+1` rows when a full page exists, so
            # hitting that count means there's at least one more beyond it.
            "has_more_before": len(before_rows) == before and before > 0,
            "has_more_after": len(after_rows) > after,
        }


def export_chat_session(session_id: int, fmt: str = "json") -> dict | None:
    """Full transcript + metadata as a consistent snapshot -- an explicit
    transaction so a message arriving mid-export (from the still-active
    session) can't produce a torn export where the message list and the
    session's own message_count-equivalent disagree (OpenClaw's review)."""
    with _connect() as conn:
        conn.execute("BEGIN")
        sess_row = conn.execute("SELECT * FROM chat_sessions WHERE id=?", (session_id,)).fetchone()
        if not sess_row:
            conn.execute("COMMIT")
            return None
        msg_rows = conn.execute(
            "SELECT * FROM chat_messages WHERE session_id=? ORDER BY id ASC", (session_id,)
        ).fetchall()
        linked = _session_linked_tasks_summaries(conn, session_id)
        conn.execute("COMMIT")

    sess = dict(sess_row)
    sess["duration_seconds"] = _session_duration_seconds(sess)
    messages = [dict(r) for r in msg_rows]
    data = {
        "schema_version": 1,
        "exported_at": _now(),
        "session": sess,
        "linked_tasks": linked,
        "messages": messages,
    }
    if fmt == "markdown":
        lines = [
            f"# CollabHub chat session #{session_id}",
            "",
            f"- Status: {sess['status']}",
            f"- Started by: {sess.get('started_by') or '?'} at {sess['started_at']}",
        ]
        if sess.get("ended_at"):
            lines.append(f"- Ended at: {sess['ended_at']} (duration: {sess.get('duration_seconds')}s)")
        if linked:
            lines.append("- Linked tasks: " + ", ".join(f"#{t['task_id']} ({t['relation_type']})" for t in linked))
        lines += ["", "---", ""]
        for m in messages:
            # Preserve multiline message bodies as their own indented block
            # rather than collapsing newlines, so multi-paragraph messages
            # stay readable in the exported file.
            body = m["text"].replace("\n", "\n  ")
            lines.append(f"**{m['author'] or '?'}** ({m['created_at']}) [#{m['id']}]:")
            lines.append(f"  {body}")
            lines.append("")
        body_text = "\n".join(lines)
        return {"content_type": "text/markdown; charset=utf-8", "filename": f"chat-session-{session_id}.md", "body": body_text}
    return {
        "content_type": "application/json; charset=utf-8",
        "filename": f"chat-session-{session_id}.json",
        "body": json.dumps(data, indent=2),
    }


# ---------------------------------------------------------------------------
# Search (tranche 3)
# ---------------------------------------------------------------------------

def _search_rowid(source_type: str, source_id: int) -> int:
    return _SEARCH_TYPE_CODE[source_type] * 10_000_000_000 + source_id


def _task_search_content(title: str, description: str, tags_str: str) -> str:
    """Task indexed text = title + description + space-joined tags (defined
    explicitly per OpenClaw's review -- tags ARE included)."""
    tags = ", ".join(t for t in (tags_str or "").split(",") if t)
    return "\n".join(filter(None, [title, description, tags]))


def _index_for_search(
    conn: sqlite3.Connection, source_type: str, source_id: int, parent_id: int | None,
    content: str, author: str, created_at: str,
) -> None:
    """Delete+reinsert a row's search entry. MUST be called with the SAME
    `conn` (and therefore the same transaction) as the canonical mutation --
    a source commit followed by a separate index commit would let the two
    drift on any partial failure (OpenClaw's review: this is a hard
    requirement, not a style preference)."""
    rowid = _search_rowid(source_type, source_id)
    # Strip the snippet sentinel characters from indexed content -- without
    # this, a user typing literal \x01/\x02 (control characters, but valid
    # in a JSON string) would survive esc() unchanged client-side and become
    # an indistinguishable-from-real <mark> tag. Stripping at index time
    # means the ONLY \x01/\x02 that can ever appear in a snippet() result are
    # the ones snippet() itself inserts -- making "only our insertions
    # become markup" literally true, not just true in practice (OpenClaw's
    # review, tranche 3 served-UI pass).
    content = content.replace(SNIPPET_START, "").replace(SNIPPET_END, "")
    conn.execute("DELETE FROM search_index WHERE rowid=?", (rowid,))
    conn.execute(
        "INSERT INTO search_index (rowid, content, source_type, source_id, parent_id, author, created_at) "
        "VALUES (?, ?, ?, ?, ?, ?, ?)",
        (rowid, content, source_type, source_id, parent_id, author, created_at),
    )


def _rebuild_search_index(conn: sqlite3.Connection) -> int:
    """Full delete+reindex of everything -- used once on first-ever table
    creation (backfill) and exposed publicly below as the drift-
    reconciliation tool (OpenClaw's review: search must cover pre-existing
    data, and there must be a way to detect/fix drift if it ever happens)."""
    conn.execute("DELETE FROM search_index")
    count = 0
    for row in conn.execute("SELECT * FROM tasks").fetchall():
        content = _task_search_content(row["title"], row["description"], row["tags"])
        _index_for_search(conn, "task", row["id"], None, content, row["created_by"], row["updated_at"])
        count += 1
    for row in conn.execute("SELECT * FROM comments").fetchall():
        _index_for_search(conn, "comment", row["id"], row["task_id"], row["text"], row["author"], row["created_at"])
        count += 1
    for row in conn.execute("SELECT * FROM notes").fetchall():
        _index_for_search(conn, "note", row["id"], None, row["text"], row["author"], row["created_at"])
        count += 1
    for row in conn.execute("SELECT * FROM chat_messages").fetchall():
        _index_for_search(conn, "chat_message", row["id"], row["session_id"], row["text"], row["author"], row["created_at"])
        count += 1
    return count


def rebuild_search_index() -> int:
    """Public entry point for the drift-reconciliation tool -- safe to call
    any time (e.g. from a maintenance script or test) since it's a full
    rebuild, not an incremental patch. Returns the number of rows indexed."""
    with _connect() as conn:
        conn.execute("BEGIN")
        count = _rebuild_search_index(conn)
        conn.execute("COMMIT")
        return count


# --- Maintenance-only deletion helpers ---------------------------------
# Deliberately NOT wired into REST/MCP in this tranche -- "can either agent
# delete arbitrary shared history" is its own product/audit-trail design
# question (who's allowed, does it emit an event, is it soft or hard) that
# deserves a real discussion, not a side effect of shipping search. These
# exist so the one-off cleanup of accumulated acceptance-test debris (search
# surfaced it -- OpenClaw's review, tranche 3) can be done precisely and
# repeatably instead of hand-editing the DB file.

def delete_task(task_id: int) -> bool:
    with _connect() as conn:
        comment_ids = [r["id"] for r in conn.execute("SELECT id FROM comments WHERE task_id=?", (task_id,)).fetchall()]
        conn.execute("DELETE FROM task_chat_sessions WHERE task_id=?", (task_id,))
        conn.execute("DELETE FROM comments WHERE task_id=?", (task_id,))
        cur = conn.execute("DELETE FROM tasks WHERE id=?", (task_id,))
        removed = cur.rowcount > 0
        if removed:
            conn.execute("DELETE FROM search_index WHERE rowid=?", (_search_rowid("task", task_id),))
            for cid in comment_ids:
                conn.execute("DELETE FROM search_index WHERE rowid=?", (_search_rowid("comment", cid),))
        return removed


def delete_note(note_id: int) -> bool:
    with _connect() as conn:
        cur = conn.execute("DELETE FROM notes WHERE id=?", (note_id,))
        removed = cur.rowcount > 0
        if removed:
            conn.execute("DELETE FROM search_index WHERE rowid=?", (_search_rowid("note", note_id),))
        return removed


def delete_chat_message(message_id: int) -> bool:
    with _connect() as conn:
        cur = conn.execute("DELETE FROM chat_messages WHERE id=?", (message_id,))
        removed = cur.rowcount > 0
        if removed:
            conn.execute("DELETE FROM search_index WHERE rowid=?", (_search_rowid("chat_message", message_id),))
        return removed


def _tokenize_for_search(query: str) -> list[str]:
    return re.findall(r"\w+", query, flags=re.UNICODE)


def search(query: str, types: list[str] | None = None, limit: int = 20, offset: int = 0) -> dict:
    """Unified search across tasks/comments/notes/chat messages. Raises
    ValueError for any malformed input (REST maps to 400) -- one validation
    path shared by the REST route and the MCP tool, so they can never drift
    on what counts as a valid query (OpenClaw's review)."""
    query = (query or "").strip()
    if not query:
        raise ValueError("q must not be empty")
    if len(query) > SEARCH_MAX_QUERY_LEN:
        raise ValueError(f"q must be <= {SEARCH_MAX_QUERY_LEN} characters")
    tokens = _tokenize_for_search(query)
    if not tokens:
        raise ValueError("q must contain at least one searchable token")
    if len(tokens) > SEARCH_MAX_TOKENS:
        raise ValueError(f"q must contain <= {SEARCH_MAX_TOKENS} tokens")
    # Rebuilt into a safe, predictable MATCH expression -- the raw query
    # string never reaches FTS5's query parser, so it can't produce a syntax
    # error or an unintended operator no matter what punctuation/special
    # characters the user typed (OpenClaw's review).
    fts_query = " AND ".join(f'"{t}"*' for t in tokens)

    if types:
        unknown = [t for t in types if t not in SEARCH_TYPES]
        if unknown:
            raise ValueError(f"unknown type(s): {unknown!r}; must be a subset of {SEARCH_TYPES}")
    else:
        types = list(SEARCH_TYPES)

    if offset < 0:
        raise ValueError("offset must be >= 0")
    if limit <= 0:
        raise ValueError("limit must be > 0")
    limit = min(limit, SEARCH_MAX_LIMIT)

    with _connect() as conn:
        placeholders = ",".join("?" * len(types))
        rows = conn.execute(
            f"SELECT source_type, source_id, parent_id, author, created_at, "
            f"snippet(search_index, 0, ?, ?, '...', 12) AS snippet, "
            f"bm25(search_index) AS rank "
            f"FROM search_index WHERE search_index MATCH ? AND source_type IN ({placeholders}) "
            f"ORDER BY rank ASC, created_at DESC, source_type ASC, source_id DESC "
            f"LIMIT ? OFFSET ?",
            [SNIPPET_START, SNIPPET_END, fts_query, *types, limit + 1, offset],
        ).fetchall()
        has_more = len(rows) > limit
        rows = rows[:limit]

        # Batched parent-context lookup -- one query per referenced
        # type/id-set, not one per result (OpenClaw's review: no N+1).
        task_ids = {r["source_id"] for r in rows if r["source_type"] == "task"}
        comment_task_ids = {r["parent_id"] for r in rows if r["source_type"] == "comment" and r["parent_id"] is not None}
        chat_session_ids = {r["parent_id"] for r in rows if r["source_type"] == "chat_message" and r["parent_id"] is not None}

        task_status: dict[int, dict] = {}
        for tid in task_ids | comment_task_ids:
            trow = conn.execute("SELECT id, title, status FROM tasks WHERE id=?", (tid,)).fetchone()
            if trow:
                task_status[tid] = dict(trow)
        session_status: dict[int, dict] = {}
        for sid in chat_session_ids:
            srow = conn.execute("SELECT id, status FROM chat_sessions WHERE id=?", (sid,)).fetchone()
            if srow:
                session_status[sid] = dict(srow)

        results = []
        for r in rows:
            d = dict(r)
            st, sid, pid = d["source_type"], d["source_id"], d["parent_id"]
            if st == "task":
                t = task_status.get(sid)
                context = {"status": t["status"]} if t else {}
            elif st == "comment":
                t = task_status.get(pid) if pid is not None else None
                context = {"task_id": pid, "task_title": t["title"], "task_status": t["status"]} if t else {"task_id": pid}
            elif st == "chat_message":
                s = session_status.get(pid) if pid is not None else None
                context = {"session_id": pid, "session_status": s["status"]} if s else {"session_id": pid}
            else:
                context = {}
            results.append({
                "source_type": st, "source_id": sid, "parent_id": pid,
                "author": d["author"], "created_at": d["created_at"],
                "rank": d["rank"], "snippet": d["snippet"], "context": context,
            })

        return {
            "query": query, "types": types, "limit": limit, "offset": offset,
            "has_more": has_more, "results": results,
        }


def send_chat_message(session_id: int, author: str, text: str) -> dict | None:
    """Append a message. Returns None (no-op) if the session doesn't exist or has
    already ended, so a straggling agent can't resurrect a closed session."""
    with _connect() as conn:
        sess = conn.execute("SELECT * FROM chat_sessions WHERE id=?", (session_id,)).fetchone()
        if not sess or sess["status"] != "active":
            return None
        now = _now()
        cur = conn.execute(
            "INSERT INTO chat_messages (session_id, author, text, created_at) VALUES (?, ?, ?, ?)",
            (session_id, author, text, now),
        )
        row = conn.execute("SELECT * FROM chat_messages WHERE id=?", (cur.lastrowid,)).fetchone()
        msg = dict(row)
        _emit_event(conn, "chat_message", {"session_id": session_id, "message_id": msg["id"], "author": author, "text": text})
        _touch_presence(conn, author, "send_chat_message")
        _index_for_search(conn, "chat_message", msg["id"], session_id, text, author, now)
        return msg


def poll_chat_messages(session_id: int, since: int = 0) -> dict:
    """Messages with id > since, oldest first, plus whether the session is still
    active -- a poller uses that to know when to stop."""
    with _connect() as conn:
        sess = conn.execute("SELECT * FROM chat_sessions WHERE id=?", (session_id,)).fetchone()
        rows = conn.execute(
            "SELECT * FROM chat_messages WHERE session_id=? AND id>? ORDER BY id ASC",
            (session_id, since),
        ).fetchall()
        return {
            "messages": [dict(r) for r in rows],
            "active": bool(sess) and sess["status"] == "active",
        }


def full_state() -> dict:
    """Snapshot every panel the dashboard needs, plus the event id that
    snapshot is consistent as-of (`event_cursor`). A client that connects its
    SSE/WS stream with `?since=event_cursor` is then guaranteed no gap and no
    duplicate: every event <= event_cursor is already reflected here, every
    event > event_cursor arrives on the stream. That guarantee only holds if
    all these reads share one WAL snapshot -- an explicit transaction on a
    single connection, not five independent auto-committing calls (which is
    what this used to be; OpenClaw's catch, 2026-08-14, before it shipped)."""
    with _connect() as conn:
        conn.execute("BEGIN")
        tasks = [_task_row_to_dict(r) for r in conn.execute("SELECT * FROM tasks ORDER BY updated_at DESC").fetchall()]
        _hydrate_checklists(conn, tasks)
        notes = [dict(r) for r in conn.execute("SELECT * FROM notes ORDER BY created_at DESC LIMIT 100").fetchall()]
        active_chat_row = conn.execute(
            "SELECT * FROM chat_sessions WHERE status='active' ORDER BY id DESC LIMIT 1"
        ).fetchone()
        latest_chat_row = conn.execute("SELECT * FROM chat_sessions ORDER BY id DESC LIMIT 1").fetchone()
        presence = [dict(r) for r in conn.execute("SELECT * FROM agent_presence ORDER BY last_seen DESC").fetchall()]
        event_cursor = (conn.execute("SELECT MAX(id) AS m FROM events").fetchone()["m"]) or 0

        # Match the shape get_active_chat_session()/get_latest_chat_session()
        # return -- duration_seconds + linked_tasks -- so a dashboard reading
        # from /api/state doesn't see a different (poorer) session shape than
        # one reading from the dedicated endpoints. Cheap here: at most two
        # sessions (often the same one), not a list needing batching.
        active_chat = None
        if active_chat_row:
            active_chat = dict(active_chat_row)
            active_chat["duration_seconds"] = _session_duration_seconds(active_chat)
            active_chat["linked_tasks"] = _session_linked_tasks_summaries(conn, active_chat["id"])
        latest_chat = None
        if latest_chat_row:
            if active_chat_row and latest_chat_row["id"] == active_chat_row["id"]:
                latest_chat = active_chat
            else:
                latest_chat = dict(latest_chat_row)
                latest_chat["duration_seconds"] = _session_duration_seconds(latest_chat)
                latest_chat["linked_tasks"] = _session_linked_tasks_summaries(conn, latest_chat["id"])

        conn.execute("COMMIT")
        return {
            "tasks": tasks,
            "notes": notes,
            "active_chat_session": active_chat,
            "latest_chat_session": latest_chat,
            "presence": presence,
            "event_cursor": event_cursor,
        }
