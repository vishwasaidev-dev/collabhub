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
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

DB_PATH = Path(__file__).resolve().parent / "collabhub.sqlite3"

VALID_STATUSES = ("open", "claimed", "in_progress", "blocked", "done")

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
                created_at TEXT NOT NULL,
                updated_at TEXT NOT NULL
            )
            """
        )
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

        my_tasks = conn.execute(
            "SELECT * FROM tasks WHERE assignee=? AND status!='done' ORDER BY updated_at DESC",
            (agent,),
        ).fetchall()
        active_chat = conn.execute(
            "SELECT * FROM chat_sessions WHERE status='active' ORDER BY id DESC LIMIT 1"
        ).fetchone()

        return {
            "agent": agent,
            "since": since,
            "next_cursor": next_cursor,
            "unread_count": len(events),
            "events": events,
            "my_open_tasks": [_task_row_to_dict(r) for r in my_tasks],
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


def create_task(title: str, description: str = "", created_by: str = "", tags: list[str] | None = None) -> dict:
    now = _now()
    tags_str = ",".join(tags or [])
    with _connect() as conn:
        cur = conn.execute(
            "INSERT INTO tasks (title, description, status, created_by, tags, created_at, updated_at) "
            "VALUES (?, ?, 'open', ?, ?, ?, ?)",
            (title, description, created_by, tags_str, now, now),
        )
        task_id = cur.lastrowid
        row = conn.execute("SELECT * FROM tasks WHERE id=?", (task_id,)).fetchone()
        task = _task_row_to_dict(row)
        _emit_event(conn, "task_created", {"task_id": task_id, "title": title, "created_by": created_by})
        _touch_presence(conn, created_by, "create_task")
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
        return [_task_row_to_dict(r) for r in rows]


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
        return task


def update_task(
    task_id: int,
    status: str | None = None,
    description: str | None = None,
    assignee: str | None = None,
) -> dict | None:
    if status is not None and status not in VALID_STATUSES:
        raise ValueError(f"invalid status {status!r}; must be one of {VALID_STATUSES}")
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
        fields.append("updated_at=?")
        params.append(_now())
        params.append(task_id)
        conn.execute(f"UPDATE tasks SET {', '.join(fields)} WHERE id=?", params)
        row = conn.execute("SELECT * FROM tasks WHERE id=?", (task_id,)).fetchone()
        task = _task_row_to_dict(row)
        _emit_event(conn, "task_updated", {"task_id": task_id, "status": task["status"], "assignee": task.get("assignee")})
        if assignee:
            _touch_presence(conn, assignee, "update_task")
        return task


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
        return note


def list_notes(limit: int = 50) -> list[dict]:
    with _connect() as conn:
        rows = conn.execute(
            "SELECT * FROM notes ORDER BY created_at DESC LIMIT ?", (limit,)
        ).fetchall()
        return [dict(r) for r in rows]


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
            raise ValueError(f"task {task_id} not found")
        if not conn.execute("SELECT id FROM chat_sessions WHERE id=?", (session_id,)).fetchone():
            raise ValueError(f"chat session {session_id} not found")
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
            raise ValueError(f"invite_task_id {invite_task_id} not found")
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
    cursoring on a strictly-decreasing id doesn't (OpenClaw's review). The
    active session (if any) is pinned first on page 1 ONLY, and excluded from
    the normal cursor query so it can never appear twice across pages -- it's
    always the highest id anyway (only one session is ever active, and a new
    one can't start while another is active), so this exclusion is exact, not
    a heuristic."""
    limit = max(1, min(limit, 100))
    with _connect() as conn:
        active = None
        if cursor is None:
            row = conn.execute(
                "SELECT * FROM chat_sessions WHERE status='active' ORDER BY id DESC LIMIT 1"
            ).fetchone()
            active = dict(row) if row else None

        q = "SELECT * FROM chat_sessions WHERE 1=1"
        params: list[Any] = []
        if cursor is not None:
            q += " AND id < ?"
            params.append(cursor)
        if active:
            q += " AND id != ?"
            params.append(active["id"])
        q += " ORDER BY id DESC LIMIT ?"
        params.append(limit + 1)  # fetch one extra to detect has_more without a second COUNT query
        rows = conn.execute(q, params).fetchall()
        has_more = len(rows) > limit
        page = [dict(r) for r in rows[:limit]]

        all_sessions = ([active] if active else []) + page
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
        return {"sessions": all_sessions, "next_cursor": next_cursor, "has_more": has_more}


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


def get_chat_session_messages(session_id: int, after_id: int = 0, limit: int = 50) -> dict | None:
    """Paginated transcript fetch. Returns None if the session doesn't exist
    (caller maps that to a 404). Ascending id order is correct and safe to
    paginate even while an active session is still appending -- new messages
    only ever get higher ids, so a page already fetched never shifts."""
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
