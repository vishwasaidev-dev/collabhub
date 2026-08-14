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
import sqlite3
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

DB_PATH = Path(__file__).resolve().parent / "collabhub.sqlite3"

VALID_STATUSES = ("open", "claimed", "in_progress", "blocked", "done")


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
# Live chat
# ---------------------------------------------------------------------------

def start_chat_session(started_by: str = "", invite_task_id: int | None = None) -> dict:
    """Start a live chat session, or return the already-active one if there is one
    (only one session is active at a time -- both agents share it). `invite_task_id`,
    if given, is a task to auto-complete when this session ends (see end_chat_session)."""
    with _connect() as conn:
        existing = conn.execute(
            "SELECT * FROM chat_sessions WHERE status='active' ORDER BY id DESC LIMIT 1"
        ).fetchone()
        if existing:
            return dict(existing)
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
        return dict(row) if row else None


def get_latest_chat_session() -> dict | None:
    """Most recent session regardless of status, so a viewer (e.g. the dashboard)
    can still show the transcript of one that just ended."""
    with _connect() as conn:
        row = conn.execute("SELECT * FROM chat_sessions ORDER BY id DESC LIMIT 1").fetchone()
        return dict(row) if row else None


def list_chat_sessions(limit: int = 20) -> list[dict]:
    with _connect() as conn:
        rows = conn.execute(
            "SELECT * FROM chat_sessions ORDER BY id DESC LIMIT ?", (limit,)
        ).fetchall()
        return [dict(r) for r in rows]


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
    return {
        "tasks": list_tasks(),
        "notes": list_notes(limit=100),
        "active_chat_session": get_active_chat_session(),
        "latest_chat_session": get_latest_chat_session(),
        "presence": list_presence(),
    }
