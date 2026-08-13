"""SQLite-backed storage for CollabHub: shared tasks + notes.

Single-file DB, opened per-call (sqlite3 handles this fine at our scale --
two low-frequency agent clients, not a real traffic load). WAL mode so the
dashboard's polling reads never block a concurrent write from either agent.
"""
from __future__ import annotations

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
        return _task_row_to_dict(row)


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
        return _task_row_to_dict(row)


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
        return dict(row)


def post_note(author: str, text: str) -> dict:
    with _connect() as conn:
        now = _now()
        cur = conn.execute(
            "INSERT INTO notes (author, text, created_at) VALUES (?, ?, ?)", (author, text, now)
        )
        row = conn.execute("SELECT * FROM notes WHERE id=?", (cur.lastrowid,)).fetchone()
        return dict(row)


def list_notes(limit: int = 50) -> list[dict]:
    with _connect() as conn:
        rows = conn.execute(
            "SELECT * FROM notes ORDER BY created_at DESC LIMIT ?", (limit,)
        ).fetchall()
        return [dict(r) for r in rows]


def full_state() -> dict:
    return {"tasks": list_tasks(), "notes": list_notes(limit=100)}
