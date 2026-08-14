"""CollabHub -- a tiny shared task board + notes log for Claude Code and
OpenClaw (or any other MCP-speaking agent) to coordinate work on this
machine.

Loopback-only (127.0.0.1), no auth: both sides reach it either directly
(Windows) or via WSL2 mirrored networking (WSL treats Windows' 127.0.0.1 as
its own), so there's no network boundary worth gating -- see
project_mcb_blender_frame.md / the blender MCP setup memory for why that's
true on this machine.

Built on the same Starlette + FastMCP shape as kinematicsworkbench/app.py:
FastMCP's streamable_http_app() mounted into an outer Starlette app, with
its lifespan forwarded explicitly (skip that and every /mcp request 500s
with "Task group is not initialized").
"""
from __future__ import annotations

import asyncio
import hashlib
import json
import os
import re
import subprocess
import time
import unicodedata
from datetime import datetime, timezone
from pathlib import Path
from typing import Any
from urllib.parse import urlparse

import uvicorn
from mcp.server.fastmcp import FastMCP
from mcp.server.transport_security import TransportSecuritySettings
from starlette.applications import Starlette
from starlette.requests import Request
from starlette.responses import FileResponse, HTMLResponse, JSONResponse, Response, StreamingResponse
from starlette.routing import Route, WebSocketRoute
from starlette.staticfiles import StaticFiles
from starlette.websockets import WebSocket, WebSocketDisconnect

import storage

ROOT = Path(__file__).resolve().parent
_INDEX_HTML = (ROOT / "templates" / "index.html").read_text(encoding="utf-8")

storage.init_db()
# Reconcile the attachment directory against the DB once at process start --
# a crash mid-upload, or between finalizing a file and committing its DB
# row, can leave a stray `.part` file or an unreferenced-but-finalized file
# behind; this cleans those up (and reports, without deleting, any DB row
# whose bytes are missing) rather than letting them accumulate silently
# across restarts (OpenClaw's review, tranche 5).
_attachment_reconcile_report = storage.reconcile_attachment_storage()
if any(_attachment_reconcile_report.values()):
    print(f"[collabhub] attachment storage reconciliation on startup: {_attachment_reconcile_report}")

mcp = FastMCP(
    "collabhub",
    transport_security=TransportSecuritySettings(enable_dns_rebinding_protection=False),
)


@mcp.tool(name="create_task", description=(
    "Create a new shared task on the board. Use this to hand work to the other agent, "
    "or to log work you're about to start so it's visible. priority is one of "
    "low/normal/high/urgent (default normal). due_date, if given, is a calendar date "
    "YYYY-MM-DD (e.g. '2026-03-15'), not a timestamp. Returns the created task."
))
def create_task_tool(
    title: str, description: str = "", created_by: str = "", tags: list[str] | None = None,
    priority: str = "normal", due_date: str | None = None,
) -> dict:
    return storage.create_task(title, description, created_by, tags, priority, due_date)


@mcp.tool(name="list_tasks", description=(
    "List shared tasks, optionally filtered by status (open/claimed/in_progress/blocked/done) "
    "and/or assignee. Call with no filters to see the whole board."
))
def list_tasks_tool(status: str | None = None, assignee: str | None = None) -> list[dict]:
    return storage.list_tasks(status, assignee)


@mcp.tool(name="get_task", description="Get one task's full detail, including its comment thread.")
def get_task_tool(task_id: int) -> dict | None:
    return storage.get_task(task_id)


@mcp.tool(name="claim_task", description=(
    "Claim an open task for yourself (pass your own identity as assignee, e.g. 'claude' or "
    "'openclaw') and set it in_progress. Overwrites any existing assignee -- check get_task "
    "first if you want to avoid stepping on the other agent's claim."
))
def claim_task_tool(task_id: int, assignee: str) -> dict | None:
    return storage.update_task(task_id, status="in_progress", assignee=assignee)


@mcp.tool(name="update_task", description=(
    "Update a task's status/description/priority/due_date. Omit fields you don't want to "
    "change. To CLEAR a due date (not just leave it unset), pass clear_due_date=true -- "
    "passing due_date alone only sets a new one, it can't distinguish 'leave it alone' from "
    "'remove it' the way a REST client's JSON body can via an explicit null. Raises on an "
    "invalid status/priority/date instead of silently corrupting the task."
))
def update_task_tool(
    task_id: int, status: str | None = None, description: str | None = None,
    assignee: str | None = None, priority: str | None = None,
    due_date: str | None = None, clear_due_date: bool = False,
) -> dict | None:
    # storage.update_task's due_date param is tri-state (omitted/None/value) via
    # a sentinel default -- that sentinel isn't representable in an MCP tool's
    # JSON schema, so this tool exposes the same three states through two
    # plain params instead: clear_due_date=true means "clear it" regardless of
    # due_date; otherwise a given due_date sets it; otherwise it's untouched.
    due_date_arg = storage._OMITTED
    if clear_due_date:
        due_date_arg = None
    elif due_date is not None:
        due_date_arg = due_date
    return storage.update_task(
        task_id, status=status, description=description, assignee=assignee,
        priority=priority, due_date=due_date_arg,
    )


@mcp.tool(name="add_checklist_item", description=(
    "Add a checklist item to a task (max 300 chars, max 50 items per task). "
    "Counts as task activity -- bumps the task's updated_at."
))
def add_checklist_item_tool(task_id: int, text: str) -> dict:
    return storage.add_checklist_item(task_id, text)


@mcp.tool(name="toggle_checklist_item", description="Set a checklist item's done state (must be a real boolean).")
def toggle_checklist_item_tool(task_id: int, item_id: int, done: bool) -> dict:
    return storage.set_checklist_item_done(task_id, item_id, done)


@mcp.tool(name="delete_checklist_item", description="Remove a checklist item. Idempotent -- returns whether anything was actually removed.")
def delete_checklist_item_tool(task_id: int, item_id: int) -> bool:
    return storage.delete_checklist_item(task_id, item_id)


@mcp.tool(name="list_attachments", description=(
    "List a task's file attachments (metadata only -- filename, size, uploader, download URL "
    "path -- never the file's bytes; those aren't representable in an MCP tool result, and "
    "your sandbox likely can't reach local disk paths anyway. Fetch "
    "GET /api/tasks/{task_id}/attachments/{id}/download over HTTP for the actual content, same "
    "as the /files/ static mount."
))
def list_attachments_tool(task_id: int) -> list[dict]:
    return storage.list_attachments(task_id)


@mcp.tool(name="comment_task", description=(
    "Add a comment/log entry to a task -- progress notes, blockers, questions for the other "
    "agent, links to files or artifacts produced. Identify yourself as author."
))
def comment_task_tool(task_id: int, author: str, text: str) -> dict | None:
    return storage.add_comment(task_id, author, text)


@mcp.tool(name="complete_task", description=(
    "Mark a task done and record a closing summary as a comment. Identify yourself as author."
))
def complete_task_tool(task_id: int, author: str, summary: str = "") -> dict | None:
    if summary:
        storage.add_comment(task_id, author, summary)
    return storage.update_task(task_id, status="done")


@mcp.tool(name="post_note", description=(
    "Post a freeform note to the shared log -- context, decisions, things the other agent "
    "should know that don't belong on a specific task. Identify yourself as author."
))
def post_note_tool(author: str, text: str) -> dict:
    return storage.post_note(author, text)


@mcp.tool(name="list_notes", description="List recent shared notes, most recent first.")
def list_notes_tool(limit: int = 50) -> list[dict]:
    return storage.list_notes(limit)


@mcp.tool(name="get_note", description="Fetch one note by id, regardless of how far back it is -- list_notes only returns the newest slice.")
def get_note_tool(note_id: int) -> dict | None:
    return storage.get_note(note_id)


@mcp.tool(name="start_chat_session", description=(
    "Start a live back-and-forth chat session with the other agent, or join the one "
    "already active (only one runs at a time). Returns the session, including its id -- "
    "pass that id to send_chat_message/poll_chat_messages. Meant for short, real-time "
    "exchanges the user is watching, not for durable handoffs (use tasks/notes for those). "
    "Pass invite_task_id if you also created a task inviting the other agent to join -- "
    "end_chat_session will then auto-close that task instead of leaving it stale."
))
def start_chat_session_tool(started_by: str = "", invite_task_id: int | None = None) -> dict:
    return storage.start_chat_session(started_by, invite_task_id)


@mcp.tool(name="end_chat_session", description=(
    "End an active chat session. Call this when the user says to finish/close the session. "
    "Auto-completes the session's linked invite task, if it had one."
))
def end_chat_session_tool(session_id: int) -> dict | None:
    return storage.end_chat_session(session_id)


@mcp.tool(name="get_active_chat_session", description="Get the currently active chat session, if any.")
def get_active_chat_session_tool() -> dict | None:
    return storage.get_active_chat_session()


@mcp.tool(name="list_chat_sessions", description="List past chat sessions (most recent first), so old transcripts are browsable, not just the live one.")
def list_chat_sessions_tool(limit: int = 20) -> list[dict]:
    return storage.list_chat_sessions(limit)


@mcp.tool(name="list_chat_session_history", description=(
    "Cursor-paginated session index (active session pinned first on page 1). Pass the "
    "previous response's next_cursor to fetch the next page; has_more tells you when to "
    "stop. Each session includes message_count, duration_seconds (null while active), "
    "and linked_tasks."
))
def list_chat_session_history_tool(cursor: int | None = None, limit: int = 20) -> dict:
    return storage.list_chat_sessions_paginated(cursor, limit)


@mcp.tool(name="get_chat_session", description=(
    "Single-session detail: status, duration_seconds (null while active), linked_tasks, "
    "message_count. Returns null if the session doesn't exist."
))
def get_chat_session_tool(session_id: int) -> dict | None:
    return storage.get_chat_session(session_id)


@mcp.tool(name="get_chat_session_messages", description=(
    "Paginated transcript for one session. Default is oldest-first: pass the previous response's "
    "next_after_id as after_id to fetch the next (newer) page. Pass before_id instead (mutually "
    "exclusive with after_id) to page backward/older -- e.g. continuing further back from a "
    "get_chat_session_messages_around window. Returns null if the session doesn't exist."
))
def get_chat_session_messages_tool(
    session_id: int, after_id: int = 0, limit: int = 50, before_id: int | None = None,
) -> dict | None:
    return storage.get_chat_session_messages(session_id, after_id, limit, before_id)


@mcp.tool(name="get_chat_session_messages_around", description=(
    "Bounded window of messages centered on message_id, so you can jump straight to a specific "
    "message (e.g. a search hit) without paging from the start. Raises if message_id doesn't "
    "belong to session_id. Returns null if the session doesn't exist."
))
def get_chat_session_messages_around_tool(session_id: int, message_id: int, before: int = 25, after: int = 25) -> dict | None:
    return storage.get_chat_session_messages_around(session_id, message_id, before, after)


@mcp.tool(name="link_task_session", description=(
    "Link a task to a chat session with a relation_type (lowercase token, e.g. 'discussion' "
    "or 'related' -- 'invite' is reserved, created automatically by start_chat_session). "
    "Idempotent -- linking the same pair twice is a no-op, not an error."
))
def link_task_session_tool(task_id: int, session_id: int, relation_type: str, linked_by: str = "") -> dict:
    return storage.link_task_session(task_id, session_id, relation_type, linked_by)


@mcp.tool(name="unlink_task_session", description=(
    "Remove a task<->session link. The 'invite' relation can't be removed this way -- it's "
    "lifecycle-owned by the session. Returns whether a link was actually removed."
))
def unlink_task_session_tool(task_id: int, session_id: int, relation_type: str) -> bool:
    return storage.unlink_task_session(task_id, session_id, relation_type)


@mcp.tool(name="export_chat_session", description=(
    "Export a session's full transcript + metadata as a consistent snapshot. format is "
    "'json' or 'markdown'. Returns null if the session doesn't exist."
))
def export_chat_session_tool(session_id: int, format: str = "json") -> dict | None:
    return storage.export_chat_session(session_id, format)


@mcp.tool(name="send_chat_message", description=(
    "Send one message in an active chat session. Identify yourself as author "
    "('claude' or 'openclaw'). No-ops (returns null) if the session has already ended."
))
def send_chat_message_tool(session_id: int, author: str, text: str) -> dict | None:
    return storage.send_chat_message(session_id, author, text)


@mcp.tool(name="poll_chat_messages", description=(
    "Poll for chat messages posted after message id `since` (0 for all). Response also "
    "reports whether the session is still active, so a poller knows when to stop."
))
def poll_chat_messages_tool(session_id: int, since: int = 0) -> dict:
    return storage.poll_chat_messages(session_id, since)


@mcp.tool(name="catch_up", description=(
    "One-call digest for `agent`: every event (task created/updated/commented, note posted, "
    "chat message) since their last ack_events call, their own open tasks, and the active chat "
    "session. Non-destructive -- does NOT advance your cursor, so calling it again before "
    "acking returns the same events. Call ack_events once you've durably processed the batch. "
    "Prefer this over separately calling list_tasks + list_notes + get_active_chat_session."
))
def catch_up_tool(agent: str, after_cursor: int | None = None) -> dict:
    return storage.catch_up(agent, after_cursor)


@mcp.tool(name="ack_events", description=(
    "Durably advance `agent`'s event cursor to `cursor` (monotonic -- never regresses). Call "
    "this after you've processed a batch of events returned by catch_up/wait_for_events, so "
    "they aren't re-delivered next time."
))
def ack_events_tool(agent: str, cursor: int) -> dict:
    return storage.ack_events(agent, cursor)


@mcp.tool(name="wait_for_events", description=(
    "Long-poll for new events since event id `since` (0 for all). Blocks server-side up to "
    "`timeout` seconds (capped at 30) and returns as soon as any new event exists, or an empty "
    "list once the timeout elapses. Use this instead of polling list_tasks/list_notes on a "
    "fixed interval -- it's push-shaped without needing a WebSocket client."
))
async def wait_for_events_tool(since: int = 0, timeout: float = 25.0) -> dict:
    timeout = min(max(timeout, 0.0), 30.0)
    loop = asyncio.get_event_loop()
    deadline = loop.time() + timeout
    while True:
        events = storage.get_events_since(since)
        if events or loop.time() >= deadline:
            next_cursor = events[-1]["id"] if events else storage.latest_event_id()
            return {"since": since, "next_cursor": next_cursor, "events": events}
        await asyncio.sleep(0.5)


@mcp.tool(name="list_presence", description=(
    "See when each known agent was last active (last_seen, last_action) -- for checking "
    "whether the other side is actually online right now, not just theoretically reachable."
))
def list_presence_tool() -> list[dict]:
    return storage.list_presence()


@mcp.tool(name="search", description=(
    "Full-text search across tasks, task comments, notes, and chat messages. `types` (optional) "
    "restricts to a subset, e.g. ['task','chat_message'] -- omit for all types. Results are "
    "ranked by relevance, each with a snippet and enough context (parent task/session id+title/"
    "status) to navigate to it -- for a chat_message hit, open its session with "
    "get_chat_session_messages_around(session_id, source_id) to land right on the match."
))
def search_tool(q: str, types: list[str] | None = None, limit: int = 20, offset: int = 0) -> dict:
    return storage.search(q, types, limit, offset)


async def index(request: Request) -> HTMLResponse:
    return HTMLResponse(_INDEX_HTML)


async def health(request: Request) -> JSONResponse:
    return JSONResponse({"status": "ok", "app": "collabhub"})


async def git_log(request: Request) -> Response:
    """Served alternative to running `git log` locally -- OpenClaw's WSL
    sandbox has no checkout to run it against at all (not just no access to
    THIS one), so 'run git log' isn't an option for it the way it is for
    Claude. This is the durable fix, not a one-off answer in chat (OpenClaw's
    review of AGENT_BRIEFING.md, 2026-08-14)."""
    try:
        result = subprocess.run(
            ["git", "log", "--oneline", "-30"],
            cwd=str(ROOT), capture_output=True, text=True, timeout=5, check=True,
        )
    except Exception as exc:
        return JSONResponse({"error": f"git log unavailable: {exc}"}, status_code=500)
    return Response(result.stdout, media_type="text/plain; charset=utf-8")


async def agent_briefing(request: Request) -> Response:
    """Serves AGENT_BRIEFING.md fresh from disk on every request (unlike
    _INDEX_HTML, not cached at import time) -- OpenClaw's WSL sandbox has no
    filesystem access to this Windows repo checkout, same reason /files/
    exists at all, so the doc needs to be reachable over the same network
    path as everything else it can already fetch. Read live rather than
    copied into shared_files/ so it can never drift out of sync with a git
    commit -- one source of truth, not two."""
    try:
        content = (ROOT / "AGENT_BRIEFING.md").read_text(encoding="utf-8")
    except FileNotFoundError:
        return JSONResponse({"error": "AGENT_BRIEFING.md not found on disk"}, status_code=404)
    return Response(content, media_type="text/markdown; charset=utf-8")


async def state_endpoint(request: Request) -> JSONResponse:
    return JSONResponse(storage.full_state())


# Plain HTTP mirrors of the chat MCP tools above, for lightweight polling scripts
# (chat_watch.py) that would rather not carry an MCP client just to poll every few
# seconds. Same storage functions underneath, so either side (MCP tool calls or
# these routes) sees a consistent session.
async def chat_active(request: Request) -> JSONResponse:
    return JSONResponse(storage.get_active_chat_session())


async def chat_start(request: Request) -> JSONResponse:
    body: dict[str, Any] = {}
    raw = await request.body()
    if raw:
        body = await request.json()
    try:
        return JSONResponse(storage.start_chat_session(body.get("started_by", ""), body.get("invite_task_id")))
    except storage.NotFoundError as exc:
        return JSONResponse({"error": str(exc)}, status_code=404)
    except ValueError as exc:
        return JSONResponse({"error": str(exc)}, status_code=400)


async def chat_end(request: Request) -> JSONResponse:
    body = await request.json()
    result = storage.end_chat_session(int(body["session_id"]))
    if result is None:
        return JSONResponse({"error": "session not found"}, status_code=404)
    return JSONResponse(result)


async def chat_send(request: Request) -> JSONResponse:
    body = await request.json()
    result = storage.send_chat_message(
        int(body["session_id"]), body.get("author", ""), body.get("text", "")
    )
    if result is None:
        return JSONResponse({"error": "session not active or not found"}, status_code=400)
    return JSONResponse(result)


async def chat_poll(request: Request) -> JSONResponse:
    session_id = int(request.query_params.get("session_id", 0))
    since = int(request.query_params.get("since", 0))
    return JSONResponse(storage.poll_chat_messages(session_id, since))


async def chat_sessions_list(request: Request) -> JSONResponse:
    """Tranche-2 history index -- cursor-paginated, active session returned
    separately as pinned_active (not folded into sessions -- OpenClaw's
    review: limit must stay a hard maximum). Superseded the old plain LIMIT
    listing (kept server-side as storage.list_chat_sessions for the legacy
    MCP tool only)."""
    cursor_param = request.query_params.get("cursor")
    cursor = int(cursor_param) if cursor_param is not None else None
    limit = int(request.query_params.get("limit", 20))
    try:
        return JSONResponse(storage.list_chat_sessions_paginated(cursor, limit))
    except ValueError as exc:
        return JSONResponse({"error": str(exc)}, status_code=400)


async def chat_session_detail(request: Request) -> JSONResponse:
    session_id = int(request.path_params["session_id"])
    result = storage.get_chat_session(session_id)
    if result is None:
        return JSONResponse({"error": "session not found"}, status_code=404)
    return JSONResponse(result)


async def chat_session_messages(request: Request) -> JSONResponse:
    session_id = int(request.path_params["session_id"])
    # after_id and before_id are mutually exclusive -- checked on RAW query
    # key presence, not on value, since after_id defaults to 0 and 0 is a
    # legitimate explicit value (OpenClaw's live black-box catch: passing
    # both was silently accepted, with before_id winning without warning).
    before_id_param = request.query_params.get("before_id")
    after_id_param = request.query_params.get("after_id")
    if before_id_param is not None and after_id_param is not None:
        return JSONResponse({"error": "after_id and before_id are mutually exclusive"}, status_code=400)
    try:
        limit = int(request.query_params.get("limit", 50))
        before_id = int(before_id_param) if before_id_param is not None else None
        after_id = int(after_id_param) if after_id_param is not None else 0
    except ValueError:
        return JSONResponse({"error": "limit/after_id/before_id must be integers"}, status_code=400)
    try:
        result = storage.get_chat_session_messages(session_id, after_id, limit, before_id)
    except ValueError as exc:
        return JSONResponse({"error": str(exc)}, status_code=400)
    if result is None:
        return JSONResponse({"error": "session not found"}, status_code=404)
    return JSONResponse(result)


async def chat_session_messages_around(request: Request) -> JSONResponse:
    session_id = int(request.path_params["session_id"])
    try:
        message_id = int(request.query_params["message_id"])
        before = int(request.query_params.get("before", 25))
        after = int(request.query_params.get("after", 25))
    except (KeyError, ValueError):
        return JSONResponse({"error": "message_id, before, and after must be integers"}, status_code=400)
    try:
        result = storage.get_chat_session_messages_around(session_id, message_id, before, after)
    except storage.NotFoundError as exc:
        return JSONResponse({"error": str(exc)}, status_code=404)
    except ValueError as exc:
        return JSONResponse({"error": str(exc)}, status_code=400)
    if result is None:
        return JSONResponse({"error": "session not found"}, status_code=404)
    return JSONResponse(result)


async def chat_session_export(request: Request) -> Response:
    session_id = int(request.path_params["session_id"])
    fmt = request.query_params.get("format", "json")
    if fmt not in ("json", "markdown"):
        return JSONResponse({"error": "format must be 'json' or 'markdown'"}, status_code=400)
    result = storage.export_chat_session(session_id, fmt)
    if result is None:
        return JSONResponse({"error": "session not found"}, status_code=404)
    return Response(
        result["body"], media_type=result["content_type"],
        headers={"Content-Disposition": f'attachment; filename="{result["filename"]}"'},
    )


async def task_link_session(request: Request) -> JSONResponse:
    task_id = int(request.path_params["task_id"])
    body = await request.json()
    try:
        return JSONResponse(storage.link_task_session(
            task_id, int(body["session_id"]), body.get("relation_type", ""), body.get("linked_by", "")
        ))
    except storage.NotFoundError as exc:
        return JSONResponse({"error": str(exc)}, status_code=404)
    except ValueError as exc:
        return JSONResponse({"error": str(exc)}, status_code=400)


async def task_unlink_session(request: Request) -> JSONResponse:
    task_id = int(request.path_params["task_id"])
    body = await request.json()
    try:
        removed = storage.unlink_task_session(task_id, int(body["session_id"]), body.get("relation_type", ""))
    except ValueError as exc:
        return JSONResponse({"error": str(exc)}, status_code=400)
    return JSONResponse({"removed": removed})


# ---------------------------------------------------------------------------
# REST mirrors for every task/note operation -- not just chat. Rationale (see
# chat comment above, same reasoning): an agent's MCP session can be stale
# post-restart (newly-added tools don't show up without a fresh session) but
# these plain routes always work, so nothing is unreachable while that's true.
# ---------------------------------------------------------------------------

async def tasks_list(request: Request) -> JSONResponse:
    return JSONResponse(storage.list_tasks(
        request.query_params.get("status"), request.query_params.get("assignee")
    ))


async def tasks_create(request: Request) -> JSONResponse:
    body = await request.json()
    try:
        task = storage.create_task(
            body.get("title", ""), body.get("description", ""), body.get("created_by", ""), body.get("tags"),
            body.get("priority", "normal"), body.get("due_date"),
        )
    except ValueError as exc:
        return JSONResponse({"error": str(exc)}, status_code=400)
    return JSONResponse(task)


async def task_get(request: Request) -> JSONResponse:
    task = storage.get_task(int(request.path_params["task_id"]))
    if task is None:
        return JSONResponse({"error": "task not found"}, status_code=404)
    return JSONResponse(task)


async def task_update(request: Request) -> JSONResponse:
    body = await request.json()
    # due_date is tri-state: key absent from the body -> leave unchanged;
    # key present with JSON null -> clear it; key present with a value ->
    # set it. This only works because REST bodies are plain dicts where
    # "absent" and "explicitly null" are naturally distinguishable -- the
    # MCP tool needs a different mechanism (see update_task_tool) since a
    # JSON-RPC/tool-schema call can't carry that same distinction as cleanly.
    # Only due_date has an explicit-null "clear" semantic. For every other
    # field, "leave unchanged" is expressed by omitting the key entirely --
    # an explicit JSON null is invalid input, not a no-op. Before this check,
    # {"priority": null} silently left priority untouched (storage.py's
    # `if priority is not None` gate treats None as "don't touch", matching
    # its OWN internal no-op convention) while still bumping updated_at,
    # emitting task_updated, and reindexing -- a validation failure with side
    # effects (OpenClaw's live-review catch). Reject before it ever reaches
    # storage.update_task.
    for key in ("status", "description", "assignee", "priority"):
        if key in body and body[key] is None:
            return JSONResponse(
                {"error": f"{key} cannot be null; omit the key entirely to leave it unchanged"},
                status_code=400,
            )
    kwargs: dict[str, Any] = {}
    for key in ("status", "description", "assignee", "priority"):
        if key in body:
            kwargs[key] = body[key]
    if "due_date" in body:
        kwargs["due_date"] = body["due_date"]
    if not kwargs:
        # An empty body (or one containing only unrecognized keys) has no
        # field to apply -- reject rather than silently bump updated_at and
        # emit a task_updated event for a change that never happened, which
        # would reorder the activity-sorted board for nothing (OpenClaw's
        # review).
        return JSONResponse({"error": "no fields to update"}, status_code=400)
    try:
        task = storage.update_task(int(request.path_params["task_id"]), **kwargs)
    except ValueError as exc:
        return JSONResponse({"error": str(exc)}, status_code=400)
    if task is None:
        return JSONResponse({"error": "task not found"}, status_code=404)
    return JSONResponse(task)


async def task_checklist_create(request: Request) -> JSONResponse:
    task_id = int(request.path_params["task_id"])
    body = await request.json()
    try:
        item = storage.add_checklist_item(task_id, body.get("text", ""))
    except storage.NotFoundError as exc:
        return JSONResponse({"error": str(exc)}, status_code=404)
    except ValueError as exc:
        return JSONResponse({"error": str(exc)}, status_code=400)
    return JSONResponse(item)


async def task_checklist_update(request: Request) -> JSONResponse:
    task_id = int(request.path_params["task_id"])
    item_id = int(request.path_params["item_id"])
    body = await request.json()
    try:
        item = storage.set_checklist_item_done(task_id, item_id, body.get("done"))
    except storage.NotFoundError as exc:
        return JSONResponse({"error": str(exc)}, status_code=404)
    except ValueError as exc:
        return JSONResponse({"error": str(exc)}, status_code=400)
    return JSONResponse(item)


async def task_checklist_delete(request: Request) -> JSONResponse:
    task_id = int(request.path_params["task_id"])
    item_id = int(request.path_params["item_id"])
    removed = storage.delete_checklist_item(task_id, item_id)
    return JSONResponse({"removed": removed})


# --- Attachments (tranche 5) -------------------------------------------
# This is the project's first open upload/delete surface. Everything before
# it was safe under "loopback-only, no auth, no network boundary worth
# gating" (see the module docstring) because nothing here could cause real
# harm if triggered from an unexpected origin. That's no longer true once a
# request can write or delete files -- a hostile webpage open in the same
# browser can reach 127.0.0.1 without any CORS opt-in on our side (a
# same-origin "simple" multipart POST needs no preflight), and DNS
# rebinding can make an attacker-controlled domain resolve to 127.0.0.1
# after the fact while still looking same-origin to the browser. This check
# is deliberately narrow: only the mutating attachment routes (upload,
# delete) call it, not the read-only ones, and it allows a request with NO
# Origin header at all through unconditionally (MCP clients, curl, this
# project's own Playwright tests, and any same-machine CLI tool never send
# one -- only a browser does) (OpenClaw's review).
_TRUSTED_HOSTNAMES = {"127.0.0.1", "localhost", "::1"}


def _check_trusted_origin(request: Request) -> JSONResponse | None:
    host = request.url.hostname
    if host not in _TRUSTED_HOSTNAMES:
        return JSONResponse({"error": "untrusted Host"}, status_code=403)
    origin = request.headers.get("origin")
    if origin:
        try:
            origin_host = urlparse(origin).hostname
        except ValueError:
            origin_host = None
        if origin_host not in _TRUSTED_HOSTNAMES:
            return JSONResponse({"error": "cross-origin request rejected"}, status_code=403)
    return None


_CONTROL_CHARS_RE = re.compile(r"[\x00-\x1f\x7f]")


def _sanitize_display_filename(filename: str, attachment_id: int) -> str:
    """Only used for the Content-Disposition download filename -- never for
    an actual filesystem path (that's always attachment.storage_name,
    resolved through storage.attachment_path()'s own grammar check).
    Starlette's FileResponse already percent-encodes anything outside a safe
    character set for the header itself (so this isn't the only thing
    standing between a malicious filename and header injection), but it
    doesn't cap length, normalize Unicode, or fall back on a degenerate
    name -- those are policy choices this function makes explicitly
    (OpenClaw's review)."""
    name = (filename or "").replace("\\", "/").rsplit("/", 1)[-1]
    name = unicodedata.normalize("NFC", name)
    name = _CONTROL_CHARS_RE.sub("", name).strip()
    name = name[: storage.ATTACHMENT_FILENAME_MAX_LEN]
    if not name or name in (".", ".."):
        name = f"attachment-{attachment_id}"
    return name


async def task_attachments_list(request: Request) -> JSONResponse:
    task_id = int(request.path_params["task_id"])
    return JSONResponse(storage.list_attachments(task_id))


async def _write_upload_chunks(upload, part_path: Path) -> tuple[int, str]:
    """Streams `upload` into `part_path` in fixed 64KB chunks, hashing as it
    goes -- `total` is a running count of actual bytes written, never a
    client-declared request header. Raises ValueError for a size-limit/empty-file violation
    (the caller maps that to 400); a genuine disk/write failure raises
    OSError and propagates as-is. Pulled into its own function specifically
    so it's independently mockable in tests -- proving a mid-stream disk
    failure (e.g. ENOSPC) cleans up correctly needs to inject a failure
    partway through a real write, which an end-to-end assertion can't do
    cleanly against the inline version this used to be (OpenClaw's
    review)."""
    hasher = hashlib.sha256()
    total = 0
    with open(part_path, "xb") as f:
        while True:
            chunk = await upload.read(64 * 1024)
            if not chunk:
                break
            total += len(chunk)
            if total > storage.MAX_ATTACHMENT_BYTES:
                raise ValueError(f"attachment exceeds the {storage.MAX_ATTACHMENT_BYTES}-byte per-file limit")
            hasher.update(chunk)
            f.write(chunk)
    if total == 0:
        raise ValueError("uploaded file is empty")
    return total, hasher.hexdigest()


async def task_attachment_upload(request: Request) -> JSONResponse:
    task_id = int(request.path_params["task_id"])
    origin_error = _check_trusted_origin(request)
    if origin_error:
        return origin_error

    try:
        form = await request.form()
    except Exception:
        return JSONResponse({"error": "could not parse multipart form data"}, status_code=400)

    file_values = form.getlist("file")
    if len(file_values) != 1:
        return JSONResponse({"error": "exactly one 'file' field is required"}, status_code=400)
    upload = file_values[0]
    if not hasattr(upload, "read"):
        # A plain text field named "file" (no filename) comes through as a
        # bare str, not an UploadFile -- reject rather than crash on .read().
        return JSONResponse({"error": "'file' field must be an uploaded file"}, status_code=400)

    original_filename = (upload.filename or "").strip()
    if not original_filename:
        await upload.close()
        return JSONResponse({"error": "uploaded file must have a filename"}, status_code=400)

    # A claimed label, not authentication -- same treatment as every other
    # author/uploaded_by field in this codebase: trim and cap, never trust.
    uploaded_by = (form.get("uploaded_by") or "").strip()[:100]

    storage.ATTACHMENT_DIR.mkdir(parents=True, exist_ok=True)
    storage_name = storage.generate_storage_name()
    part_path = storage.ATTACHMENT_DIR / f"{storage_name}.part"
    final_path = storage.attachment_path(storage_name)

    total = 0
    digest = ""
    write_error: tuple[int, str] | None = None
    try:
        # 'xb' = exclusive create -- refuses to open if the (randomly named,
        # so collision is already astronomically unlikely) path already
        # exists, rather than silently overwriting something. Written under
        # a `.part` name and only atomically renamed to its final name once
        # the full transfer is validated, so nothing can ever observe a
        # partially-written file under a name attachment_path() would
        # resolve to.
        try:
            total, digest = await _write_upload_chunks(upload, part_path)
        except ValueError as exc:
            write_error = (400, str(exc))
        if write_error is None:
            # os.replace()/os.rename() both silently OVERWRITE an existing
            # destination on POSIX and Windows alike -- os.link() is the
            # portable primitive that instead FAILS if the destination
            # already exists, giving a true exclusive finalize. In normal
            # operation storage_name is a fresh 128-bit random token and a
            # collision is cryptographically implausible, but a plain
            # os.replace() here would have silently clobbered an already-
            # finalized file's bytes on the (however unlikely) collision
            # path instead of failing loudly -- caught by a dedicated test
            # forcing a collision via monkeypatching, not just reasoned
            # about (OpenClaw's review asked for token-collision evidence
            # explicitly, and this is exactly what it found).
            try:
                os.link(part_path, final_path)
            except FileExistsError:
                # 409 Conflict, not a generic 500 -- this is a well-understood,
                # bounded condition (the storage allocation collided), not an
                # unexpected server fault, and the client's own natural
                # response (retry) is exactly what 409 signals (OpenClaw's
                # review).
                write_error = (409, "storage allocation conflict -- please retry")
            else:
                part_path.unlink(missing_ok=True)
    except OSError:
        write_error = (500, "upload failed while writing to disk")
    finally:
        await upload.close()

    if write_error is not None:
        # Only ever unlink part_path here, never final_path -- final_path
        # is untouched in every failure case EXCEPT the collision path
        # above, where it already belongs to a different, already-
        # successful upload and must be left alone.
        part_path.unlink(missing_ok=True)
        status, message = write_error
        return JSONResponse({"error": message}, status_code=status)

    try:
        item = storage.create_attachment(
            task_id, original_filename, upload.content_type or "application/octet-stream",
            total, digest, storage_name, uploaded_by,
        )
    except storage.NotFoundError as exc:
        final_path.unlink(missing_ok=True)
        return JSONResponse({"error": str(exc)}, status_code=404)
    except ValueError as exc:
        final_path.unlink(missing_ok=True)
        return JSONResponse({"error": str(exc)}, status_code=400)
    except Exception:
        # ANY other failure from create_attachment (a DB-layer exception we
        # didn't anticipate, e.g. an event-emit failure inside its
        # transaction) must still clean up the just-finalized file before
        # propagating -- the two explicit except clauses above originally
        # left this bare, so an unanticipated exception type would leave an
        # orphaned file with no cleanup at all. Re-raised so Starlette's
        # normal 500 handling still applies; only the cleanup is added here
        # (OpenClaw's review: inject an event-write failure and prove
        # rollback + cleanup, not just the two happy-path error types).
        final_path.unlink(missing_ok=True)
        raise
    return JSONResponse(item, status_code=201)


async def task_attachment_download(request: Request) -> Response:
    task_id = int(request.path_params["task_id"])
    attachment_id = int(request.path_params["attachment_id"])
    row = storage.get_attachment(attachment_id)
    if not row or row["task_id"] != task_id:
        return JSONResponse({"error": "attachment not found"}, status_code=404)
    try:
        path = storage.attachment_path(row["storage_name"])
    except ValueError:
        return JSONResponse({"error": "attachment storage reference is invalid"}, status_code=500)
    if not path.exists():
        # A row whose bytes are missing (e.g. a lost unlink race, or manual
        # tampering) is a deliberate signal, not an arbitrary fallback --
        # 410 Gone, and reconcile_attachment_storage() will surface it on
        # the next startup too.
        return JSONResponse({"error": "attachment bytes are missing"}, status_code=410)

    display_name = _sanitize_display_filename(row["filename"], attachment_id)
    return FileResponse(
        path,
        media_type="application/octet-stream",  # never the claimed MIME, regardless of what it was (OpenClaw's review)
        filename=display_name,
        headers={
            "X-Content-Type-Options": "nosniff",
            "Cache-Control": "private, no-store",
            "Cross-Origin-Resource-Policy": "same-origin",
        },
    )


async def task_attachment_delete(request: Request) -> JSONResponse:
    task_id = int(request.path_params["task_id"])
    attachment_id = int(request.path_params["attachment_id"])
    origin_error = _check_trusted_origin(request)
    if origin_error:
        return origin_error
    result = storage.delete_attachment(task_id, attachment_id)
    if result["removed"] and result["storage_name"]:
        try:
            storage.attachment_path(result["storage_name"]).unlink(missing_ok=True)
        except (ValueError, OSError):
            pass  # DB delete already succeeded; a failed unlink is a recorded orphan, not a reason to fail this request
    return JSONResponse(result)


async def task_claim(request: Request) -> JSONResponse:
    body = await request.json()
    task = storage.update_task(int(request.path_params["task_id"]), status="in_progress", assignee=body.get("assignee", ""))
    if task is None:
        return JSONResponse({"error": "task not found"}, status_code=404)
    return JSONResponse(task)


async def task_comment(request: Request) -> JSONResponse:
    body = await request.json()
    comment = storage.add_comment(int(request.path_params["task_id"]), body.get("author", ""), body.get("text", ""))
    if comment is None:
        return JSONResponse({"error": "task not found"}, status_code=404)
    return JSONResponse(comment)


async def task_complete(request: Request) -> JSONResponse:
    body = await request.json()
    task_id = int(request.path_params["task_id"])
    if body.get("summary"):
        storage.add_comment(task_id, body.get("author", ""), body["summary"])
    task = storage.update_task(task_id, status="done")
    if task is None:
        return JSONResponse({"error": "task not found"}, status_code=404)
    return JSONResponse(task)


async def notes_list(request: Request) -> JSONResponse:
    return JSONResponse(storage.list_notes(int(request.query_params.get("limit", 50))))


async def note_get(request: Request) -> JSONResponse:
    note = storage.get_note(int(request.path_params["note_id"]))
    if note is None:
        return JSONResponse({"error": "note not found"}, status_code=404)
    return JSONResponse(note)


async def notes_post(request: Request) -> JSONResponse:
    body = await request.json()
    return JSONResponse(storage.post_note(body.get("author", ""), body.get("text", "")))


async def catch_up_endpoint(request: Request) -> JSONResponse:
    if request.method == "POST":
        body = await request.json()
    else:
        body = dict(request.query_params)
    agent = body.get("agent", "")
    after_cursor = body.get("after_cursor")
    try:
        return JSONResponse(storage.catch_up(agent, int(after_cursor) if after_cursor is not None else None))
    except ValueError as exc:
        return JSONResponse({"error": str(exc)}, status_code=400)


async def ack_events_endpoint(request: Request) -> JSONResponse:
    body = await request.json()
    try:
        return JSONResponse(storage.ack_events(body.get("agent", ""), int(body.get("cursor", 0))))
    except ValueError as exc:
        return JSONResponse({"error": str(exc)}, status_code=400)


async def presence_list(request: Request) -> JSONResponse:
    return JSONResponse(storage.list_presence())


async def search_endpoint(request: Request) -> JSONResponse:
    q = request.query_params.get("q", "")
    types_param = request.query_params.get("types")
    types = [t for t in types_param.split(",") if t] if types_param else None
    limit = int(request.query_params.get("limit", 20))
    offset = int(request.query_params.get("offset", 0))
    try:
        return JSONResponse(storage.search(q, types, limit, offset))
    except ValueError as exc:
        return JSONResponse({"error": str(exc)}, status_code=400)


def _ts() -> str:
    return datetime.now(timezone.utc).strftime("%H:%M:%S.%f")[:-3]


async def events_stream(request: Request) -> StreamingResponse:
    """Server-Sent Events tap on the same event log the WebSocket route and
    wait_for_events serve -- for a client that wants push but can't/won't hold
    a raw WebSocket open. One-directional, auto-reconnecting per the SSE spec.

    Frames carry `id:`/`event:` (not just `data:`) and a reconnect honors the
    browser-sent `Last-Event-ID` header, so a plain `<EventSource>` can resume
    on its own after a dropped connection -- not just clients that manage an
    explicit ?since= cursor themselves (OpenClaw's finding, 2026-08-14)."""
    last_event_id_header = request.headers.get("last-event-id")
    if last_event_id_header is not None:
        since = int(last_event_id_header)
    else:
        since = int(request.query_params.get("since", storage.latest_event_id()))
    client = request.client.host if request.client else "?"
    conn_id = id(request)
    t0 = time.monotonic()
    print(f"[sse {conn_id}] {_ts()} connect from={client} since={since} (Last-Event-ID={last_event_id_header})", flush=True)

    async def gen():
        nonlocal since
        yield b": connected\n\n"
        tick = 0
        try:
            while True:
                disconnected = await request.is_disconnected()
                print(f"[sse {conn_id}] {_ts()} tick={tick} disconnected={disconnected} since={since}", flush=True)
                if disconnected:
                    break
                events = storage.get_events_since(since)
                if events:
                    print(f"[sse {conn_id}] {_ts()} tick={tick} found ids={[e['id'] for e in events]}", flush=True)
                for event in events:
                    since = event["id"]
                    frame = f"id: {event['id']}\nevent: {event['type']}\ndata: {json.dumps(event)}\n\n"
                    yield frame.encode("utf-8")
                yield b": ping\n\n"
                tick += 1
                await asyncio.sleep(1.0)
        except BaseException as exc:
            print(f"[sse {conn_id}] {_ts()} EXCEPTION in generator loop: {type(exc).__name__}: {exc}", flush=True)
            raise
        finally:
            print(f"[sse {conn_id}] {_ts()} generator exiting, final since={since}, elapsed={time.monotonic()-t0:.2f}s, ticks={tick}", flush=True)

    return StreamingResponse(gen(), media_type="text/event-stream", headers={"Cache-Control": "no-cache"})


async def events_ws(websocket: WebSocket) -> None:
    """WebSocket tap on the event log -- built for Claude Code's Monitor tool,
    which watches a ws:// URL natively and turns each text frame into a
    notification (no polling script needed on that side)."""
    await websocket.accept()
    since = int(websocket.query_params.get("since", storage.latest_event_id()))
    try:
        while True:
            for event in storage.get_events_since(since):
                since = event["id"]
                await websocket.send_text(json.dumps(event))
            await asyncio.sleep(1.0)
    except WebSocketDisconnect:
        pass


def build_app() -> Starlette:
    mcp_app = mcp.streamable_http_app()
    app = Starlette(
        routes=[
            Route("/", index),
            Route("/health", health),
            Route("/AGENT_BRIEFING.md", agent_briefing),
            Route("/api/git-log", git_log),
            Route("/api/state", state_endpoint),
            Route("/api/chat/active", chat_active),
            Route("/api/chat/start", chat_start, methods=["POST"]),
            Route("/api/chat/end", chat_end, methods=["POST"]),
            Route("/api/chat/send", chat_send, methods=["POST"]),
            Route("/api/chat/poll", chat_poll),
            Route("/api/chat/sessions", chat_sessions_list),
            Route("/api/chat/sessions/{session_id:int}/messages", chat_session_messages),
            Route("/api/chat/sessions/{session_id:int}/messages/around", chat_session_messages_around),
            Route("/api/chat/sessions/{session_id:int}/export", chat_session_export),
            Route("/api/chat/sessions/{session_id:int}", chat_session_detail),
            Route("/api/tasks", tasks_list),
            Route("/api/tasks", tasks_create, methods=["POST"]),
            Route("/api/tasks/{task_id:int}", task_get),
            Route("/api/tasks/{task_id:int}", task_update, methods=["PATCH", "POST"]),
            Route("/api/tasks/{task_id:int}/claim", task_claim, methods=["POST"]),
            Route("/api/tasks/{task_id:int}/comment", task_comment, methods=["POST"]),
            Route("/api/tasks/{task_id:int}/complete", task_complete, methods=["POST"]),
            Route("/api/tasks/{task_id:int}/checklist", task_checklist_create, methods=["POST"]),
            Route("/api/tasks/{task_id:int}/checklist/{item_id:int}", task_checklist_update, methods=["PATCH"]),
            Route("/api/tasks/{task_id:int}/checklist/{item_id:int}", task_checklist_delete, methods=["DELETE"]),
            Route("/api/tasks/{task_id:int}/attachments", task_attachments_list),
            Route("/api/tasks/{task_id:int}/attachments", task_attachment_upload, methods=["POST"]),
            Route("/api/tasks/{task_id:int}/attachments/{attachment_id:int}/download", task_attachment_download),
            Route("/api/tasks/{task_id:int}/attachments/{attachment_id:int}", task_attachment_delete, methods=["DELETE"]),
            Route("/api/tasks/{task_id:int}/link", task_link_session, methods=["POST"]),
            Route("/api/tasks/{task_id:int}/unlink", task_unlink_session, methods=["POST"]),
            Route("/api/notes", notes_list),
            Route("/api/notes", notes_post, methods=["POST"]),
            Route("/api/notes/{note_id:int}", note_get),
            Route("/api/catchup", catch_up_endpoint, methods=["GET", "POST"]),
            Route("/api/ack", ack_events_endpoint, methods=["POST"]),
            Route("/api/presence", presence_list),
            Route("/api/search", search_endpoint),
            Route("/api/events/stream", events_stream),
            WebSocketRoute("/ws/events", events_ws),
        ],
        # FastMCP's streamable_http_app() carries its own lifespan (starts the
        # session manager's task group); mounting it below does NOT run that
        # lifespan automatically, so it must be forwarded here.
        lifespan=mcp_app.router.lifespan_context,
    )
    # Static file handoff: drop files under shared_files/<subdir>/ to make them
    # fetchable by either agent over the same network path as the MCP calls
    # (important for OpenClaw specifically -- its WSL sandbox has no filesystem
    # access to the Windows drive, so a bare C:\ path is useless to it; this
    # exposes just the opted-in files, not the whole project tree).
    shared_dir = ROOT / "shared_files"
    shared_dir.mkdir(exist_ok=True)
    app.mount("/files", StaticFiles(directory=str(shared_dir)), name="files")
    # FastMCP's app serves its endpoint at "/mcp" internally by default, so
    # it's mounted at root here (mounting at "/mcp" too would double the prefix).
    app.mount("/", mcp_app)
    return app


app = build_app()


def main() -> None:
    port = int(os.environ.get("PORT", 8765))
    uvicorn.run(app, host="127.0.0.1", port=port, workers=1)


if __name__ == "__main__":
    main()
