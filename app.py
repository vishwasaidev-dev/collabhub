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
import json
import os
import time
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

import uvicorn
from mcp.server.fastmcp import FastMCP
from mcp.server.transport_security import TransportSecuritySettings
from starlette.applications import Starlette
from starlette.requests import Request
from starlette.responses import HTMLResponse, JSONResponse, Response, StreamingResponse
from starlette.routing import Route, WebSocketRoute
from starlette.staticfiles import StaticFiles
from starlette.websockets import WebSocket, WebSocketDisconnect

import storage

ROOT = Path(__file__).resolve().parent
_INDEX_HTML = (ROOT / "templates" / "index.html").read_text(encoding="utf-8")

storage.init_db()

mcp = FastMCP(
    "collabhub",
    transport_security=TransportSecuritySettings(enable_dns_rebinding_protection=False),
)


@mcp.tool(name="create_task", description=(
    "Create a new shared task on the board. Use this to hand work to the other agent, "
    "or to log work you're about to start so it's visible. Returns the created task."
))
def create_task_tool(title: str, description: str = "", created_by: str = "", tags: list[str] | None = None) -> dict:
    return storage.create_task(title, description, created_by, tags)


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
    "Update a task's status (open/claimed/in_progress/blocked/done) and/or description. "
    "Omit fields you don't want to change. Raises on an invalid status instead of silently "
    "corrupting the task."
))
def update_task_tool(task_id: int, status: str | None = None, description: str | None = None) -> dict | None:
    return storage.update_task(task_id, status=status, description=description)


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
    task = storage.create_task(
        body.get("title", ""), body.get("description", ""), body.get("created_by", ""), body.get("tags")
    )
    return JSONResponse(task)


async def task_get(request: Request) -> JSONResponse:
    task = storage.get_task(int(request.path_params["task_id"]))
    if task is None:
        return JSONResponse({"error": "task not found"}, status_code=404)
    return JSONResponse(task)


async def task_update(request: Request) -> JSONResponse:
    body = await request.json()
    try:
        task = storage.update_task(
            int(request.path_params["task_id"]),
            status=body.get("status"), description=body.get("description"), assignee=body.get("assignee"),
        )
    except ValueError as exc:
        return JSONResponse({"error": str(exc)}, status_code=400)
    if task is None:
        return JSONResponse({"error": "task not found"}, status_code=404)
    return JSONResponse(task)


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
