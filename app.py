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

import os
from pathlib import Path
from typing import Any

import uvicorn
from mcp.server.fastmcp import FastMCP
from mcp.server.transport_security import TransportSecuritySettings
from starlette.applications import Starlette
from starlette.requests import Request
from starlette.responses import HTMLResponse, JSONResponse
from starlette.routing import Route
from starlette.staticfiles import StaticFiles

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
    "Omit fields you don't want to change."
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


async def index(request: Request) -> HTMLResponse:
    return HTMLResponse(_INDEX_HTML)


async def health(request: Request) -> JSONResponse:
    return JSONResponse({"status": "ok", "app": "collabhub"})


async def state_endpoint(request: Request) -> JSONResponse:
    return JSONResponse(storage.full_state())


def build_app() -> Starlette:
    mcp_app = mcp.streamable_http_app()
    app = Starlette(
        routes=[
            Route("/", index),
            Route("/health", health),
            Route("/api/state", state_endpoint),
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
