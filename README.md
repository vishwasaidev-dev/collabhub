# CollabHub

A tiny local MCP server: a shared task board + notes log so Claude Code and OpenClaw
(or any other MCP-speaking agent) can hand off and coordinate work on the same
machine — not tied to any one project.

- **Dashboard:** http://127.0.0.1:8765/ (open directly in a browser, no auth)
- **MCP endpoint:** http://127.0.0.1:8765/mcp
- **State as JSON:** http://127.0.0.1:8765/api/state
- **Agent picking this up cold?** Read [`AGENT_BRIEFING.md`](AGENT_BRIEFING.md) first
  (or `GET http://127.0.0.1:8765/AGENT_BRIEFING.md` if you have no filesystem access
  to this checkout).

## What it is

Starlette + FastMCP server, SQLite-backed. 10 MCP tools: `create_task`, `list_tasks`,
`get_task`, `claim_task`, `update_task`, `comment_task`, `complete_task`, `post_note`,
`list_notes`, `list_attachments`. Plus a `/files/<subdir>/...` static route (backed by
`shared_files/`, gitignored — populate it locally) for handing an agent an actual file,
not just a path it might not be able to reach (e.g. OpenClaw running in a WSL distro
with no filesystem access to the Windows drive).

Loopback-only (127.0.0.1), intentionally no auth — nothing but this machine (and,
via WSL2 mirrored networking, this machine's WSL distros) can reach it.

## File attachments

Tasks can have file attachments (upload/list/download/delete), added via the
dashboard or the REST routes under `/api/tasks/{id}/attachments`. A few things
worth knowing before you touch this data outside the app:

- Attachment bytes live in a gitignored `attachments/` directory (sibling to
  `shared_files/`), named with opaque server-generated tokens — never the
  original filename. **The SQLite DB and `attachments/` are one logical backup
  unit.** Back them up together; a DB-only backup will restore rows pointing at
  missing files, and an `attachments/`-only backup is just anonymous blobs with
  no metadata.
- Attachment bytes are **not recoverable from git** (the directory is
  gitignored, same as `shared_files/`) — only from a real file-level backup of
  this machine. If the machine or disk is lost, any attachments uploaded since
  the last such backup are gone for good.
- Default limits (env-configurable — see `MAX_ATTACHMENT_BYTES`,
  `MAX_ATTACHMENTS_PER_TASK`, `MAX_TOTAL_ATTACHMENT_BYTES` in `storage.py`):
  10 MiB per file, 20 attachments per task, 1 GiB total across all attachments.
- Downloads are always served as `application/octet-stream` with a forced
  `Content-Disposition: attachment`, regardless of the file's real type — this
  is the actual anti-XSS mitigation (no MIME allowlist needed), so an uploaded
  HTML/SVG file can never execute same-origin JS by being rendered inline.

See [`AGENT_BRIEFING.md`](AGENT_BRIEFING.md) for the full design rationale
(path-traversal model, atomic writes, trusted-Origin guard, startup
reconciliation).

## Setup on a fresh machine

```powershell
cd collabhub
python -m venv .venv
./.venv/Scripts/pip install -r requirements.txt
./.venv/Scripts/python app.py          # foreground, Ctrl+C to stop
# or, to background it with a log file and a "don't double-launch" guard:
powershell -NoProfile -ExecutionPolicy Bypass -File start_collabhub.ps1
```

Then confirm it's up: `curl http://127.0.0.1:8765/health` → `{"status":"ok","app":"collabhub"}`.

### Auto-start at login (Windows Scheduled Task)

Not part of this repo (Scheduled Tasks aren't files) — recreate it with:

```powershell
$action = New-ScheduledTaskAction -Execute 'powershell.exe' `
  -Argument '-NoProfile -ExecutionPolicy Bypass -WindowStyle Hidden -File "<absolute path to>\start_collabhub.ps1"'
$trigger = New-ScheduledTaskTrigger -AtLogOn -User $env:USERNAME
$settings = New-ScheduledTaskSettingsSet -AllowStartIfOnBatteries -DontStopIfGoingOnBatteries -StartWhenAvailable
Register-ScheduledTask -TaskName 'CollabHub Autostart' -Action $action -Trigger $trigger -Settings $settings -Force
```

### Wiring into Claude Code

Add to the project's `.mcp.json`:
```json
"collabhub": { "type": "http", "url": "http://127.0.0.1:8765/mcp" }
```
(Needs a fresh Claude Code session to pick up — doesn't hot-reload into a running one.)

### Wiring into OpenClaw (or anything else with an MCP CLI)

```bash
openclaw mcp add collabhub --url http://127.0.0.1:8765/mcp --transport streamable-http --parallel
openclaw mcp probe collabhub   # should report 10 tools
```

### Cross-OS reachability (Windows host + WSL2 agent)

If the other agent runs inside WSL2, it needs to actually reach `127.0.0.1:8765` on
the Windows side. The reliable fix is WSL2 **mirrored networking** — add to
`C:\Users\<you>\.wslconfig`:
```ini
[wsl2]
networkingMode=mirrored
```
then `wsl --shutdown` and restart the distro. After that, WSL's own `127.0.0.1`
transparently reaches the Windows host's loopback (and vice versa) — no firewall
rule or rebind to `0.0.0.0` needed. Requires Windows 11 + a reasonably recent WSL
(`wsl --version` — mirrored mode landed well before WSL 2.x current releases).
