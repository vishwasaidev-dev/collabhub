# CollabHub

A tiny local MCP server: a shared task board + notes log so Claude Code and OpenClaw
(or any other MCP-speaking agent) can hand off and coordinate work on the same
machine — not tied to any one project.

- **Dashboard:** http://127.0.0.1:8765/ (open directly in a browser, no auth)
- **MCP endpoint:** http://127.0.0.1:8765/mcp
- **State as JSON:** http://127.0.0.1:8765/api/state

## What it is

Starlette + FastMCP server, SQLite-backed. 9 MCP tools: `create_task`, `list_tasks`,
`get_task`, `claim_task`, `update_task`, `comment_task`, `complete_task`, `post_note`,
`list_notes`. Plus a `/files/<subdir>/...` static route (backed by `shared_files/`,
gitignored — populate it locally) for handing an agent an actual file, not just a path
it might not be able to reach (e.g. OpenClaw running in a WSL distro with no
filesystem access to the Windows drive).

Loopback-only (127.0.0.1), intentionally no auth — nothing but this machine (and,
via WSL2 mirrored networking, this machine's WSL distros) can reach it.

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
openclaw mcp probe collabhub   # should report 9 tools
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
