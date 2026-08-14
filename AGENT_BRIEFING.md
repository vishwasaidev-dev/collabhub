# Agent briefing — read this first if you're picking up CollabHub cold

This file exists because OpenClaw (one of the two agents working on this project)
doesn't carry conversation memory across its own updates/restarts the way Claude
Code does. If you're an agent (either one) and you don't remember the state of this
project, **read this file top to bottom before doing anything else.** It's kept
up to date at the end of every tranche — if it looks stale (check the "Last updated"
line against `git log -1`), trust `git log` and the task board over this file's prose.

**Last updated:** 2026-08-14, after tranche 4 Part A shipped (commit `8e32733`, awaiting review).

**Fetching this file:** if you can `git clone`/read this repo's working tree
directly, just open it. If you're OpenClaw specifically — your WSL sandbox has no
filesystem access to this Windows checkout (same reason `/files/` exists) — fetch
it live instead: `GET http://127.0.0.1:8765/AGENT_BRIEFING.md`. That route reads
straight from disk on every request, so it's always current with whatever's
actually committed.

## What this is

CollabHub is a shared task board + notes + live chat + event-log MCP server that
Claude Code and OpenClaw use to coordinate work with each other on this machine.
Loopback-only (`127.0.0.1:8765`), no auth. Dashboard at `http://127.0.0.1:8765/`.
Both MCP tools and REST routes exist for everything — REST is the fallback when
an MCP client's tool list is stale (see "known gotchas" below).

## The pattern this project runs on

1. **Claude implements** (code lives on the Windows side; OpenClaw's WSL sandbox
   has no filesystem access to it).
2. **Before writing code for anything non-trivial, Claude posts a design contract**
   as a task on the board and asks OpenClaw to review it *before* implementation
   starts.
3. **After code ships, OpenClaw reviews the actual served code/behavior** — not
   just the design. This has caught real bugs every single time it's been done:
   several review rounds on tranche 1 (see task #8's comment thread for the exact
   sequence — don't trust a specific number here without checking it, this file
   has already had one wrong), 13 issues across 3 rounds on tranche 2, 10 issues
   across 2 rounds on tranche 3. **Multiple review rounds finding new issues each
   time is the normal, expected shape of this process — it is not a sign anything
   is going wrong.** Do not shortcut it, and do not be surprised when a "final"
   fix itself has a bug the next review pass catches (this happened more than
   once — see tranche 1's recovery-barrier saga in the git log for the canonical
   example).
4. Every review round happens **in chat session #4** (see below) and gets
   summarized as comments on the relevant tracking task (#8, #17, #20, ...).
5. When a tranche is approved, the tracking task is marked `done` and this file
   gets updated.

## How to re-orient fast

If you (OpenClaw) have lost context on where things stand, **you cannot run
`git log` locally** — your sandbox has no checkout of this repo at all, not just
no access to this particular one. Use the OpenClaw-specific alternatives first:

1. **`GET http://127.0.0.1:8765/api/git-log`** — served commit history (`git log
   --oneline -30`, run server-side, returned as plain text), specifically so you
   don't need local git access to get it. This is the richest source of truth on
   *why* things are the way they are — every commit message is deliberately
   long-form and explains what review finding it was responding to. If this route
   is ever unreachable, ask Claude to paste recent `git log` output directly
   rather than guessing.
   (`openclaw mcp probe collabhub` may refresh MCP tool visibility, but it isn't
   reliable in every environment — REST is the dependable fallback, not probe.)
   If you DO have local checkout access (Claude, or a future OpenClaw setup that
   changes this), plain `git log --oneline` works too and is equivalent.
2. **Check chat session #4** — `get_active_chat_session()` should show it's still
   active. `poll_chat_messages(4, since=0)` (or since your last known message id)
   gets the full transcript. This is where design contracts get negotiated and
   review findings get reported in real time.
3. **Read the tracking tasks' comment threads**: task `#8` (tranche 1, dashboard),
   `#17` (tranche 2, chat history/linkage), `#20` (tranche 3, search). Each
   comment thread is the full back-and-forth for that tranche — proposal, review
   round(s), fixes, re-review, approval. `get_task(8|17|20)`.
4. **`list_tasks()`** for current board state. As of this writing, tasks `#1-5`,
   `#8`, `#17`, `#20` are real; there is no other live work in progress. (Earlier
   task ids in that range that no longer exist, e.g. `#6,7,9-16,18,19,21-23`,
   were Playwright acceptance-test artifacts that accumulated in the live DB
   across tranches and were deleted once tranche 3's search made them visibly
   polluting real results — see the `dc8e63f` commit message for exact IDs and
   reasoning. If you're about to run acceptance tests that create real rows in
   this DB, prefer a disposable DB or a clear test-marker convention so a future
   cleanup pass doesn't need another manual inventory.)
5. **This file** for the high-level shape of what's shipped and what's pending.

## What's shipped (all on GitHub `main`, `vishwasaidev-dev/collabhub`)

| Phase/tranche | What | Status | Key commits |
|---|---|---|---|
| Phase 1 | Event log (`events` table), non-destructive `catch_up`/`ack_events` cursor model, `wait_for_events` long-poll, SSE (`/api/events/stream`) + WebSocket (`/ws/events`) push, REST parity for every MCP tool | Shipped, reviewed live in chat (predates task-based tracking) | `c52100b` |
| Tranche 1 | Live dashboard driven by the SSE feed (Live/Reconnecting/Polling indicator, presence panel, "N new" badges) | **APPROVED** (task #8, 5 review passes) | `9a9c15c` → `cd00624` |
| Tranche 2 | Browsable chat history, deep-linkable transcript drawer (`?session=N`), many-to-many task↔chat-session linkage | **APPROVED** (task #17, 3 rounds / 13 issues) | `56790c8` → `f19c587` |
| Tranche 3 | Full-text search (FTS5) across tasks/comments/notes/chat, around-message navigation (`?session=N&message=M`) | **APPROVED** (task #20, 2 rounds / 10 issues) | `dc8e63f` |
| Tranche 4 Part A | Task priority/due-date/checklist (schema, tri-state due_date, checklist CRUD, inline checklist UI) | Shipped, **awaiting OpenClaw review** (task #24) | `8e32733` |

## What's still open

- **Tranche 4 Part B**: user-requested UI feature (2026-08-14, no rush) — make
  dashboard sections collapsible: board columns and the sidebar panels
  (Presence/Shared notes/Chat history/Live chat), state persisted client-side
  across reloads. Contract already reviewed (task #24 comment 52); not yet
  implemented as of this writing.
- **Tranche 5**: file attachments on tasks/comments. Deliberately last — real
  security-surface questions (path traversal, size/MIME limits, storage location)
  that deserve their own careful design pass, not a bolt-on.

## Known gotchas — don't waste a review cycle rediscovering these

- **MCP tool discovery doesn't hot-reload.** A client's tool list is fetched once
  at session start; tools added to the server after that are invisible until the
  client reconnects/refreshes. This is a known, accepted limitation, not a server
  bug — every MCP tool has a REST equivalent specifically so this never blocks
  either agent. `openclaw mcp probe collabhub` *might* refresh visibility, but
  it's not reliable in every environment (confirmed: returned no output at least
  once) — treat **REST as the dependable fallback**, probe as a maybe-it-helps,
  not the other way around.
- **Canonical multi-table/index mutations share one SQLite transaction.** Any DB
  write that touches more than one table (or the search index) must happen in one
  `with _connect() as conn:` block, reusing the same `conn`. Two separate
  connections/transactions for what's logically one operation is exactly the bug
  class behind tranche 2's atomic-invite-link requirement and tranche 3's
  same-transaction search-index sync. If you're adding a new mutation, grep the
  existing ones (`create_task`, `start_chat_session`, `_index_for_search`'s
  caller sites) for the pattern before writing a new one.
- **Client-side event cursors only advance after successful projection or a
  completed full resync — never on "an attempt was made."** This is a *browser
  JS* ordering bug, not a DB transaction bug (don't conflate it with the gotcha
  above) — the dashboard's `lastAppliedEventId`/`recoveryRequired` machinery
  exists because an early version advanced the cursor as soon as it *tried* to
  apply an event, before confirming that actually succeeded, silently dropping
  events on failure. This is tranche 1's recovery-barrier saga (4 review rounds
  on one ~15-line function before it was actually correct) and is worth reading
  in full in the git log before writing any similar retry/recovery logic.
- **Snippet/highlight output from search is never raw HTML.** `snippet()` uses
  non-printable sentinel delimiters (`\x01`/`\x02`), not literal tags — the
  client escapes the whole string first, then swaps sentinels for `<mark>`.
  Indexed content also has those sentinel characters stripped at write time.
  Don't reintroduce a path where snippet output reaches `innerHTML` unescaped.
- **A "fix" for an async-ordering/recovery bug needs to be tested against the
  actual failure**, not just re-read. Every recovery-barrier-style bug in this
  project's history was caught by literally blocking the relevant network
  request with Playwright route interception and observing the real behavior,
  not by reasoning about the code. Do the same when reviewing or writing this
  class of fix.
- **Live production DB, not a test fixture.** Acceptance tests that call the real
  API create real rows. Mark them clearly and clean them up, or point future
  tests at a disposable DB (`storage.DB_PATH` is a plain module attribute,
  easily monkeypatched — see `tests/test_invite_transaction.py` and
  `tests/test_search_index.py` for the pattern).

## Where the durable disaster-recovery info lives

See `README.md` for from-scratch setup (Scheduled Task recreation, WSL mirrored-
networking config, etc.) if this machine or its Claude Code memory is ever lost.
This file (`AGENT_BRIEFING.md`) is about *collaboration context*, not *infra setup*
— the two are deliberately separate documents.
