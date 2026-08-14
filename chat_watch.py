"""Poll a CollabHub live chat session and print each new message from the OTHER
author, one per stdout line, on a fixed interval. Exits (after printing
SESSION_ENDED) once the session is closed by either side.

Meant to be run under something that turns new stdout lines into notifications
(e.g. the Monitor tool) so an agent finds out about a new message without
holding a blocking connection open.

Usage: python chat_watch.py <session_id> <my_author_name> [interval_seconds] [since_id]
"""
from __future__ import annotations

import json
import sys
import time
import urllib.error
import urllib.request

# Windows consoles default stdout to the system codepage (cp1252 etc), which
# can't encode emoji/other non-Latin1 characters agents may send in chat --
# force UTF-8 with a safe fallback so a fancy message can't crash the poller.
sys.stdout.reconfigure(encoding="utf-8", errors="replace")

BASE = "http://127.0.0.1:8765"


def poll(session_id: int, since: int) -> dict:
    url = f"{BASE}/api/chat/poll?session_id={session_id}&since={since}"
    with urllib.request.urlopen(url, timeout=5) as resp:
        return json.loads(resp.read().decode("utf-8"))


def main() -> None:
    if len(sys.argv) < 3:
        print("usage: chat_watch.py <session_id> <my_author_name> [interval_seconds]", file=sys.stderr)
        raise SystemExit(2)

    session_id = int(sys.argv[1])
    me = sys.argv[2]
    interval = float(sys.argv[3]) if len(sys.argv) > 3 else 10.0
    since = int(sys.argv[4]) if len(sys.argv) > 4 else 0
    while True:
        try:
            data = poll(session_id, since)
        except (urllib.error.URLError, TimeoutError, ConnectionError) as exc:
            print(f"POLL_ERROR {exc}", flush=True)
            time.sleep(interval)
            continue

        for msg in data.get("messages", []):
            since = max(since, msg["id"])
            if msg["author"] != me:
                text = msg["text"].replace("\n", "\\n")
                print(f"MSG {msg['id']} {msg['author']}: {text}", flush=True)

        if not data.get("active", False):
            print("SESSION_ENDED", flush=True)
            break

        time.sleep(interval)


if __name__ == "__main__":
    main()
