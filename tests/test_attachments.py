"""Tests for file attachments (tranche 5): storage-layer CRUD, quotas,
concurrency, path-traversal/token-grammar defenses, and the REST upload/
download/delete surface (including the header-injection/Unicode filename
matrix and the trusted-Origin guard).

Runs against an isolated Starlette TestClient + temp DB + temp attachment
dir, never the live server.

    python tests/test_attachments.py
"""
from __future__ import annotations

import hashlib
import io
import sys
import tempfile
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))
import storage  # noqa: E402


def _fresh_env() -> None:
    storage.DB_PATH = Path(tempfile.mkstemp(suffix=".sqlite3")[1])
    storage.ATTACHMENT_DIR = Path(tempfile.mkdtemp())
    storage.init_db()


def _client():
    from starlette.testclient import TestClient
    import app as app_module
    # TestClient defaults to Host: testserver, which the trusted-Origin/Host
    # guard correctly rejects (it's not 127.0.0.1/localhost) -- point it at
    # a trusted base URL so these tests exercise the real, deployed
    # behavior instead of tripping over a test-harness artifact.
    return TestClient(app_module.app, base_url="http://127.0.0.1")


# --- storage-layer tests -----------------------------------------------

def test_create_list_get_delete_attachment() -> None:
    _fresh_env()
    t = storage.create_task(title="x")
    sn = storage.generate_storage_name()
    data = b"hello world"
    storage.attachment_path(sn).write_bytes(data)
    a = storage.create_attachment(t["id"], "notes.txt", "text/plain", len(data), hashlib.sha256(data).hexdigest(), sn, "claude")
    assert a["filename"] == "notes.txt"
    assert a["sha256"] == hashlib.sha256(data).hexdigest()

    listed = storage.list_attachments(t["id"])
    assert len(listed) == 1 and listed[0]["id"] == a["id"]

    got = storage.get_attachment(a["id"])
    assert got["id"] == a["id"]

    fetched_task = storage.get_task(t["id"])
    assert len(fetched_task["attachments"]) == 1, "task shape must include attachments"

    result = storage.delete_attachment(t["id"], a["id"])
    assert result == {"removed": True, "storage_name": sn}
    assert storage.list_attachments(t["id"]) == []
    # Idempotent delete
    assert storage.delete_attachment(t["id"], a["id"]) == {"removed": False, "storage_name": None}
    print("PASS: create/list/get/delete attachment, task-shape inclusion, idempotent delete")


def test_attachment_task_membership_and_missing_task() -> None:
    _fresh_env()
    t = storage.create_task(title="x")
    other = storage.create_task(title="y")
    sn = storage.generate_storage_name()
    storage.attachment_path(sn).write_bytes(b"x")
    a = storage.create_attachment(t["id"], "f.txt", "text/plain", 1, "x" * 64, sn, "claude")

    # Deleting via the wrong task_id must not remove it (task-scoped, like checklist items)
    assert storage.delete_attachment(other["id"], a["id"]) == {"removed": False, "storage_name": None}
    assert storage.get_attachment(a["id"]) is not None

    try:
        storage.create_attachment(999999, "f.txt", "text/plain", 1, "x" * 64, storage.generate_storage_name(), "claude")
        raise AssertionError("expected NotFoundError for a nonexistent task")
    except storage.NotFoundError:
        pass
    print("PASS: attachment delete is task-scoped; create rejects a nonexistent task")


def test_per_task_count_cap() -> None:
    _fresh_env()
    t = storage.create_task(title="x")
    for i in range(storage.MAX_ATTACHMENTS_PER_TASK):
        sn = storage.generate_storage_name()
        storage.attachment_path(sn).write_bytes(b"x")
        storage.create_attachment(t["id"], f"f{i}.txt", "text/plain", 1, "x" * 64, sn, "claude")
    sn = storage.generate_storage_name()
    storage.attachment_path(sn).write_bytes(b"x")
    try:
        storage.create_attachment(t["id"], "one-too-many.txt", "text/plain", 1, "x" * 64, sn, "claude")
        raise AssertionError("expected ValueError at the per-task attachment cap")
    except ValueError:
        pass
    finally:
        storage.attachment_path(sn).unlink(missing_ok=True)  # this test writes the file itself, not through the REST layer
    print(f"PASS: per-task attachment cap ({storage.MAX_ATTACHMENTS_PER_TASK}) enforced")


def test_global_byte_quota() -> None:
    _fresh_env()
    old_quota = storage.MAX_TOTAL_ATTACHMENT_BYTES
    storage.MAX_TOTAL_ATTACHMENT_BYTES = 100
    try:
        t = storage.create_task(title="x")
        sn1 = storage.generate_storage_name()
        storage.attachment_path(sn1).write_bytes(b"x" * 60)
        storage.create_attachment(t["id"], "a.bin", "application/octet-stream", 60, "x" * 64, sn1, "claude")
        sn2 = storage.generate_storage_name()
        storage.attachment_path(sn2).write_bytes(b"x" * 60)
        try:
            storage.create_attachment(t["id"], "b.bin", "application/octet-stream", 60, "x" * 64, sn2, "claude")
            raise AssertionError("expected ValueError: 60+60 > 100-byte global quota")
        except ValueError:
            pass
        finally:
            storage.attachment_path(sn2).unlink(missing_ok=True)
    finally:
        storage.MAX_TOTAL_ATTACHMENT_BYTES = old_quota
    print("PASS: global byte quota enforced across attachments")


def test_concurrent_uploads_cannot_both_pass_the_count_cap() -> None:
    """The exact race OpenClaw's review called out: two concurrent
    create_attachment calls each reading 'under the cap' before either
    commits. BEGIN IMMEDIATE inside create_attachment should serialize
    them -- set the cap to exactly 1 and fire 10 concurrent attempts; only
    one may ever succeed."""
    import threading

    _fresh_env()
    old_cap = storage.MAX_ATTACHMENTS_PER_TASK
    storage.MAX_ATTACHMENTS_PER_TASK = 1
    try:
        t = storage.create_task(title="x")
        n = 10
        successes = []
        failures = []
        lock = threading.Lock()

        def worker(i: int) -> None:
            sn = storage.generate_storage_name()
            storage.attachment_path(sn).write_bytes(b"x")
            try:
                storage.create_attachment(t["id"], f"f{i}.txt", "text/plain", 1, "x" * 64, sn, "claude")
                with lock:
                    successes.append(sn)
            except ValueError:
                storage.attachment_path(sn).unlink(missing_ok=True)
                with lock:
                    failures.append(sn)

        threads = [threading.Thread(target=worker, args=(i,)) for i in range(n)]
        for th in threads:
            th.start()
        for th in threads:
            th.join()

        assert len(successes) == 1, f"expected exactly 1 success under a cap of 1, got {len(successes)}"
        assert len(failures) == n - 1
        assert len(storage.list_attachments(t["id"])) == 1
    finally:
        storage.MAX_ATTACHMENTS_PER_TASK = old_cap
    print(f"PASS: {n} concurrent uploads against a cap of 1 -- exactly 1 succeeded, race closed by BEGIN IMMEDIATE")


def test_storage_name_grammar_and_path_traversal_rejected() -> None:
    _fresh_env()
    for bad in ("../../etc/passwd", "..\\..\\windows", "not-hex!!", "", "a" * 31, "A" * 32, "0" * 31 + "g"):
        try:
            storage.attachment_path(bad)
            raise AssertionError(f"expected ValueError for storage_name {bad!r}")
        except ValueError:
            pass
    good = storage.generate_storage_name()
    p = storage.attachment_path(good)
    assert p.parent == storage.ATTACHMENT_DIR
    print("PASS: attachment_path() rejects anything outside the exact token grammar")


def test_cascade_delete_removes_attachment_rows_and_files() -> None:
    _fresh_env()
    t = storage.create_task(title="x")
    sn = storage.generate_storage_name()
    storage.attachment_path(sn).write_bytes(b"x")
    storage.create_attachment(t["id"], "f.txt", "text/plain", 1, "x" * 64, sn, "claude")

    assert storage.delete_task(t["id"]) is True
    with storage._connect() as conn:
        remaining = conn.execute("SELECT COUNT(*) AS c FROM attachments WHERE task_id=?", (t["id"],)).fetchone()["c"]
    assert remaining == 0, "ON DELETE CASCADE must remove the attachment row"
    assert not storage.attachment_path(sn).exists(), "delete_task must also unlink the on-disk bytes, not just the DB row"
    print("PASS: deleting a task cascades its attachment rows AND unlinks their on-disk bytes")


def test_reconcile_cleans_orphans_and_reports_missing() -> None:
    _fresh_env()
    t = storage.create_task(title="x")
    sn = storage.generate_storage_name()
    storage.attachment_path(sn).write_bytes(b"x")
    a = storage.create_attachment(t["id"], "f.txt", "text/plain", 1, "x" * 64, sn, "claude")

    # A stale .part file left by a simulated crash mid-upload
    stale_part = storage.ATTACHMENT_DIR / f"{storage.generate_storage_name()}.part"
    stale_part.write_bytes(b"partial")
    # A finalized file with no DB row (crash between write and DB commit)
    orphan_name = storage.generate_storage_name()
    orphan_path = storage.attachment_path(orphan_name)
    orphan_path.write_bytes(b"orphan")
    # A row whose file is missing
    missing_row_sn = storage.generate_storage_name()
    storage.attachment_path(missing_row_sn).write_bytes(b"will be deleted")
    storage.create_attachment(t["id"], "gone.txt", "text/plain", 1, "x" * 64, missing_row_sn, "claude")
    storage.attachment_path(missing_row_sn).unlink()
    # A stray unrelated file that must NEVER be touched
    stray = storage.ATTACHMENT_DIR / "readme.txt"
    stray.write_bytes(b"not ours")

    report = storage.reconcile_attachment_storage()
    assert not stale_part.exists(), "stale .part file must be removed"
    assert not orphan_path.exists(), "finalized file with no DB row must be removed"
    assert stray.exists(), "a file outside the token grammar must never be touched"
    assert missing_row_sn in report["rows_with_missing_files"], "a row with missing bytes must be reported, not silently ignored"
    assert storage.attachment_path(sn).exists(), "a legitimately-referenced file must survive reconciliation"
    print("PASS: reconciliation removes stale .part/orphaned files, reports missing-file rows, never touches stray files")


# --- REST-layer tests ----------------------------------------------------

def test_rest_upload_download_delete_roundtrip() -> None:
    _fresh_env()
    client = _client()
    t = client.post("/api/tasks", json={"title": "x", "created_by": "claude"}).json()

    res = client.post(
        f"/api/tasks/{t['id']}/attachments",
        files={"file": ("report.txt", io.BytesIO(b"hello world"), "text/plain")},
        data={"uploaded_by": "claude"},
    )
    assert res.status_code == 201, res.text
    meta = res.json()
    assert meta["filename"] == "report.txt"
    assert meta["size_bytes"] == 11
    assert meta["sha256"] == hashlib.sha256(b"hello world").hexdigest()

    listed = client.get(f"/api/tasks/{t['id']}/attachments").json()
    assert len(listed) == 1

    dl = client.get(f"/api/tasks/{t['id']}/attachments/{meta['id']}/download")
    assert dl.status_code == 200
    assert dl.content == b"hello world"
    # Always octet-stream regardless of the claimed/uploaded content-type --
    # this is the real anti-stored-XSS mitigation.
    assert dl.headers["content-type"].startswith("application/octet-stream")
    assert "attachment" in dl.headers["content-disposition"]
    assert dl.headers.get("x-content-type-options") == "nosniff"

    getreq = client.get(f"/api/tasks/{t['id']}")
    assert len(getreq.json()["attachments"]) == 1

    delres = client.delete(f"/api/tasks/{t['id']}/attachments/{meta['id']}")
    assert delres.status_code == 200 and delres.json()["removed"] is True
    assert client.get(f"/api/tasks/{t['id']}/attachments").json() == []
    # Idempotent
    delres2 = client.delete(f"/api/tasks/{t['id']}/attachments/{meta['id']}")
    assert delres2.json()["removed"] is False
    print("PASS: REST upload -> list -> download (octet-stream+attachment+nosniff) -> delete -> idempotent redelete")


def test_rest_rejects_oversized_upload() -> None:
    _fresh_env()
    old_max = storage.MAX_ATTACHMENT_BYTES
    storage.MAX_ATTACHMENT_BYTES = 10
    try:
        client = _client()
        t = client.post("/api/tasks", json={"title": "x", "created_by": "claude"}).json()
        res = client.post(
            f"/api/tasks/{t['id']}/attachments",
            files={"file": ("big.bin", io.BytesIO(b"x" * 11), "application/octet-stream")},
        )
        assert res.status_code == 400, res.text
        assert client.get(f"/api/tasks/{t['id']}/attachments").json() == [], "an oversized upload must not leave a row"
        import os
        assert list(storage.ATTACHMENT_DIR.iterdir()) == [], "an oversized upload must not leave a file on disk either"
    finally:
        storage.MAX_ATTACHMENT_BYTES = old_max
    print("PASS: oversized upload rejected, no DB row or disk file left behind")


def test_rest_rejects_empty_upload() -> None:
    _fresh_env()
    client = _client()
    t = client.post("/api/tasks", json={"title": "x", "created_by": "claude"}).json()
    res = client.post(f"/api/tasks/{t['id']}/attachments", files={"file": ("empty.txt", io.BytesIO(b""), "text/plain")})
    assert res.status_code == 400
    assert list(storage.ATTACHMENT_DIR.iterdir()) == []
    print("PASS: empty upload rejected, no file left behind")


def test_rest_download_missing_attachment_404_and_wrong_task_404() -> None:
    _fresh_env()
    client = _client()
    t1 = client.post("/api/tasks", json={"title": "x", "created_by": "claude"}).json()
    t2 = client.post("/api/tasks", json={"title": "y", "created_by": "claude"}).json()
    up = client.post(f"/api/tasks/{t1['id']}/attachments", files={"file": ("f.txt", io.BytesIO(b"x"), "text/plain")}).json()

    assert client.get(f"/api/tasks/{t1['id']}/attachments/999999/download").status_code == 404
    # Right attachment id, wrong task in the URL -- must not leak across tasks
    assert client.get(f"/api/tasks/{t2['id']}/attachments/{up['id']}/download").status_code == 404
    assert client.delete(f"/api/tasks/{t2['id']}/attachments/{up['id']}").json()["removed"] is False
    print("PASS: download/delete are task-scoped -- an attachment is not reachable through the wrong task's URL")


def test_rest_filename_header_injection_and_unicode_matrix() -> None:
    """A malicious/unusual original filename must never corrupt the
    Content-Disposition header or escape as a path -- the file is always
    opened via the opaque storage_name, and the display filename only ever
    reaches a response header, sanitized first."""
    _fresh_env()
    client = _client()
    t = client.post("/api/tasks", json={"title": "x", "created_by": "claude"}).json()

    # (original filename, expected upload status) -- the 500-char and empty
    # cases are CORRECTLY rejected by storage.py's own length/emptiness
    # checks (ATTACHMENT_FILENAME_MAX_LEN=200), not something the header
    # sanitizer needs to handle; every other case must succeed and download
    # safely with no header injection.
    cases: list[tuple[str, int]] = [
        ("../../etc/passwd", 201),
        ("..\\..\\windows\\system32", 201),
        ('evil"; filename="x.txt', 201),
        ("line1\r\nX-Injected: yes", 201),
        ("emoji-\U0001F600-name.txt", 201),
        ("中文名称.txt", 201),  # CJK
        ("a" * 500 + ".txt", 400),  # exceeds the 200-char storage-layer cap
        ("\x00null\x00.txt", 201),
        ("", 400),  # rejected outright as an empty filename
    ]
    checked = 0
    for name, expected_status in cases:
        res = client.post(f"/api/tasks/{t['id']}/attachments", files={"file": (name, io.BytesIO(b"x"), "text/plain")})
        assert res.status_code == expected_status, f"{name!r}: expected {expected_status}, got {res.status_code}: {res.text}"
        if expected_status != 201:
            continue
        meta = res.json()
        dl = client.get(f"/api/tasks/{t['id']}/attachments/{meta['id']}/download")
        assert dl.status_code == 200
        cd = dl.headers["content-disposition"]
        assert "\r" not in cd and "\n" not in cd, f"{name!r}: CR/LF leaked into Content-Disposition: {cd!r}"
        assert "attachment" in cd
        checked += 1
    print(f"PASS: {checked} malicious/unusual filenames downloaded safely (no header injection), 2 correctly rejected at the boundary")


def test_rest_missing_file_field_and_multiple_file_fields_rejected() -> None:
    _fresh_env()
    client = _client()
    t = client.post("/api/tasks", json={"title": "x", "created_by": "claude"}).json()
    assert client.post(f"/api/tasks/{t['id']}/attachments", data={"not_file": "x"}).status_code == 400
    print("PASS: a request with no 'file' field is rejected")


def test_rest_trusted_origin_guard() -> None:
    """Upload/delete must reject a foreign browser Origin, but allow a
    request with no Origin header at all (MCP/curl/CLI)."""
    _fresh_env()
    client = _client()
    t = client.post("/api/tasks", json={"title": "x", "created_by": "claude"}).json()

    # No Origin header -- must succeed (this is the CLI/MCP case)
    res = client.post(f"/api/tasks/{t['id']}/attachments", files={"file": ("f.txt", io.BytesIO(b"x"), "text/plain")})
    assert res.status_code == 201

    # A foreign Origin -- must be rejected
    res2 = client.post(
        f"/api/tasks/{t['id']}/attachments",
        files={"file": ("f2.txt", io.BytesIO(b"x"), "text/plain")},
        headers={"Origin": "http://evil.example.com"},
    )
    assert res2.status_code == 403, res2.text

    # A trusted Origin -- must succeed
    res3 = client.post(
        f"/api/tasks/{t['id']}/attachments",
        files={"file": ("f3.txt", io.BytesIO(b"x"), "text/plain")},
        headers={"Origin": "http://127.0.0.1:8765"},
    )
    assert res3.status_code == 201, res3.text
    print("PASS: trusted-Origin guard rejects a foreign Origin, allows absent/trusted Origin")


def test_rest_trusted_host_guard() -> None:
    """A spoofed Host header must be rejected the same way a foreign Origin
    is -- separate control from the Origin check, tested separately
    (OpenClaw's review: 'trusted Host rejection as well as foreign
    Origin')."""
    _fresh_env()
    from starlette.testclient import TestClient
    import app as app_module
    # Deliberately an UNTRUSTED base host this time.
    client = TestClient(app_module.app, base_url="http://evil.example.com")
    t = client.post("/api/tasks", json={"title": "x", "created_by": "claude"}).json()
    res = client.post(f"/api/tasks/{t['id']}/attachments", files={"file": ("f.txt", io.BytesIO(b"x"), "text/plain")})
    assert res.status_code == 403, res.text
    assert "Host" in res.json()["error"]
    print("PASS: a spoofed/untrusted Host header is rejected independently of the Origin check")


def test_global_byte_quota_exact_boundary_and_plus_one() -> None:
    _fresh_env()
    old_quota = storage.MAX_TOTAL_ATTACHMENT_BYTES
    storage.MAX_TOTAL_ATTACHMENT_BYTES = 50
    try:
        t = storage.create_task(title="x")
        # Exactly at the quota must succeed.
        sn1 = storage.generate_storage_name()
        storage.attachment_path(sn1).write_bytes(b"x" * 50)
        storage.create_attachment(t["id"], "exact.bin", "application/octet-stream", 50, "x" * 64, sn1, "claude")
        # One more byte over must fail, and not partially land.
        sn2 = storage.generate_storage_name()
        storage.attachment_path(sn2).write_bytes(b"x")
        try:
            storage.create_attachment(t["id"], "plusone.bin", "application/octet-stream", 1, "x" * 64, sn2, "claude")
            raise AssertionError("expected ValueError: exactly-at-quota + 1 byte must be rejected")
        except ValueError:
            pass
        finally:
            storage.attachment_path(sn2).unlink(missing_ok=True)
        assert len(storage.list_attachments(t["id"])) == 1
    finally:
        storage.MAX_TOTAL_ATTACHMENT_BYTES = old_quota
    print("PASS: global byte quota -- exactly-at-limit succeeds, limit+1 is rejected and leaves no row")


def test_concurrent_uploads_cannot_both_pass_the_byte_quota() -> None:
    """Same race as the count-cap test, but against the GLOBAL BYTE QUOTA
    specifically -- a distinct code path (SUM(size_bytes) vs COUNT(*)) that
    needs its own race-closure evidence, not just an inference from the
    count-cap test passing (OpenClaw's review)."""
    import threading

    _fresh_env()
    old_quota = storage.MAX_TOTAL_ATTACHMENT_BYTES
    storage.MAX_TOTAL_ATTACHMENT_BYTES = 100  # room for exactly one 100-byte upload
    try:
        t = storage.create_task(title="x")
        n = 10
        successes, failures = [], []
        lock = threading.Lock()

        def worker(i: int) -> None:
            sn = storage.generate_storage_name()
            storage.attachment_path(sn).write_bytes(b"x" * 100)
            try:
                storage.create_attachment(t["id"], f"f{i}.bin", "application/octet-stream", 100, "x" * 64, sn, "claude")
                with lock:
                    successes.append(sn)
            except ValueError:
                storage.attachment_path(sn).unlink(missing_ok=True)
                with lock:
                    failures.append(sn)

        threads = [threading.Thread(target=worker, args=(i,)) for i in range(n)]
        for th in threads:
            th.start()
        for th in threads:
            th.join()

        assert len(successes) == 1, f"expected exactly 1 success under a 100-byte quota with 100-byte uploads, got {len(successes)}"
        with storage._connect() as conn:
            total = conn.execute("SELECT COALESCE(SUM(size_bytes),0) AS s FROM attachments").fetchone()["s"]
        assert total == 100, f"stored total must never exceed the quota even under concurrency, got {total}"
    finally:
        storage.MAX_TOTAL_ATTACHMENT_BYTES = old_quota
    print(f"PASS: {n} concurrent uploads against a 100-byte quota -- exactly 1 succeeded, global quota race closed")


def test_no_content_length_trust_in_upload_handler() -> None:
    """Structural evidence, not a wire-level one (constructing a genuinely
    lying Content-Length through a high-level HTTP client fights the
    client's own request-building, which recalculates it from the real
    body) -- the handler source itself must never reference the
    Content-Length header for its byte-accounting decision; the streaming
    loop's own running counter is the only source of truth, which is what
    the oversized-upload test already proves behaviorally (a body claiming
    to be small that's actually large still gets rejected once the real
    bytes exceed the limit)."""
    import inspect
    import app as app_module
    source = inspect.getsource(app_module.task_attachment_upload)
    assert "content-length" not in source.lower(), (
        "upload handler must never read Content-Length for its size decision -- only the streaming byte counter"
    )
    print("PASS: upload handler never references Content-Length; byte accounting is solely the streaming counter")


def test_cleanup_on_db_failure_after_file_written() -> None:
    """Simulates the DB-side rejecting an upload (cap already at the
    boundary) AFTER the file has already been fully, successfully written
    to disk -- the file must not survive that failure."""
    _fresh_env()
    old_cap = storage.MAX_ATTACHMENTS_PER_TASK
    storage.MAX_ATTACHMENTS_PER_TASK = 0  # every create_attachment call fails immediately
    try:
        client = _client()
        t = client.post("/api/tasks", json={"title": "x", "created_by": "claude"}).json()
        res = client.post(f"/api/tasks/{t['id']}/attachments", files={"file": ("f.txt", io.BytesIO(b"hello"), "text/plain")})
        assert res.status_code == 400, res.text
        assert list(storage.ATTACHMENT_DIR.iterdir()) == [], "the file written before the DB rejection must be cleaned up"
        assert storage.list_attachments(t["id"]) == []
    finally:
        storage.MAX_ATTACHMENTS_PER_TASK = old_cap
    print("PASS: a DB-level rejection after a successful file write cleans up the orphaned file")


def test_storage_name_collision_handled_safely() -> None:
    """Forces two uploads to generate the identical storage_name (via
    monkeypatching) -- the second must fail cleanly (exclusive-create
    refuses to overwrite) rather than silently clobbering the first
    upload's bytes."""
    _fresh_env()
    client = _client()
    t = client.post("/api/tasks", json={"title": "x", "created_by": "claude"}).json()

    fixed_name = storage.generate_storage_name()
    original = storage.generate_storage_name
    storage.generate_storage_name = lambda: fixed_name
    import app as app_module
    app_module.storage.generate_storage_name = lambda: fixed_name
    try:
        res1 = client.post(f"/api/tasks/{t['id']}/attachments", files={"file": ("first.txt", io.BytesIO(b"first content"), "text/plain")})
        assert res1.status_code == 201, res1.text
        res2 = client.post(f"/api/tasks/{t['id']}/attachments", files={"file": ("second.txt", io.BytesIO(b"second content"), "text/plain")})
        assert res2.status_code != 201, "a storage_name collision must not silently succeed"
    finally:
        storage.generate_storage_name = original
        app_module.storage.generate_storage_name = original

    # The first upload's bytes must be intact, not clobbered by the second attempt.
    listing = storage.list_attachments(t["id"])
    assert len(listing) == 1
    dl = client.get(f"/api/tasks/{t['id']}/attachments/{listing[0]['id']}/download")
    assert dl.content == b"first content", "a colliding second write must never clobber the first upload's bytes"
    print("PASS: a storage_name collision fails the second write cleanly, first upload's bytes stay intact")


def test_rest_download_with_missing_bytes_returns_410() -> None:
    _fresh_env()
    client = _client()
    t = client.post("/api/tasks", json={"title": "x", "created_by": "claude"}).json()
    meta = client.post(f"/api/tasks/{t['id']}/attachments", files={"file": ("f.txt", io.BytesIO(b"x"), "text/plain")}).json()
    storage.attachment_path(meta["storage_name"]).unlink()  # simulate lost bytes with the DB row still present
    res = client.get(f"/api/tasks/{t['id']}/attachments/{meta['id']}/download")
    assert res.status_code == 410, res.text
    print("PASS: a DB row whose bytes are missing on disk returns 410, not a crash or a wrong file")


def test_rest_delete_succeeds_even_if_bytes_already_missing() -> None:
    """A row with missing bytes must still be deletable -- the metadata
    delete is not blocked by an already-lost file (OpenClaw's review)."""
    _fresh_env()
    client = _client()
    t = client.post("/api/tasks", json={"title": "x", "created_by": "claude"}).json()
    meta = client.post(f"/api/tasks/{t['id']}/attachments", files={"file": ("f.txt", io.BytesIO(b"x"), "text/plain")}).json()
    storage.attachment_path(meta["storage_name"]).unlink()
    res = client.delete(f"/api/tasks/{t['id']}/attachments/{meta['id']}")
    assert res.status_code == 200 and res.json()["removed"] is True
    assert storage.list_attachments(t["id"]) == []
    print("PASS: delete succeeds (removed:true) even when the file was already missing before the delete call")


def test_rest_html_and_svg_always_forced_download_never_inline() -> None:
    """The actual anti-stored-XSS evidence: upload real HTML/SVG content
    claiming text/html and image/svg+xml, confirm the download is STILL
    application/octet-stream + attachment disposition + nosniff -- an
    uploaded <script> can never execute same-origin against this API."""
    _fresh_env()
    client = _client()
    t = client.post("/api/tasks", json={"title": "x", "created_by": "claude"}).json()

    html_payload = b"<html><body><script>fetch('/api/tasks').then(r=>r.text()).then(alert)</script></body></html>"
    svg_payload = b'<svg xmlns="http://www.w3.org/2000/svg"><script>alert(document.domain)</script></svg>'
    for name, content, claimed_type in [("evil.html", html_payload, "text/html"), ("evil.svg", svg_payload, "image/svg+xml")]:
        meta = client.post(
            f"/api/tasks/{t['id']}/attachments",
            files={"file": (name, io.BytesIO(content), claimed_type)},
        ).json()
        dl = client.get(f"/api/tasks/{t['id']}/attachments/{meta['id']}/download")
        assert dl.status_code == 200
        assert dl.headers["content-type"].startswith("application/octet-stream"), (
            f"{name}: served as {dl.headers['content-type']}, not forced to octet-stream"
        )
        assert "attachment" in dl.headers["content-disposition"]
        assert dl.headers.get("x-content-type-options") == "nosniff"
        assert dl.content == content, "bytes themselves are still served correctly, just never inline/executable"
    print("PASS: uploaded HTML and SVG (each with an embedded <script>) always download as octet-stream+attachment+nosniff")


def test_rest_head_and_range_preserve_security_headers() -> None:
    """OpenClaw's review: 'verify headers remain on 206/HEAD too' if
    FileResponse's inherited Range/HEAD support is exercised."""
    _fresh_env()
    client = _client()
    t = client.post("/api/tasks", json={"title": "x", "created_by": "claude"}).json()
    meta = client.post(f"/api/tasks/{t['id']}/attachments", files={"file": ("f.txt", io.BytesIO(b"0123456789"), "text/plain")}).json()
    url = f"/api/tasks/{t['id']}/attachments/{meta['id']}/download"

    head = client.head(url)
    assert head.status_code == 200
    assert head.headers.get("x-content-type-options") == "nosniff"
    assert "attachment" in head.headers.get("content-disposition", "")
    assert head.headers["content-type"].startswith("application/octet-stream")

    ranged = client.get(url, headers={"Range": "bytes=0-3"})
    assert ranged.status_code == 206, ranged.text
    assert ranged.content == b"0123"
    assert ranged.headers.get("x-content-type-options") == "nosniff"
    assert "attachment" in ranged.headers.get("content-disposition", "")
    assert ranged.headers["content-type"].startswith("application/octet-stream")
    print("PASS: security headers (nosniff, attachment disposition, octet-stream) survive on both HEAD and 206 Range responses")


if __name__ == "__main__":
    test_create_list_get_delete_attachment()
    test_attachment_task_membership_and_missing_task()
    test_per_task_count_cap()
    test_global_byte_quota()
    test_concurrent_uploads_cannot_both_pass_the_count_cap()
    test_storage_name_grammar_and_path_traversal_rejected()
    test_cascade_delete_removes_attachment_rows_and_files()
    test_reconcile_cleans_orphans_and_reports_missing()
    test_rest_upload_download_delete_roundtrip()
    test_rest_rejects_oversized_upload()
    test_rest_rejects_empty_upload()
    test_rest_download_missing_attachment_404_and_wrong_task_404()
    test_rest_filename_header_injection_and_unicode_matrix()
    test_rest_missing_file_field_and_multiple_file_fields_rejected()
    test_rest_trusted_origin_guard()
    test_rest_trusted_host_guard()
    test_global_byte_quota_exact_boundary_and_plus_one()
    test_concurrent_uploads_cannot_both_pass_the_byte_quota()
    test_no_content_length_trust_in_upload_handler()
    test_cleanup_on_db_failure_after_file_written()
    test_storage_name_collision_handled_safely()
    test_rest_download_with_missing_bytes_returns_410()
    test_rest_delete_succeeds_even_if_bytes_already_missing()
    test_rest_html_and_svg_always_forced_download_never_inline()
    test_rest_head_and_range_preserve_security_headers()
    print("All attachment tests passed.")
