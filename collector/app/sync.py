"""Sync worker: paginated fetch with durable checkpoints and atomic publish.

Late-arrival handling
---------------------
Each job carries ``after_seq`` — the arrival watermark already published by
previous jobs. The source feed predicate is

    changed_at <= end AND (changed_at >= start OR seq > after_seq)

so the job collects the ordinary closed time window *and* late arrivals whose
change time sits in an already-consumed interval. The feed is pinned to a
source snapshot (``snapshot_seq``); events arriving while pages are being
taken stay out of this feed and are guaranteed to appear in a later job,
because the published watermark is the snapshot — never the source's live
watermark.

On every page, rows plus the continuation cursor commit in one transaction.
A 410 (expired cursor) discards this job's staging and re-fetches the
*original* feed (same interval, same after_seq) — never a newer cutoff.
The whole feed is published together: papers, completed time and watermark
commit atomically, so any failure leaves the old library fully queryable.
"""
from __future__ import annotations

import threading
from datetime import datetime, timezone

import httpx

from . import db

# In-memory guard so a single process never runs two workers at once.
_running = threading.Lock()


class SyncError(RuntimeError):
    """Any fetch/transport failure; the job stays resumable."""


def _normalize_iso(value: str) -> str:
    """Accept ...Z / +00:00 / naive; emit canonical UTC 'YYYY-MM-DDTHH:MM:SSZ'."""
    text = value.strip()
    try:
        if text.endswith("Z"):
            dt = datetime.fromisoformat(text[:-1]).replace(tzinfo=timezone.utc)
        else:
            dt = datetime.fromisoformat(text)
            if dt.tzinfo is None:
                dt = dt.replace(tzinfo=timezone.utc)
            dt = dt.astimezone(timezone.utc)
    except ValueError as exc:
        raise SyncError(f"invalid ISO-8601 timestamp: {value!r}") from exc
    return dt.strftime("%Y-%m-%dT%H:%M:%SZ")


def _fetch_page(client: httpx.Client, source_url: str, start: str, end: str,
                after_seq: int, cursor: str | None) -> dict:
    params = {"start": start, "end": end, "after_seq": after_seq}
    if cursor:
        params["cursor"] = cursor
    try:
        resp = client.get(f"{source_url.rstrip('/')}/changes", params=params)
    except httpx.HTTPError as exc:
        raise SyncError(f"source request failed: {exc}") from exc
    if resp.status_code != 200:
        raise SyncError(
            f"source returned HTTP {resp.status_code}: {resp.text[:300]}"
        )
    return resp.json()


def run_sync(job_id: int, db_path: str | None = None) -> None:
    """Execute (or resume) one sync job until completion or failure.

    On every page: persist rows + next cursor in ONE transaction before the
    next request. A 410 (expired cursor) discards this job's staging and
    re-fetches the *original* feed — never a newer cutoff.
    """
    if not _running.acquire(blocking=False):
        return  # another worker is active in this process

    conn = db.connect(db_path)
    try:
        job = db.get_job(conn, job_id)
        if job is None:
            return
        if job["status"] not in ("collecting", "interrupted", "failed"):
            return
        db.mark_status(conn, "collecting", job_id)
        conn.commit()

        source_url = job["source_url"]
        start = job["interval_start"]
        end = job["interval_end"]
        after_seq = int(job["after_seq"])
        cursor = job["cursor"]
        pages = job["pages_fetched"]
        fetch_done = bool(job["fetch_done"])
        snapshot_seq = job["snapshot_seq"]

        with httpx.Client(timeout=10.0) as client:
            while not fetch_done:
                try:
                    page = _fetch_page(
                        client, source_url, start, end, after_seq, cursor
                    )
                except SyncError as exc:
                    msg = str(exc)
                    if "HTTP 410" in msg:
                        # Cursor expired/invalid: restart the ORIGINAL feed.
                        # after_seq is intentionally left untouched so the
                        # re-fetch uses the identical arrival baseline.
                        with db.transaction(conn) as tx:
                            db.clear_staged(tx, job_id)
                            resets = db.increment_cursor_resets(tx, job_id)
                            db.mark_status(
                                tx, job_id, "failed",
                                f"cursor expired; feed re-fetch required "
                                f"(reset #{resets})",
                            )
                        return
                    with db.transaction(conn) as tx:
                        db.mark_status(tx, job_id, "failed", msg)
                    return

                # Page 1 pins the source snapshot; later pages (resumed via
                # the signed cursor) keep seeing the same event set.
                if snapshot_seq is None:
                    snapshot_seq = int(page["snapshot_seq"])

                # Durable checkpoint: page rows and continuation cursor commit
                # together, so resume never loses or skips a page.
                with db.transaction(conn) as tx:
                    for event in page["changes"]:
                        db.upsert_staged(tx, job_id, event)
                    cursor = page["next_cursor"]
                    pages += 1
                    fetch_done = bool(page["done"])
                    db.update_cursor(
                        tx, job_id, cursor, page["next_offset"],
                        pages, fetch_done, snapshot_seq,
                    )

        # Whole feed collected: publish staging, completed time AND arrival
        # watermark in one atomic transaction. Any error rolls back; the old
        # library stays queryable and staging remains private until a
        # successful resume.
        with db.transaction(conn) as tx:
            db.mark_status(tx, job_id, "committing")
            published_at = db.utc_now_iso()
            for row in db.list_staged(tx, job_id):
                db.upsert_paper(tx, row)
            # completed_at is the change-time baseline for the next job, so it
            # must equal this interval's closed end (not wall-clock time);
            # otherwise a historical cutoff would never be re-fetchable.
            #
            # The watermark advances to the pinned SNAPSHOT, not the source's
            # live watermark: anything that arrived while paging was above the
            # snapshot and will be collected by a later job's after_seq window.
            # It is also monotonic: after_seq is itself a previous snapshot.
            watermark = int(snapshot_seq) if snapshot_seq is not None else after_seq
            if watermark < after_seq:
                watermark = after_seq
            tx.execute(
                "UPDATE sync_state SET source_url=?, last_start=?, last_end=?, "
                "completed_at=?, watermark_seq=? WHERE id=1",
                (source_url, start, end, end, watermark),
            )
            tx.execute(
                "UPDATE jobs SET updated_at=? WHERE id=?", (published_at, job_id)
            )
            db.mark_status(tx, job_id, "completed")
    except Exception as exc:  # pragma: no cover - defensive guard
        conn.rollback()
        with db.transaction(conn) as tx:
            db.mark_status(tx, job_id, "failed", f"unexpected error: {exc}")
    finally:
        conn.close()
        _running.release()


def start_background(job_id: int, db_path: str | None = None) -> bool:
    """Spawn a daemon worker thread. Returns False if one is already running."""
    if _running.locked():
        return False
    thread = threading.Thread(
        target=run_sync, args=(job_id, db_path), daemon=True,
        name=f"sync-job-{job_id}",
    )
    thread.start()
    return True
