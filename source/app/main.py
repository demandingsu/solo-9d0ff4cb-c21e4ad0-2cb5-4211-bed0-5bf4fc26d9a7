"""Mock institutional-repository source service.

No authentication is required (local demo source). It serves immutable change
events over a closed [start, end] interval, paginated with opaque resumption
cursors. Cursors have a short TTL and can also be invalidated globally via the
control API, letting the collector demonstrate re-fetching the *original*
interval instead of jumping to a newer cutoff.

Arrival watermark
-----------------
Every accepted change is assigned a monotonically increasing arrival sequence
``seq`` at ingestion time (independent of its business ``changed_at``, which
may lie far in the past for late-delivered events). The global *watermark* is
the highest assigned seq. ``GET /changes`` accepts an ``after_seq`` argument so
collectors can catch up on late arrivals whose change time falls inside an
interval already consumed, and every feed is pinned to a request-level
``snapshot_seq``: pages resumed via the cursor always see exactly the same
event set as page 1, even if new events arrive while paging is in progress.
"""
from __future__ import annotations

import base64
import hashlib
import hmac
import json
import os
import threading
from datetime import datetime, timezone

from fastapi import FastAPI, HTTPException, Query
from pydantic import BaseModel, Field

from .data import APPENDED_EVENTS, LATE_BATCH_EVENTS, SEED_EVENTS

PAGE_SIZE = int(os.getenv("SOURCE_PAGE_SIZE", "3"))
CURSOR_TTL_SECONDS = int(os.getenv("CURSOR_TTL_SECONDS", "60"))
SECRET = os.getenv("CURSOR_SECRET", "local-demo-secret").encode()

app = FastAPI(title="Mock Institutional Repository Source", version="2.0.0")
_lock = threading.Lock()


# --------------------------------------------------------------------------- #
# In-memory runtime state
# --------------------------------------------------------------------------- #
class _State:
    def __init__(self) -> None:
        # Each appended row is a full event dict carrying its arrival seq.
        self.appended: list[dict] = []
        # Monotonic arrival-sequence counter. Seed events own seq 1..N.
        self.next_seq = len(SEED_EVENTS) + 1
        # Events waiting to be ingested right after the next successful
        # /changes response — deterministic "arrive while paging" demo.
        self.pending_append: list[dict] = []
        # Bumped on forced cursor expiry; cursors signed under an older
        # generation are rejected even while inside their TTL.
        self.cursor_generation = 0
        self.fail_next = 0  # number of upcoming /changes requests that must fail
        # When > 0, decremented after every successful /changes response; once
        # it reaches 0 the cursor generation is bumped (post-response) so the
        # cursor just handed out is rejected (410) on the next request.
        # Gives a deterministic, race-free cursor-expiry demo.
        self.expire_after_requests = 0
        self.changes_calls = 0


STATE = _State()


def _seed_events_locked() -> list[dict]:
    return [dict(e, seq=i + 1) for i, e in enumerate(SEED_EVENTS)]


def _events_locked() -> list[dict]:
    """Caller must already hold ``_lock`` (the lock is non-reentrant)."""
    return _seed_events_locked() + list(STATE.appended)


def _all_events() -> list[dict]:
    with _lock:
        return _events_locked()


def _watermark_locked() -> int:
    """Highest arrival seq currently visible (0 on an empty feed)."""
    events = _events_locked()
    return int(events[-1]["seq"]) if events else 0


def _append_locked(rows: list[dict]) -> int:
    """Ingest rows under the lock, assigning the next monotonic arrival seq."""
    added = 0
    for row in rows:
        event = dict(row)
        event["seq"] = STATE.next_seq
        STATE.next_seq += 1
        STATE.appended.append(event)
        added += 1
    return added


# --------------------------------------------------------------------------- #
# Opaque signed cursor: base64url(payload).hmac
# --------------------------------------------------------------------------- #
def _sign(payload_b64: str, generation: int) -> str:
    mac = hmac.new(SECRET, f"{generation}:{payload_b64}".encode(), hashlib.sha256)
    return f"{generation}.{payload_b64}.{mac.hexdigest()}"


def make_cursor(start: str, end: str, after_seq: int, offset: int,
                page_size: int, snapshot_seq: int) -> str:
    payload = {
        "start": start,
        "end": end,
        "after_seq": after_seq,
        "offset": offset,
        "page_size": page_size,
        "snapshot_seq": snapshot_seq,
        "exp": datetime.now(timezone.utc).timestamp() + CURSOR_TTL_SECONDS,
    }
    raw = json.dumps(payload, separators=(",", ":"), sort_keys=True).encode()
    payload_b64 = base64.urlsafe_b64encode(raw).decode().rstrip("=")
    return _sign(payload_b64, STATE.cursor_generation)


def parse_cursor(cursor: str) -> dict:
    """Validate signature, generation and TTL. Raises HTTPException(410)."""
    try:
        gen_str, payload_b64, sig = cursor.split(".", 2)
        generation = int(gen_str)
    except (ValueError, AttributeError):
        raise HTTPException(status_code=410, detail="cursor invalid or expired")
    expected = _sign(payload_b64, generation)
    if not hmac.compare_digest(expected, cursor):
        raise HTTPException(status_code=410, detail="cursor invalid or expired")
    if generation != STATE.cursor_generation:
        raise HTTPException(status_code=410, detail="cursor invalid or expired")
    try:
        padded = payload_b64 + "=" * (-len(payload_b64) % 4)
        payload = json.loads(base64.urlsafe_b64decode(padded.encode()))
    except Exception:
        raise HTTPException(status_code=410, detail="cursor invalid or expired")
    if float(payload.get("exp", 0)) < datetime.now(timezone.utc).timestamp():
        raise HTTPException(status_code=410, detail="cursor invalid or expired")
    return payload


# --------------------------------------------------------------------------- #
# Schemas
# --------------------------------------------------------------------------- #
class ChangeEvent(BaseModel):
    seq: int = Field(ge=1, description="monotonic arrival sequence assigned by the source")
    paper_id: str
    rev: int
    title: str
    authors: str
    is_retracted: int = Field(ge=0, le=1)
    changed_at: str


class AppendEvent(BaseModel):
    """Ingestion input: the source assigns seq, so callers never provide it."""
    paper_id: str
    rev: int
    title: str
    authors: str
    is_retracted: int = Field(ge=0, le=1)
    changed_at: str


class ChangesResponse(BaseModel):
    changes: list[ChangeEvent]
    next_cursor: str | None
    next_offset: int
    page: int
    page_size: int
    total_matched: int
    done: bool
    # Pinned snapshot of this feed: resumed pages never see events that arrived
    # after page 1, so cursor/commit timing cannot permanently skip an event.
    snapshot_seq: int
    # The source's current high-water mark at response time (may exceed the
    # snapshot when events arrived during paging).
    watermark: int
    after_seq: int


class AppendRequest(BaseModel):
    events: list[AppendEvent] | None = Field(
        None, description="events to ingest; arrival seq is assigned by the source"
    )


class FailRequest(BaseModel):
    times: int = Field(ge=0)


class ExpireAfterRequest(BaseModel):
    requests: int = Field(ge=0, description="invalidate cursors after this many /changes calls; 0 disables")


# --------------------------------------------------------------------------- #
# Core change feed
# --------------------------------------------------------------------------- #
@app.get("/health")
def health() -> dict:
    with _lock:
        watermark = _watermark_locked()
    return {"status": "ok", "events": watermark, "watermark": watermark,
            "page_size": PAGE_SIZE}


@app.get("/watermark")
def get_watermark() -> dict:
    """Monotonic arrival high-water mark: max assigned arrival seq."""
    with _lock:
        return {"watermark": _watermark_locked()}


def _matches(event: dict, start: str, end: str, after_seq: int) -> bool:
    """Feed predicate.

    Included when the change time is inside the closed [start, end] window,
    OR the event arrived after the consumer's watermark while still not
    exceeding the job cutoff. The second branch is the late-arrival catch-up:
    it rescues events whose changed_at lies in an already-finished interval.
    Events newer than ``end`` are deliberately excluded — a later job picks
    them up once the cutoff catches up.
    """
    if event["changed_at"] > end:
        return False
    return event["changed_at"] >= start or int(event["seq"]) > after_seq


@app.get("/changes", response_model=ChangesResponse)
def get_changes(
    start: str = Query(..., description="closed interval start, ISO-8611 UTC"),
    end: str = Query(..., description="closed interval end, ISO-8601 UTC"),
    after_seq: int = Query(0, ge=0, description="arrival watermark already published by the consumer"),
    cursor: str | None = Query(None),
) -> ChangesResponse:
    # Deterministic fault injection for the disconnect demo.
    with _lock:
        STATE.changes_calls += 1
        if STATE.fail_next > 0:
            STATE.fail_next -= 1
            raise HTTPException(status_code=503, detail="injected transient source failure")

    if start > end:
        raise HTTPException(status_code=400, detail="start must be <= end (ISO-8601 UTC)")

    page_size = PAGE_SIZE
    if cursor is None:
        offset = 0
        # Pin the feed to the current snapshot: events arriving while paging
        # is in flight get seq values above the snapshot and stay invisible to
        # every page of THIS feed (they belong to a future job).
        with _lock:
            snapshot_seq = _watermark_locked()
    else:
        payload = parse_cursor(cursor)  # raises 410 when expired/invalid
        if (payload["start"] != start or payload["end"] != end
                or int(payload["after_seq"]) != after_seq):
            # A cursor may only be resumed inside its original feed (same
            # interval, same arrival-watermark baseline).
            raise HTTPException(status_code=410, detail="cursor invalid or expired")
        offset = int(payload["offset"])
        page_size = int(payload["page_size"])
        snapshot_seq = int(payload["snapshot_seq"])

    with _lock:
        # Snapshot view: only events visible at the pinned watermark.
        snapshot_events = [e for e in _events_locked() if int(e["seq"]) <= snapshot_seq]
        current_watermark = _watermark_locked()

    matched = [e for e in snapshot_events if _matches(e, start, end, after_seq)]
    page_rows = matched[offset:offset + page_size]
    next_offset = offset + len(page_rows)
    done = next_offset >= len(matched)
    next_cursor = (
        None if done
        else make_cursor(start, end, after_seq, next_offset, page_size, snapshot_seq)
    )
    page = offset // page_size + 1 if page_size else 1

    response = ChangesResponse(
        changes=[ChangeEvent(**row) for row in page_rows],
        next_cursor=next_cursor,
        next_offset=next_offset,
        page=page,
        page_size=page_size,
        total_matched=len(matched),
        done=done,
        snapshot_seq=snapshot_seq,
        watermark=current_watermark,
        after_seq=after_seq,
    )

    # Post-response hooks run AFTER the response (and its cursor) has been
    # built under the current generation/snapshot, so the side effects only
    # affect subsequent requests.
    with _lock:
        if STATE.expire_after_requests > 0:
            STATE.expire_after_requests -= 1
            if STATE.expire_after_requests == 0:
                STATE.cursor_generation += 1
        # Deterministic "arrive while paging": ingest the queued events now.
        # Their seq values exceed snapshot_seq, so this feed's later pages
        # (bound to the snapshot) cannot return them; the next job's
        # after_seq catch-up window is what guarantees delivery.
        if STATE.pending_append:
            _append_locked(STATE.pending_append)
            STATE.pending_append = []

    return response


# --------------------------------------------------------------------------- #
# Control API (demo helpers; the collector never calls these)
# --------------------------------------------------------------------------- #
@app.post("/control/fail")
def control_fail(req: FailRequest) -> dict:
    with _lock:
        if req.times == 0:
            # Explicit clear of any queued fault injections.
            STATE.fail_next = 0
        else:
            STATE.fail_next += req.times
        remaining = STATE.fail_next
    return {"fail_next": remaining}


@app.post("/control/expire-after")
def control_expire_after(req: ExpireAfterRequest) -> dict:
    """Schedule a deterministic cursor invalidation after N /changes calls."""
    with _lock:
        STATE.expire_after_requests = req.requests
        return {
            "expire_after_requests": STATE.expire_after_requests,
            "cursor_generation": STATE.cursor_generation,
        }


@app.post("/control/expire-cursors")
def control_expire_cursors() -> dict:
    with _lock:
        STATE.cursor_generation += 1
    return {"cursor_generation": STATE.cursor_generation, "status": "all cursors invalidated"}


@app.post("/control/append-batch")
def control_append_batch() -> dict:
    """Append the prepared incremental batch (idempotent per demo run)."""
    with _lock:
        if not STATE.appended:
            added = _append_locked(APPENDED_EVENTS)
        else:
            added = 0
        total = _watermark_locked()
    return {"added": added, "watermark": total, "total_events": total}


@app.post("/control/append-late-batch")
def control_append_late_batch() -> dict:
    """Append the prepared LATE-arrival batch (idempotent per demo run).

    The retraction's changed_at is inside the already-synced interval; only
    the arrival watermark lets the next job observe it.
    """
    with _lock:
        marker = ("P0004", 2)
        already = any(
            (e["paper_id"], e["rev"]) == marker for e in STATE.appended
        )
        added = 0 if already else _append_locked(LATE_BATCH_EVENTS)
        total = _watermark_locked()
    return {"added": added, "watermark": total, "total_events": total}


@app.post("/control/append")
def control_append(req: AppendRequest) -> dict:
    rows = [e.model_dump(exclude={"seq"}) for e in req.events] if req.events else []
    with _lock:
        added = _append_locked(rows)
        total = _watermark_locked()
    return {"added": added, "watermark": total, "total_events": total}


@app.post("/control/append-during-paging")
def control_append_during_paging(req: AppendRequest) -> dict:
    """Queue events to be ingested right after the next successful /changes.

    Demonstrates that events arriving mid-pagination cannot be skipped by
    cursor or commit timing: the pinned snapshot excludes them from this feed
    while the next job's after_seq window guarantees they are collected.
    """
    rows = [e.model_dump(exclude={"seq"}) for e in req.events] if req.events else []
    with _lock:
        STATE.pending_append.extend(rows)
        queued = len(STATE.pending_append)
    return {"queued_for_next_feed_response": queued}


@app.post("/control/reset")
def control_reset() -> dict:
    with _lock:
        STATE.appended = []
        STATE.pending_append = []
        STATE.next_seq = len(SEED_EVENTS) + 1
        STATE.fail_next = 0
        STATE.cursor_generation = 0
        STATE.expire_after_requests = 0
        STATE.changes_calls = 0
        total = _watermark_locked()
    return {"status": "reset", "watermark": total, "total_events": total}


@app.get("/control/state")
def control_state() -> dict:
    with _lock:
        return {
            "total_events": _watermark_locked(),
            "watermark": _watermark_locked(),
            "appended": len(STATE.appended),
            "pending_append": len(STATE.pending_append),
            "next_seq": STATE.next_seq,
            "fail_next": STATE.fail_next,
            "cursor_generation": STATE.cursor_generation,
            "expire_after_requests": STATE.expire_after_requests,
            "changes_calls": STATE.changes_calls,
            "cursor_ttl_seconds": CURSOR_TTL_SECONDS,
        }


@app.get("/events")
def list_events() -> dict:
    return {"events": _all_events(), "watermark": _watermark_locked()}
