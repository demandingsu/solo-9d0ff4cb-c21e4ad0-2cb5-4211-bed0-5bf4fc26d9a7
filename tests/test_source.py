"""Source service tests: paging, cursor expiry, watermark and snapshots.

The lock regression test matters: control endpoints must not call another
lock-acquiring helper while already holding the global state lock.
"""
from __future__ import annotations

import threading

import pytest
from fastapi.testclient import TestClient

from source.app.main import STATE, _lock, app


@pytest.fixture
def client():
    with TestClient(app) as c:
        c.post("/control/reset")
        yield c
        c.post("/control/reset")


def test_lock_is_never_reentered(client):
    # Every control endpoint must return promptly even though the state lock is
    # non-reentrant. A self-deadlock would make these time out.
    done = threading.Event()
    errors: list[Exception] = []

    def exercise():
        try:
            for call in (
                lambda: client.get("/health"),
                lambda: client.get("/watermark"),
                lambda: client.get("/control/state"),
                lambda: client.post("/control/reset"),
                lambda: client.post(
                    "/control/append",
                    json={"events": [{
                        "paper_id": "X1", "rev": 1, "title": "t",
                        "authors": "a", "is_retracted": 0,
                        "changed_at": "2026-11-09T00:00:00Z",
                    }]},
                ),
                lambda: client.post(
                    "/control/append-during-paging",
                    json={"events": [{
                        "paper_id": "X2", "rev": 1, "title": "t",
                        "authors": "a", "is_retracted": 0,
                        "changed_at": "2026-11-09T01:00:00Z",
                    }]},
                ),
                lambda: client.post("/control/append-batch"),
                lambda: client.post("/control/append-late-batch"),
                lambda: client.get("/events"),
            ):
                r = call()
                assert r.status_code == 200, r.text
        except Exception as exc:  # pragma: no cover
            errors.append(exc)
        finally:
            done.set()

    t = threading.Thread(target=exercise, daemon=True)
    t.start()
    if not done.wait(timeout=10):
        pytest.fail("source endpoint self-deadlocked on the non-reentrant lock")
    assert not errors


def test_closed_interval_paging_and_dedup_inputs(client):
    r = client.get(
        "/changes",
        params={"start": "2026-11-01T00:00:00Z", "end": "2026-11-04T00:00:00Z"},
    )
    assert r.status_code == 200
    first = r.json()
    assert first["page_size"] == 3
    assert first["done"] is False and first["next_cursor"]
    # Every event carries a monotonic arrival seq and the feed pins a snapshot.
    assert all(e["seq"] >= 1 for e in first["changes"])
    assert first["snapshot_seq"] >= first["changes"][-1]["seq"]
    assert first["watermark"] >= first["snapshot_seq"]
    cursor = first["next_cursor"]

    r2 = client.get(
        "/changes",
        params={"start": "2026-11-01T00:00:00Z",
                "end": "2026-11-04T00:00:00Z", "cursor": cursor},
    )
    assert r2.status_code == 200
    body = r2.json()
    assert body["page"] == 2
    # Resumed pages keep the same pinned snapshot.
    assert body["snapshot_seq"] == first["snapshot_seq"]


def test_cursor_rejected_after_generation_bump(client):
    r = client.get(
        "/changes",
        params={"start": "2026-11-01T00:00:00Z", "end": "2026-11-08T00:00:00Z"},
    )
    cursor = r.json()["next_cursor"]
    client.post("/control/expire-cursors")
    r2 = client.get(
        "/changes",
        params={"start": "2026-11-01T00:00:00Z",
                "end": "2026-11-08T00:00:00Z", "cursor": cursor},
    )
    assert r2.status_code == 410


def test_cursor_bound_to_original_interval(client):
    r = client.get(
        "/changes",
        params={"start": "2026-11-01T00:00:00Z", "end": "2026-11-04T00:00:00Z"},
    )
    cursor = r.json()["next_cursor"]
    # Reuse the cursor against a different (newer) interval -> 410, never a
    # silent jump to the newer cutoff.
    r2 = client.get(
        "/changes",
        params={"start": "2026-11-01T00:00:00Z",
                "end": "2026-11-09T00:00:00Z", "cursor": cursor},
    )
    assert r2.status_code == 410


def test_cursor_bound_to_original_after_seq(client):
    r = client.get(
        "/changes",
        params={"start": "2026-11-01T00:00:00Z", "end": "2026-11-08T00:00:00Z",
                "after_seq": 5},
    )
    cursor = r.json()["next_cursor"]
    r2 = client.get(
        "/changes",
        params={"start": "2026-11-01T00:00:00Z", "end": "2026-11-08T00:00:00Z",
                "after_seq": 6, "cursor": cursor},
    )
    assert r2.status_code == 410


def test_injected_failure_then_recovery(client):
    client.post("/control/fail", json={"times": 1})
    r1 = client.get(
        "/changes",
        params={"start": "2026-11-01T00:00:00Z", "end": "2026-11-04T00:00:00Z"},
    )
    assert r1.status_code == 503
    r2 = client.get(
        "/changes",
        params={"start": "2026-11-01T00:00:00Z", "end": "2026-11-04T00:00:00Z"},
    )
    assert r2.status_code == 200
    assert _lock.acquire(blocking=False)  # lock must be free
    _lock.release()
    assert STATE.fail_next == 0


def test_expire_after_hook_invalidates_next_cursor(client):
    # Wide window covers all seed events (multiple pages), so the first call
    # always returns a continuation cursor.
    params = {"start": "1970-01-01T00:00:00Z", "end": "2026-12-31T23:59:59Z"}
    # First /changes call succeeds; afterwards the generation is bumped, so the
    # cursor returned by that call is rejected on the following request.
    client.post("/control/expire-after", json={"requests": 1})
    r1 = client.get("/changes", params=params)
    assert r1.status_code == 200
    cursor = r1.json()["next_cursor"]
    assert cursor is not None  # multi-page window guarantees a cursor
    r2 = client.get("/changes", params={**params, "cursor": cursor})
    assert r2.status_code == 410
    # A request without a cursor still works (re-fetch from offset 0 allowed).
    r3 = client.get("/changes", params=params)
    assert r3.status_code == 200 and r3.json()["page"] == 1


def test_arrival_seq_is_monotonic_and_watermark_tracks_it(client):
    wm0 = client.get("/watermark").json()["watermark"]
    r = client.post("/control/append", json={"events": [
        {"paper_id": "W1", "rev": 1, "title": "t", "authors": "a",
         "is_retracted": 0, "changed_at": "2026-11-20T00:00:00Z"},
        {"paper_id": "W2", "rev": 1, "title": "t", "authors": "a",
         "is_retracted": 0, "changed_at": "2026-11-20T01:00:00Z"},
    ]})
    assert r.json()["added"] == 2
    wm1 = client.get("/watermark").json()["watermark"]
    assert wm1 == wm0 + 2
    events = client.get("/events").json()["events"]
    seqs = [e["seq"] for e in events]
    assert seqs == sorted(seqs) and len(set(seqs)) == len(seqs)
    assert max(seqs) == wm1


def test_snapshot_pins_feed_when_events_arrive_during_paging(client):
    params = {"start": "1970-01-01T00:00:00Z", "end": "2026-12-31T23:59:59Z"}
    first = client.get("/changes", params=params).json()
    snapshot = first["snapshot_seq"]
    total = first["total_matched"]
    assert total == snapshot  # all seed events visible, matched count frozen

    # Queue arrivals to be ingested right after the next response.
    client.post("/control/append-during-paging", json={"events": [
        {"paper_id": "D1", "rev": 1, "title": "t", "authors": "a",
         "is_retracted": 0, "changed_at": "2026-11-21T00:00:00Z"},
        {"paper_id": "D2", "rev": 1, "title": "t", "authors": "a",
         "is_retracted": 0, "changed_at": "2026-11-21T01:00:00Z"},
    ]})

    second = client.get("/changes",
                        params={**params, "cursor": first["next_cursor"]}).json()
    # The feed remains pinned to the old snapshot: total_matched is unchanged.
    # The pending rows are ingested only AFTER this response is built, so its
    # live watermark still equals the snapshot...
    assert second["snapshot_seq"] == snapshot
    assert second["total_matched"] == total
    assert second["watermark"] == snapshot
    # ...and the very next request observes the new arrivals.
    assert client.get("/watermark").json()["watermark"] == snapshot + 2

    # A fresh feed (no cursor) observes the new snapshot including D1/D2.
    fresh = client.get("/changes", params=params).json()
    assert fresh["snapshot_seq"] == snapshot + 2
    assert fresh["total_matched"] == total + 2


def test_after_seq_catches_late_arrival_inside_old_interval(client):
    # The late batch's retraction has changed_at inside the seed window but
    # only arrives now.
    client.post("/control/append-late-batch")
    client.get("/watermark").json()["watermark"]

    # Time-window-only predicate would miss it (changed_at < start)...
    late_params = {
        "start": "2026-11-09T00:00:00Z", "end": "2026-11-10T00:00:00Z",
        "after_seq": 13,  # watermark published by an earlier finished job
    }
    # ...but after_seq pulls it in. Page through the whole feed to collect
    # every matched row.
    full: list[dict] = []
    cursor = None
    while True:
        params = dict(late_params)
        if cursor:
            params["cursor"] = cursor
        page = client.get("/changes", params=params).json()
        full.extend(page["changes"])
        if page["done"]:
            break
        cursor = page["next_cursor"]
    ids = {e["paper_id"]: e for e in full}
    assert "P0004" in ids and ids["P0004"]["is_retracted"] == 1
    assert "P0008" in ids
    # Every returned event either sits in the time window or is after the seq.
    for e in full:
        assert (e["changed_at"] >= late_params["start"]
                and e["changed_at"] <= late_params["end"]) or e["seq"] > 13
        assert e["changed_at"] <= late_params["end"]


def test_events_beyond_cutoff_are_excluded_regardless_of_seq(client):
    client.post("/control/append", json={"events": [
        {"paper_id": "F1", "rev": 1, "title": "t", "authors": "a",
         "is_retracted": 0, "changed_at": "2026-12-25T00:00:00Z"},
    ]})
    body = client.get("/changes", params={
        "start": "1970-01-01T00:00:00Z", "end": "2026-11-30T00:00:00Z",
        "after_seq": 0,
    }).json()
    assert "F1" not in {e["paper_id"] for e in body["changes"]}
