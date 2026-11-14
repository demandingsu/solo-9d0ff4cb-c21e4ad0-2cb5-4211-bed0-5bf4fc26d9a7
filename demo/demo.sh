#!/usr/bin/env bash
# End-to-end demo:
#   1. first full sync (paging, shuffled/duplicate events, retraction hidden)
#   2. disconnect mid-sync, then resume along the same durable checkpoint
#   3. LATE-arriving retraction: changed_at is inside an already-finished
#      interval; only the arrival watermark can bring it home
#   4. events arriving DURING pagination: pinned snapshot keeps them out of
#      the current feed, and the next job's after_seq window guarantees them
#   5. cursor expiry: the expired cursor re-fetches the ORIGINAL feed
#   6. collector restart: an in-flight job becomes interrupted and resumes
#
# Usage:
#   docker compose up -d --build
#   ./demo/demo.sh
set -euo pipefail

SOURCE=${SOURCE_URL:-http://localhost:8000}
COLLECTOR=${COLLECTOR_URL:-http://localhost:8001}

hr()    { printf '\n==================== %s ====================\n' "$1"; }
say()   { printf '\n--- %s ---\n' "$1"; }
req()   { curl -fsS "$@"; }
wait_for() {
  local url=$1
  for _ in $(seq 1 60); do
    curl -fsS "$url" >/dev/null 2>&1 && return 0
    sleep 1
  done
  echo "timeout waiting for $url" >&2
  exit 1
}
# Poll a job until it leaves collecting/committing. A stale 'failed' right
# after /resume (the worker thread has not flipped status yet) is tolerated.
poll_job() {
  local id=$1 seen_active=0 status
  for _ in $(seq 1 60); do
    status=$(req "$COLLECTOR/sync/jobs/$id" | jq -r .status)
    { [ "$status" = "collecting" ] || [ "$status" = "committing" ]; } && seen_active=1
    [ "$status" = "completed" ] && { echo "$status"; return 0; }
    [ "$status" = "failed" ] && [ "$seen_active" = "1" ] && { echo "$status"; return 0; }
    [ "$status" = "interrupted" ] && { echo "$status"; return 0; }
    sleep 0.5
  done
  echo "timeout waiting for job $id (last=$status)" >&2
  exit 1
}

wait_for "$SOURCE/health"
wait_for "$COLLECTOR/health"

hr "0. Reset demo state"
req -X POST "$SOURCE/control/reset" | jq .
req "$SOURCE/control/state" | jq .
say "Source arrival watermark before any sync"
req "$SOURCE/watermark" | jq .

# ---------------------------------------------------------------------------
hr "1. First full sync (closed interval, paged, shuffled/duplicated events)"
# ---------------------------------------------------------------------------
JOB=$(req -X POST "$COLLECTOR/sync/jobs" \
        -H 'Content-Type: application/json' \
        -d '{"end": "2026-11-08T23:59:59Z"}' | jq .)
echo "$JOB" | jq '{id, interval_start, interval_end, after_seq, status}'
JOB_ID=$(echo "$JOB" | jq -r .id)
echo "  final status: $(poll_job "$JOB_ID")"
req "$COLLECTOR/sync/jobs/$JOB_ID" \
  | jq '{id, status, pages_fetched, staged_count, cursor_resets, snapshot_seq}'

say "Completed time AND arrival watermark are now recorded (two baselines)"
req "$COLLECTOR/sync/state" | jq .

say "Library: 4 visible papers; P0001 and P0003 are retracted (hidden)"
req "$COLLECTOR/papers" | jq -r '.[] | "\(.paper_id) rev=\(.rev) retracted=\(.is_retracted)  \(.title)"'
req "$COLLECTOR/papers-count?include_retracted=true" | jq .

say "Retracted paper lookup -> HTTP 410 Gone; with include_retracted -> visible"
curl -s -o /dev/null -w "  GET /papers/P0001 -> HTTP %{http_code}\n" "$COLLECTOR/papers/P0001"
req "$COLLECTOR/papers/P0001?include_retracted=true" \
  | jq -r '"  with flag: \(.paper_id) rev=\(.rev) retracted=\(.is_retracted)"'

say "Highest revision wins (P0002 -> rev 3) despite stale duplicate delivery"
req "$COLLECTOR/papers/P0002" | jq -r '"  P0002 rev=\(.rev): \(.title)"'

# ---------------------------------------------------------------------------
hr "2. Disconnect mid-sync: inject source fault, then resume"
# ---------------------------------------------------------------------------
say "Append the incremental batch to the repository"
req -X POST "$SOURCE/control/append-batch" | jq .

say "Make the next /changes request fail with HTTP 503 (one-shot)"
req -X POST "$SOURCE/control/fail" -H 'Content-Type: application/json' -d '{"times":1}' | jq .

say "Start incremental sync (start defaults to last completed time)"
JOB2=$(req -X POST "$COLLECTOR/sync/jobs" \
         -H 'Content-Type: application/json' \
         -d '{"end": "2026-11-10T23:59:59Z"}')
JOB2_ID=$(echo "$JOB2" | jq -r .id)
echo "$JOB2" | jq '{id, interval_start, interval_end, after_seq, status}'
echo "  final status: $(poll_job "$JOB2_ID")"
req "$COLLECTOR/sync/jobs/$JOB2_ID" | jq '{id, status, error, pages_fetched}'

say "Old library stays queryable; staged changes stay invisible (P0007 not found)"
curl -s -o /dev/null -w "  GET /papers/P0007 -> HTTP %{http_code} (404 expected)\n" "$COLLECTOR/papers/P0007"
req "$COLLECTOR/papers-count" | jq -r '"  visible papers while job failed: \(.count)"'

say "Exhaust the injected fault, then resume the SAME job (same interval/after_seq)"
req -X POST "$SOURCE/control/fail" -H 'Content-Type: application/json' -d '{"times":0}' >/dev/null
req -X POST "$COLLECTOR/sync/jobs/$JOB2_ID/resume" >/dev/null
echo "  final status: $(poll_job "$JOB2_ID")"
req "$COLLECTOR/sync/jobs/$JOB2_ID" \
  | jq '{id, status, pages_fetched, staged_count, cursor_resets, after_seq}'

say "Incremental results: P0007 added, P0005 at rev 3, watermark advanced"
req "$COLLECTOR/papers" | jq -r '.[] | "  \(.paper_id) rev=\(.rev) retracted=\(.is_retracted)"'
req "$COLLECTOR/papers/P0005" | jq -r '"  P0005 -> rev=\(.rev), authors=\(.authors)"'
req "$COLLECTOR/sync/state" | jq .

say "Retraction cannot be overwritten by an old revision (P0002 retracted at rev 4)"
curl -s -o /dev/null -w "  GET /papers/P0002 -> HTTP %{http_code} (410 expected)\n" "$COLLECTOR/papers/P0002"
req "$COLLECTOR/papers/P0002?include_retracted=true" | jq -r '"  with flag: rev=\(.rev) retracted=\(.is_retracted)"'

# ---------------------------------------------------------------------------
hr "3. LATE-arriving retraction: change time is inside the finished window"
# ---------------------------------------------------------------------------
say "P0004 is currently visible and was last synced inside the 11-08 window"
curl -s -o /dev/null -w "  GET /papers/P0004 -> HTTP %{http_code} (200 expected)\n" "$COLLECTOR/papers/P0004"

say "The knowledge base confirms a retraction whose changed_at is 2026-11-04"
req -X POST "$SOURCE/control/append-late-batch" | jq .
say "It only arrives now: source watermark moved, but the event time is old"
req "$SOURCE/watermark" | jq .
req "$SOURCE/events" | jq -r '.events[] | select(.paper_id=="P0004") | "  seq=\(.seq) rev=\(.rev) retracted=\(.is_retracted) changed_at=\(.changed_at)"'

say "Next incremental job: time window starts at 11-10, after_seq covers the gap"
JOB3=$(req -X POST "$COLLECTOR/sync/jobs" \
         -H 'Content-Type: application/json' \
         -d '{"end": "2026-11-11T23:59:59Z"}')
JOB3_ID=$(echo "$JOB3" | jq -r .id)
echo "$JOB3" | jq '{id, interval_start, interval_end, after_seq}'
echo "  final status: $(poll_job "$JOB3_ID")"

say "The late retraction is applied: P0004 disappears from retrieval"
curl -s -o /dev/null -w "  GET /papers/P0004 -> HTTP %{http_code} (410 expected)\n" "$COLLECTOR/papers/P0004"
req "$COLLECTOR/papers/P0004?include_retracted=true" \
  | jq -r '"  with flag: \(.paper_id) rev=\(.rev) retracted=\(.is_retracted) changed_at=\(.changed_at)"'
say "The in-window new paper riding the same batch is also published"
req "$COLLECTOR/papers/P0008" | jq -r '"  P0008 rev=\(.rev): \(.title)"'
req "$COLLECTOR/sync/state" | jq .

# ---------------------------------------------------------------------------
hr "4. Events arriving DURING pagination must never be skipped"
# ---------------------------------------------------------------------------
say "Queue two events to arrive right after the next /changes response"
req -X POST "$SOURCE/control/append-during-paging" -H 'Content-Type: application/json' \
  -d '{"events":[
    {"paper_id":"D1","rev":1,"title":"Pager Arrival One","authors":"S. Lin","is_retracted":0,"changed_at":"2026-11-12T08:00:00Z"},
    {"paper_id":"D2","rev":1,"title":"Pager Arrival Two","authors":"T. Moore","is_retracted":0,"changed_at":"2026-11-12T09:00:00Z"}
  ]}' | jq .
say "Add four more in-window events so the feed spans multiple pages (page_size=3)"
req -X POST "$SOURCE/control/append" -H 'Content-Type: application/json' \
  -d '{"events":[
    {"paper_id":"E1","rev":1,"title":"E One","authors":"a","is_retracted":0,"changed_at":"2026-11-12T10:00:00Z"},
    {"paper_id":"E2","rev":1,"title":"E Two","authors":"a","is_retracted":0,"changed_at":"2026-11-12T11:00:00Z"},
    {"paper_id":"E3","rev":1,"title":"E Three","authors":"a","is_retracted":0,"changed_at":"2026-11-12T12:00:00Z"},
    {"paper_id":"E4","rev":1,"title":"E Four","authors":"a","is_retracted":0,"changed_at":"2026-11-12T13:00:00Z"}
  ]}' | jq .

JOB4=$(req -X POST "$COLLECTOR/sync/jobs" \
         -H 'Content-Type: application/json' \
         -d '{"end": "2026-11-12T23:59:59Z"}')
JOB4_ID=$(echo "$JOB4" | jq -r .id)
echo "  final status: $(poll_job "$JOB4_ID")"
req "$COLLECTOR/sync/jobs/$JOB4_ID" | jq '{id, status, pages_fetched, snapshot_seq}'

say "The feed was pinned to the page-1 snapshot: D1/D2 stay invisible for now"
curl -s -o /dev/null -w "  GET /papers/D1 -> HTTP %{http_code} (404 expected)\n" "$COLLECTOR/papers/D1"
req "$COLLECTOR/papers/E4" | jq -r '"  E4 (inside snapshot) -> rev=\(.rev)"'
say "Published watermark is the SNAPSHOT, not the live source watermark"
echo "  collector watermark=$(req "$COLLECTOR/sync/state" | jq -r .watermark_seq)"
echo "  source    watermark=$(req "$SOURCE/watermark" | jq -r .watermark)  (2 higher: D1, D2)"

say "The next job collects D1/D2 through after_seq, even though their times are old"
JOB5=$(req -X POST "$COLLECTOR/sync/jobs" \
         -H 'Content-Type: application/json' \
         -d '{"end": "2026-11-13T23:59:59Z"}')
JOB5_ID=$(echo "$JOB5" | jq -r .id)
echo "$JOB5" | jq '{id, interval_start, after_seq}'
echo "  final status: $(poll_job "$JOB5_ID")"
req "$COLLECTOR/papers/D1" | jq -r '"  D1 -> rev=\(.rev): \(.title)"'
req "$COLLECTOR/papers/D2" | jq -r '"  D2 -> rev=\(.rev): \(.title)"'
req "$COLLECTOR/sync/state" | jq .

# ---------------------------------------------------------------------------
hr "5. Cursor expiry: expired cursor re-fetches the ORIGINAL feed"
# ---------------------------------------------------------------------------
say "Add FOUR events so the window spans multiple pages (page_size=3)"
req -X POST "$SOURCE/control/append" -H 'Content-Type: application/json' \
  -d '{"events":[
    {"paper_id":"F1","rev":1,"title":"Fresh One","authors":"U. Vega","is_retracted":0,"changed_at":"2026-11-14T10:00:00Z"},
    {"paper_id":"F2","rev":1,"title":"Fresh Two","authors":"V. Wood","is_retracted":0,"changed_at":"2026-11-14T11:00:00Z"},
    {"paper_id":"F3","rev":1,"title":"Fresh Three","authors":"W. Xu","is_retracted":0,"changed_at":"2026-11-14T12:00:00Z"},
    {"paper_id":"F4","rev":1,"title":"Fresh Four","authors":"X. Young","is_retracted":0,"changed_at":"2026-11-14T13:00:00Z"}
  ]}' >/dev/null

say "Deterministic hook: invalidate cursors right after the FIRST /changes page"
req -X POST "$SOURCE/control/expire-after" \
  -H 'Content-Type: application/json' -d '{"requests":1}' | jq -c .

JOB6=$(req -X POST "$COLLECTOR/sync/jobs" \
         -H 'Content-Type: application/json' \
         -d '{"end":"2026-11-14T23:59:59Z"}')
JOB6_ID=$(echo "$JOB6" | jq -r .id)
ORIGINAL_END=$(echo "$JOB6" | jq -r .interval_end)
ORIGINAL_ASEQ=$(echo "$JOB6" | jq -r .after_seq)
echo "  final status: $(poll_job "$JOB6_ID")"
req "$COLLECTOR/sync/jobs/$JOB6_ID" \
  | jq '{id, status, error, cursor_resets, staged_count, interval_start, interval_end, after_seq}'

say "Resume: staging restarts from offset 0 but keeps the SAME interval and after_seq"
req -X POST "$COLLECTOR/sync/jobs/$JOB6_ID/resume" >/dev/null
echo "  final status: $(poll_job "$JOB6_ID")"
req "$COLLECTOR/sync/jobs/$JOB6_ID" \
  | jq '{id, status, pages_fetched, cursor_resets, interval_end, after_seq}'
say "The interval end and arrival baseline never moved"
[ "$(req "$COLLECTOR/sync/jobs/$JOB6_ID" | jq -r .interval_end)" = "$ORIGINAL_END" ] \
  && echo "  OK: interval_end still $ORIGINAL_END"
[ "$(req "$COLLECTOR/sync/jobs/$JOB6_ID" | jq -r .after_seq)" = "$ORIGINAL_ASEQ" ] \
  && echo "  OK: after_seq still $ORIGINAL_ASEQ"
for p in F1 F2 F3 F4; do
  req "$COLLECTOR/papers/$p" | jq -r --arg p "$p" '"  " + $p + " rev=\(.rev): \(.title)"'
done

# ---------------------------------------------------------------------------
hr "6. Restart resilience: kill the collector mid-job, then resume"
# ---------------------------------------------------------------------------
say "Append an event, then make the next /changes call fail so the job stays open"
req -X POST "$SOURCE/control/append" -H 'Content-Type: application/json' \
  -d '{"events":[{"paper_id":"P0009","rev":1,"title":"Federated Search Latency Budgets",
                  "authors":"N. Ahmed","is_retracted":0,"changed_at":"2026-11-15T09:00:00Z"}]}' >/dev/null
req -X POST "$SOURCE/control/fail" -H 'Content-Type: application/json' -d '{"times":1}' >/dev/null

req -X POST "$COLLECTOR/sync/jobs" -H 'Content-Type: application/json' \
  -d '{"end":"2026-11-15T23:59:59Z"}' | jq '{id, status, interval_end}'
for _ in $(seq 1 20); do
  ST=$(req "$COLLECTOR/sync/jobs" | jq -r '.[0].status')
  [ "$ST" != "collecting" ] && [ "$ST" != "committing" ] && break
  sleep 0.5
done

say "Restarting the collector process/container (SQLite lives on a named volume)"
if command -v docker >/dev/null 2>&1 && docker compose ps >/dev/null 2>&1; then
  docker compose restart collector >/dev/null
else
  echo "  docker not available here; in production the same flip happens on process start"
fi
wait_for "$COLLECTOR/health"

say "After restart the open job is resumable (interrupted)"
req "$COLLECTOR/sync/jobs" | jq -r '.[0] | "  job \(.id): status=\(.status) pages=\(.pages_fetched) resets=\(.cursor_resets) after_seq=\(.after_seq)"'
LATEST=$(req "$COLLECTOR/sync/jobs" | jq -r '.[0].id')

say "Clear the fault and resume (same closed interval, durable checkpoint)"
req -X POST "$SOURCE/control/fail" -H 'Content-Type: application/json' -d '{"times":0}' >/dev/null
req -X POST "$COLLECTOR/sync/jobs/$LATEST/resume" >/dev/null
echo "  final status: $(poll_job "$LATEST")"
req "$COLLECTOR/papers/P0009" | jq -r '"  P0009 -> rev=\(.rev): \(.title)"'

# ---------------------------------------------------------------------------
hr "7. Final library state"
# ---------------------------------------------------------------------------
req "$COLLECTOR/sync/state" | jq .
say "Visible (non-retracted) papers:"
req "$COLLECTOR/papers" | jq -r '.[] | "  \(.paper_id) rev=\(.rev)  \(.title)"'
say "All papers including retracted:"
req "$COLLECTOR/papers?include_retracted=true" \
  | jq -r '.[] | "  \(.paper_id) rev=\(.rev) retracted=\(.is_retracted)  \(.title)"'
say "Job history:"
req "$COLLECTOR/sync/jobs" | jq -r '.[] | "  job \(.id): \(.status) pages=\(.pages_fetched) resets=\(.cursor_resets) after_seq=\(.after_seq) [\(.interval_start), \(.interval_end)]"'

hr "Demo complete"
