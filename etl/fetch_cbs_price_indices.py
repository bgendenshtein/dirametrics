"""Fetch CBS price indices and upsert into Supabase.

Source: CBS price-index API (https://api.cbs.gov.il/index/data/price).
Target: Supabase table `cbs_price_indices`
        (series_id, date, value, is_provisional) UNIQUE (series_id, date).

Series fetched (see etl/NOTES_CBS.md for provenance and cross-checks):
  - 40010  : מדד מחירי דירות (national housing prices, from 1994)
  - 120460 : מדד שכר דירה (rent — private, public, long-term regulated)
  - 120010 : מדד המחירים לצרכן - כללי (CPI general)
  - 60000–60500 : housing prices by district (from 2017-10)

Notes on provisionality:
  CBS documents that the 3 most recent values per series are provisional
  and may be revised in later publications. We mark the 3 latest returned
  observations per series as is_provisional=true, everything older as
  false. Upsert on (series_id, date) means re-running the job overwrites
  provisional rows with the latest values (including promoting them to
  non-provisional once they fall out of that series's top-3 window).

Notes on base-period chaining:
  CBS rebases price indices every ~2 years (e.g. CPI's most recent base
  is "2024 average = 100"; rent and CPI have rebased ~18 times since
  1959). The API's `currBase.value` field returns each observation on
  the base in effect AT OBSERVATION TIME — not chained to today's base.
  Stored raw, this produces a saw-tooth where January-after-rebase
  values reset to ~100 and lose comparability with prior values.

  We post-process via chain_to_latest_base, which detects multi-base
  series dynamically (>1 distinct currBase.baseDesc across history)
  and rescales older segments so the entire series reads continuously
  on the latest base. Single-base series like 40010 (housing) pass
  through unchanged.
"""

from __future__ import annotations

import logging
import os
import sys
import time
from pathlib import Path

import requests
from dotenv import load_dotenv
from supabase import Client, create_client

API_BASE = "https://api.cbs.gov.il/index/data/price"
TABLE = "cbs_price_indices"
BATCH_SIZE = 500
PROVISIONAL_TAIL = 3
PAGE_SIZE = 100
USER_AGENT = "DiraMetrics/1.0 (+bgendenshtein@gmail.com)"
DEFAULT_TIMEOUT = 60

# Per-request retry on transient HTTP/connection errors. Mirrors the
# resilience layer in fetch_cbs_series.py — same constants, same log
# format, same retry policy. See etl/NOTES_CBS.md for the rationale and
# why we did NOT also backport gap recovery / lenient acceptance to this
# script.
HTTP_RETRY_ATTEMPTS = 3                       # retries after the initial attempt
HTTP_RETRY_BACKOFF_SEQUENCE = [2, 5, 15]      # seconds between attempts (must align with HTTP_RETRY_ATTEMPTS)
TRANSIENT_HTTP_STATUSES = {500, 502, 503, 504}
TRANSIENT_EXCEPTIONS = (
    requests.exceptions.ConnectionError,
    requests.exceptions.ChunkedEncodingError,
    requests.exceptions.Timeout,
)

# Series to fetch. Each entry's `id` is the CBS codeId used by
# /data/price?id=...; `name` is stored in the `series_name` column.
# See etl/NOTES_CBS.md for how these were identified.
SERIES = [
    {"id": 40010,  "name": "מדד מחירי דירות"},
    {"id": 120460, "name": "מדד שכר דירה"},
    {"id": 120010, "name": "מדד המחירים לצרכן - כללי"},
    # By-district housing price indices (CBS chapter aa, subject 166).
    # Available from October 2017. Distinct from the national index (40010,
    # available from 1994) — UI should show this gap as missing data, not zero.
    {"id": 60000,  "name": "ירושלים"},
    {"id": 60100,  "name": "צפון"},
    {"id": 60200,  "name": "חיפה"},
    {"id": 60300,  "name": "מרכז"},
    {"id": 60400,  "name": "תל אביב"},
    {"id": 60500,  "name": "דרום"},
]

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s %(levelname)s %(message)s",
)
log = logging.getLogger("cbs_price_indices")


def make_session() -> requests.Session:
    """Build a requests Session with a User-Agent.

    The price-index API does not require User-Agent the way the
    time-series API at apis.cbs.gov.il does, but setting one costs
    nothing and matches the convention in fetch_cbs_series.py.
    """
    s = requests.Session()
    s.headers.update({"User-Agent": USER_AGENT})
    return s


def _http_get_with_retry(
    session: requests.Session,
    url: str,
    params: dict,
    *,
    series_id: int,
) -> requests.Response:
    """GET with retry on transient HTTP/connection errors.

    Retries up to HTTP_RETRY_ATTEMPTS times after the initial attempt
    (so HTTP_RETRY_ATTEMPTS+1 tries max). Backoff follows
    HTTP_RETRY_BACKOFF_SEQUENCE — element [i] is the delay before
    attempt i+1.

    Retried on:
      - HTTP 500/502/503/504 (TRANSIENT_HTTP_STATUSES)
      - ConnectionError, ChunkedEncodingError, Timeout

    Non-transient errors (4xx, other 5xx, unexpected exceptions) are
    raised immediately without retry. Identical behavior and log format
    to the helper in fetch_cbs_series.py.
    """
    total_tries = HTTP_RETRY_ATTEMPTS + 1
    for attempt in range(total_tries):
        is_last = attempt == total_tries - 1
        try:
            resp = session.get(url, params=params, timeout=DEFAULT_TIMEOUT)
        except TRANSIENT_EXCEPTIONS as exc:
            err = type(exc).__name__
            if is_last:
                log.warning(
                    "Series %s: %s persisted after %d attempts: %s",
                    series_id, err, total_tries, exc,
                )
                raise
            wait = HTTP_RETRY_BACKOFF_SEQUENCE[attempt]
            log.warning(
                "Series %s: %s on attempt %d/%d, retrying in %ds",
                series_id, err, attempt + 1, total_tries, wait,
            )
            time.sleep(wait)
            continue

        if resp.status_code in TRANSIENT_HTTP_STATUSES:
            if is_last:
                log.warning(
                    "Series %s: HTTP %d persisted after %d attempts",
                    series_id, resp.status_code, total_tries,
                )
                resp.raise_for_status()
            wait = HTTP_RETRY_BACKOFF_SEQUENCE[attempt]
            log.warning(
                "Series %s: HTTP %d on attempt %d/%d, retrying in %ds",
                series_id, resp.status_code, attempt + 1, total_tries, wait,
            )
            time.sleep(wait)
            continue

        resp.raise_for_status()
        if attempt > 0:
            log.info(
                "Series %s: succeeded after %d retr%s",
                series_id, attempt, "y" if attempt == 1 else "ies",
            )
        return resp

    raise RuntimeError(f"Series {series_id}: HTTP retry loop exited without return")


def fetch_series(series_id: int, session: requests.Session) -> list[dict]:
    """Fetch all paginated observations for a CBS price-index series.

    Returns a list of {year, month, value} dicts ordered most-recent
    first (preserving API order). Callers can rely on that order for
    provisional marking.

    De-duplicates by (year, month) across pages as defense in depth —
    not currently observed on this API (pagination here is deterministic
    in repeated probes), but cheap and matches the dedup convention in
    fetch_cbs_series.py where the time-series API DOES exhibit overlap.
    """
    rows: list[dict] = []
    seen: set[tuple[int, int]] = set()
    raw_count = 0
    page = 1
    while True:
        params = {
            "id": series_id,
            "format": "json",
            "Page": page,
            "PageSize": PAGE_SIZE,
        }
        log.info("GET %s page=%d", API_BASE, page)
        resp = _http_get_with_retry(session, API_BASE, params, series_id=series_id)
        payload = resp.json()

        # `month` is a list with (usually) a single element whose `date`
        # field holds the observations for this page.
        month_buckets = payload.get("month") or []
        page_dups = 0
        for bucket in month_buckets:
            for obs in bucket.get("date", []) or []:
                curr_base = obs.get("currBase") or {}
                value = curr_base.get("value")
                base_desc = curr_base.get("baseDesc")
                year = obs.get("year")
                month = obs.get("month")
                if value is None or year is None or month is None:
                    continue
                raw_count += 1
                key = (int(year), int(month))
                if key in seen:
                    page_dups += 1
                    continue
                seen.add(key)
                rows.append({
                    "year": key[0],
                    "month": key[1],
                    "value": float(value),
                    # baseDesc is the "currBase" period in effect at
                    # observation time. CBS rebases CPI/rent every
                    # ~2 years and the API returns each obs on its
                    # at-time base, NOT chained — chain_to_latest_base
                    # post-processes this so the series reads
                    # continuously across rebases.
                    "base_desc": base_desc,
                })
        if page_dups:
            log.info(
                "Series %s page %d: %d duplicate (year, month) skipped "
                "(defense-in-depth dedup)",
                series_id, page, page_dups,
            )

        paging = payload.get("paging") or {}
        last_page = paging.get("last_page") or 1
        if page >= last_page:
            break
        page += 1

    log.info(
        "Series %s: %d unique obs (raw=%d, deduped=%d)",
        series_id, len(rows), raw_count, raw_count - len(rows),
    )
    return rows


def chain_to_latest_base(observations: list[dict]) -> list[dict]:
    """Re-anchor a multi-base index series to the most recent base.

    CBS rebases price indices every ~2 years (most recently to "2024
    average = 100"). Each obs returns on its AT-TIME base via
    `currBase`, so a series spanning multiple rebases shows a saw-
    tooth — values reset to ~100 at every rebase. This pass stitches
    the segments into one continuous series anchored on the LATEST
    base (so the most recent observation keeps its native value and
    historical values are scaled to be comparable).

    Algorithm:
      1. Sort ascending by (year, month).
      2. Identify segments where `base_desc` is constant.
      3. The newest segment is the canonical reference (factor 1.0).
      4. For each older segment, walking backward, the boundary's
         chain factor = first_value_of_newer_segment /
         last_value_of_older_segment. This assumes month-over-month
         change at the boundary is ≈0; rebases happen at year-start
         where MoM is small relative to the rebase magnitude, so
         the residual error is at most ~1pp per chain step. (The
         API also exposes `percent` per obs which would let us
         remove this assumption — kept simple here; revisit if a
         user spots a precision-level discrepancy.)
      5. Compound: a segment N rebases removed from the newest gets
         the product of all N boundary factors.

    Single-base series (e.g. housing 40010, which has used the same
    1993Q2-derived base since 1994) pass through unchanged. Detection
    is dynamic: any series with > 1 distinct base_desc value is
    chained; any with ≤ 1 is returned as-is.

    Returns a new list with `value` rescaled. Other fields (year,
    month, base_desc) are preserved on each obs. The returned list
    is sorted ascending; callers that need the original most-recent-
    first order should re-sort.
    """
    if len(observations) < 2:
        return list(observations)

    sorted_obs = sorted(observations, key=lambda o: (o["year"], o["month"]))

    distinct_bases = {o.get("base_desc") for o in sorted_obs if o.get("base_desc")}
    if len(distinct_bases) <= 1:
        # Single-base series — no chaining needed. Housing 40010 lives
        # here, as does any future series CBS doesn't periodically
        # rebase.
        return list(sorted_obs)

    # Build segments of constant base_desc
    segments: list[tuple[int, int, str]] = []
    seg_start = 0
    for i in range(1, len(sorted_obs)):
        if sorted_obs[i].get("base_desc") != sorted_obs[i - 1].get("base_desc"):
            segments.append((seg_start, i, str(sorted_obs[seg_start].get("base_desc"))))
            seg_start = i
    segments.append(
        (seg_start, len(sorted_obs), str(sorted_obs[seg_start].get("base_desc")))
    )

    # Per-segment cumulative chain factors. Walk from the newest backward;
    # each older segment compounds the factors of all newer segments it
    # has to cross to reach the canonical base.
    factors: list[float] = [1.0] * len(segments)
    for k in range(len(segments) - 2, -1, -1):
        last_old_value = float(sorted_obs[segments[k][1] - 1]["value"])
        first_new_value = float(sorted_obs[segments[k + 1][0]]["value"])
        if last_old_value == 0:
            # Avoid divide-by-zero; pass through unchanged for this
            # boundary. Should never happen for an index value but
            # guard anyway.
            boundary_factor = 1.0
        else:
            boundary_factor = first_new_value / last_old_value
        factors[k] = factors[k + 1] * boundary_factor

    log.info(
        "Chained %d segments across base periods %s",
        len(segments),
        " → ".join(s[2] for s in segments),
    )

    chained: list[dict] = []
    for s_idx, (start, end, _) in enumerate(segments):
        f = factors[s_idx]
        for i in range(start, end):
            o = sorted_obs[i]
            chained.append({**o, "value": float(o["value"]) * f})
    return chained


def to_db_rows(series_id: int, series_name: str, observations: list[dict]) -> list[dict]:
    """Convert API observations to DB rows with is_provisional flags.

    `observations` is expected in most-recent-first order (as returned by
    fetch_series). The first PROVISIONAL_TAIL entries are flagged
    provisional. The date is stored as YYYY-MM-01 since the index is
    monthly and the day has no meaning.

    `base_desc` (added to obs by fetch_series for chaining) is dropped
    here — the DB schema doesn't carry it. The chaining factor has
    already been applied to `value` by chain_to_latest_base, so the
    DB row carries continuous-base values without needing to track
    the underlying base periods.
    """
    db_rows: list[dict] = []
    for idx, obs in enumerate(observations):
        date_str = f"{obs['year']:04d}-{obs['month']:02d}-01"
        db_rows.append({
            "series_id": series_id,
            "series_name": series_name,
            "date": date_str,
            "value": obs["value"],
            "is_provisional": idx < PROVISIONAL_TAIL,
        })
    return db_rows


def upsert_rows(client: Client, rows: list[dict]) -> int:
    """Upsert rows on (series_id, date). Returns count of rows sent."""
    sent = 0
    for start in range(0, len(rows), BATCH_SIZE):
        batch = rows[start : start + BATCH_SIZE]
        client.table(TABLE).upsert(batch, on_conflict="series_id,date").execute()
        sent += len(batch)
        log.info("Upserted batch %d-%d (%d total)", start, start + len(batch), sent)
    return sent


def main() -> int:
    load_dotenv(Path(__file__).parent / ".env")

    url = os.environ.get("SUPABASE_URL")
    key = os.environ.get("SUPABASE_SERVICE_ROLE_KEY")
    if not url or not key:
        log.error("SUPABASE_URL and SUPABASE_SERVICE_ROLE_KEY must be set (see .env.example)")
        return 2

    session = make_session()
    all_rows: list[dict] = []
    for series in SERIES:
        try:
            observations = fetch_series(series["id"], session)
        except Exception as exc:
            log.exception("Failed to fetch CBS series %s: %s", series["id"], exc)
            return 1
        if not observations:
            log.warning("Series %s returned no observations", series["id"])
            continue
        # Chain across rebases. Single-base series pass through. The
        # function logs when it actually chains so the operator sees
        # which series got rescaled and over how many segments.
        chained = chain_to_latest_base(observations)
        # Restore most-recent-first order for to_db_rows's
        # provisional-tail logic (which expects the most-recent
        # PROVISIONAL_TAIL entries at indices 0..2).
        chained_recent_first = sorted(
            chained, key=lambda o: (o["year"], o["month"]), reverse=True
        )
        all_rows.extend(
            to_db_rows(series["id"], series["name"], chained_recent_first)
        )

    if not all_rows:
        log.warning("No rows to upsert; exiting")
        return 0

    client = create_client(url, key)
    try:
        sent = upsert_rows(client, all_rows)
    except Exception as exc:
        log.exception("Upsert failed: %s", exc)
        return 1

    log.info("Done. Upserted %d rows into %s across %d series", sent, TABLE, len(SERIES))
    return 0


if __name__ == "__main__":
    sys.exit(main())
