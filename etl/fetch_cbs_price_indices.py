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
                value = (obs.get("currBase") or {}).get("value")
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
                rows.append({"year": key[0], "month": key[1], "value": float(value)})
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


def to_db_rows(series_id: int, series_name: str, observations: list[dict]) -> list[dict]:
    """Convert API observations to DB rows with is_provisional flags.

    `observations` is expected in most-recent-first order (as returned by
    fetch_series). The first PROVISIONAL_TAIL entries are flagged
    provisional. The date is stored as YYYY-MM-01 since the index is
    monthly and the day has no meaning.
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
        all_rows.extend(to_db_rows(series["id"], series["name"], observations))

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
