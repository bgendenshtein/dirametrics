"""Fetch CBS macro indicators (population / unemployment / wage) and
upsert into Supabase.

Source: CBS time-series API (https://apis.cbs.gov.il/series/data/list).
Target: Supabase table `cbs_series`
        UNIQUE (topic, district, frequency, time_period).

Stored under three new topics, all with district='national' since CBS
doesn't publish district-level breakdowns for these macro indicators
that we'd want to use:

  - population_addition  (DERIVED)
        From series 3763 (total population, monthly, 1991-01 →
        present, values in thousands). For each month t we store
        value[t] − value[t−1]; the first month (1991-01) is skipped
        since it has no t−1.

  - unemployment_rate    (DERIVED — spliced)
        Two underlying CBS series feed this topic because CBS revised
        the labor-force-survey methodology in Jan 2025:
            490013 — old definition, 2008-census base   (Jan 2012 –
                                                          Dec 2025)
            40013  — new definition, 2022-census base   (Jan 2025  –
                                                          present )
        The two cohorts overlap through 2025; differences are 0.0–
        0.1pp at the boundary. We splice with NEW preferred when both
        exist for a given period, falling back to OLD elsewhere. The
        resulting series is continuous from Jan 2012.

  - average_wage         (RAW)
        From series 613208 (monthly average wage per employee
        position, total economy including foreign workers, original
        data, 2011 industry classification, Bituach Leumi). Values in
        NIS. Strong December seasonality from year-end bonuses.

Frequency stored as 'monthly' for all three. Higher-frequency
aggregations (quarterly / semiannual / annual) are computed
client-side via aggregateData using each series's registry-declared
aggregation method ('sum' for population_addition, 'average' for
unemployment_rate and average_wage — see seriesRegistry.ts).

is_provisional: top 3 most-recent observations per topic are flagged.
is_derived:     true for population_addition (computed diff) and
                unemployment_rate (spliced). false for average_wage.

Failure handling: each series fetched independently; failures are
logged and the run continues for the rest. Splice + diff are skipped
if their inputs are missing.
"""

from __future__ import annotations

import logging
import os
import sys
import time
from datetime import date
from pathlib import Path

import requests
from dotenv import load_dotenv
from supabase import Client, create_client

API_BASE = "https://apis.cbs.gov.il/series/data/list"
TABLE = "cbs_series"
BATCH_SIZE = 500
PROVISIONAL_TAIL = 3
PAGE_SIZE = 100
USER_AGENT = "DiraMetrics/1.0 (+bgendenshtein@gmail.com)"
DEFAULT_TIMEOUT = 60

# CBS series IDs. Investigation in chat → 2026-05-01.
POPULATION_TOTAL_ID = 3763           # monthly, 1991-01 onward, in thousands
UNEMPLOYMENT_OLD_ID = 490013         # 2008-census methodology, Jan 2012 – Dec 2025
UNEMPLOYMENT_NEW_ID = 40013          # 2022-census methodology, Jan 2025 – present
AVERAGE_WAGE_ID = 613208             # monthly NIS, Jan 2012 onward

POPULATION_NAME = "תוספת אוכלוסייה (חודשית)"
UNEMPLOYMENT_NAME = "שיעור אבטלה"
WAGE_NAME = "שכר ממוצע למשרת שכיר"

# Per-request retry on transient HTTP/connection errors. Same constants
# and policy as fetch_cbs_series.py and fetch_cbs_price_indices.py.
HTTP_RETRY_ATTEMPTS = 3
HTTP_RETRY_BACKOFF_SEQUENCE = [2, 5, 15]
TRANSIENT_HTTP_STATUSES = {500, 502, 503, 504}
TRANSIENT_EXCEPTIONS = (
    requests.exceptions.ConnectionError,
    requests.exceptions.ChunkedEncodingError,
    requests.exceptions.Timeout,
)

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s %(levelname)s %(message)s",
)
log = logging.getLogger("cbs_macro")


def make_session() -> requests.Session:
    """The time-series API silently rejects requests without a UA
    header — set one on a session reused across all calls."""
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
    """GET with retry on transient HTTP/connection errors. Mirrors the
    helper in the other two CBS ETL scripts. Retries up to
    HTTP_RETRY_ATTEMPTS times after the initial attempt; non-transient
    errors are raised immediately."""
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


def fetch_series(series_id: int, session: requests.Session) -> dict[str, float]:
    """Fetch all observations for a CBS time-series and return them as a
    {time_period: value} dict.

    Pagination: 100 obs per page; the time-series API exposes
    `paging.last_page` so we loop until exhausted. First-occurrence wins
    on duplicate periods (the API has occasionally been observed to
    return the same period across pages — defensive dedup matches
    fetch_cbs_series.py).
    """
    rows: dict[str, float] = {}
    page = 1
    raw_count = 0
    while True:
        params = {
            "id": series_id,
            "format": "json",
            "Page": page,
            "PageSize": PAGE_SIZE,
        }
        log.info("GET %s id=%s page=%d", API_BASE, series_id, page)
        resp = _http_get_with_retry(session, API_BASE, params, series_id=series_id)
        payload = resp.json()
        try:
            obs_list = payload["DataSet"]["Series"][0].get("obs", []) or []
        except (KeyError, IndexError) as exc:
            log.warning("Series %s page %d: unexpected payload shape (%s)", series_id, page, exc)
            obs_list = []

        for obs in obs_list:
            tp = obs.get("TimePeriod")
            v = obs.get("Value")
            if tp is None or v is None:
                continue
            raw_count += 1
            if tp in rows:
                continue
            rows[str(tp)] = float(v)

        paging = payload.get("DataSet", {}).get("paging", {}) or {}
        last_page = paging.get("last_page") or 1
        if page >= last_page:
            break
        page += 1

    log.info(
        "Series %s: %d unique obs (raw=%d, deduped=%d)",
        series_id, len(rows), raw_count, raw_count - len(rows),
    )
    return rows


def parse_period(time_period: str) -> date:
    """CBS monthly TimePeriod strings come as 'YYYY-MM' or 'YYYY-MM-01'.
    Always normalize to the first of the month."""
    parts = time_period.split("-")
    return date(int(parts[0]), int(parts[1]), 1)


def compute_population_addition(pop: dict[str, float]) -> dict[str, float]:
    """Compute month-over-month addition from a total-population dict.

    Iterates periods in ascending order; for each t after the first,
    emits value[t] - value[t-1]. The first observed period is skipped
    (no prior to subtract from). Both input and output values are in
    the same units (thousands, since the population series is published
    in thousands).
    """
    if len(pop) < 2:
        log.warning("population: need at least 2 obs to compute addition; got %d", len(pop))
        return {}

    sorted_periods = sorted(pop.keys())
    out: dict[str, float] = {}
    for i in range(1, len(sorted_periods)):
        prev = sorted_periods[i - 1]
        curr = sorted_periods[i]
        # Sanity: ensure consecutive months. If a gap exists in the
        # source, the diff would be cumulative for the gap window —
        # tag a warning and still emit so the operator sees it.
        prev_d = parse_period(prev)
        curr_d = parse_period(curr)
        expected_month = prev_d.month % 12 + 1
        expected_year = prev_d.year + (1 if prev_d.month == 12 else 0)
        if curr_d.month != expected_month or curr_d.year != expected_year:
            log.warning(
                "population: gap %s → %s (not consecutive); diff will span the gap",
                prev, curr,
            )
        out[curr] = pop[curr] - pop[prev]

    log.info("population_addition: %d months derived", len(out))
    return out


def splice_unemployment(
    old: dict[str, float], new: dict[str, float]
) -> dict[str, float]:
    """Splice old (490013) + new (40013) unemployment series. New
    methodology wins for any period present in both; old fills
    everything new doesn't cover.

    Logs the boundary so the operator can spot-check that the splice
    landed where expected (Jan 2025 should be the first 'new' period;
    pre-2025 should be 'old' only)."""
    spliced = dict(old)
    overlap = 0
    new_only = 0
    for k, v in new.items():
        if k in spliced:
            overlap += 1
        else:
            new_only += 1
        spliced[k] = v
    log.info(
        "unemployment_rate splice: old=%d, new=%d, overlap=%d, new-only=%d → spliced=%d",
        len(old), len(new), overlap, new_only, len(spliced),
    )
    return spliced


def to_db_rows(
    topic: str,
    name_he: str,
    series_id: int | str,
    is_derived: bool,
    period_to_value: dict[str, float],
) -> list[dict]:
    """Convert a {period: value} dict to cbs_series row dicts.

    Sorts most-recent-first and flags the first PROVISIONAL_TAIL
    entries as provisional — same convention as the other CBS ETLs.
    All rows go in as district='national', frequency='monthly'."""
    if not period_to_value:
        return []
    sorted_recent_first = sorted(period_to_value.keys(), reverse=True)
    rows: list[dict] = []
    for idx, tp in enumerate(sorted_recent_first):
        rows.append({
            "series_id": str(series_id),
            "series_name": name_he,
            "topic": topic,
            "district": "national",
            "frequency": "monthly",
            "time_period": parse_period(tp).isoformat(),
            "value": period_to_value[tp],
            "is_provisional": idx < PROVISIONAL_TAIL,
            "is_derived": is_derived,
        })
    return rows


def upsert_rows(client: Client, rows: list[dict]) -> int:
    """Upsert rows on (topic, district, frequency, time_period). Returns
    count of rows sent."""
    sent = 0
    for start in range(0, len(rows), BATCH_SIZE):
        batch = rows[start : start + BATCH_SIZE]
        client.table(TABLE).upsert(
            batch, on_conflict="topic,district,frequency,time_period"
        ).execute()
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
    failures: list[str] = []

    # 1. Population total → addition
    try:
        pop_total = fetch_series(POPULATION_TOTAL_ID, session)
        addition = compute_population_addition(pop_total)
        all_rows.extend(to_db_rows(
            "population_addition", POPULATION_NAME, POPULATION_TOTAL_ID,
            is_derived=True, period_to_value=addition,
        ))
    except Exception as exc:
        log.exception("population_addition failed: %s", exc)
        failures.append("population_addition")

    # 2. Unemployment rate (splice)
    try:
        unemp_old = fetch_series(UNEMPLOYMENT_OLD_ID, session)
    except Exception as exc:
        log.exception("unemployment old fetch failed: %s", exc)
        unemp_old = {}
    try:
        unemp_new = fetch_series(UNEMPLOYMENT_NEW_ID, session)
    except Exception as exc:
        log.exception("unemployment new fetch failed: %s", exc)
        unemp_new = {}
    if unemp_old or unemp_new:
        spliced = splice_unemployment(unemp_old, unemp_new)
        # Use the NEW series id as the canonical series_id since it's
        # the active methodology going forward.
        all_rows.extend(to_db_rows(
            "unemployment_rate", UNEMPLOYMENT_NAME, UNEMPLOYMENT_NEW_ID,
            is_derived=True, period_to_value=spliced,
        ))
    else:
        failures.append("unemployment_rate")

    # 3. Average wage (raw)
    try:
        wage = fetch_series(AVERAGE_WAGE_ID, session)
        all_rows.extend(to_db_rows(
            "average_wage", WAGE_NAME, AVERAGE_WAGE_ID,
            is_derived=False, period_to_value=wage,
        ))
    except Exception as exc:
        log.exception("average_wage failed: %s", exc)
        failures.append("average_wage")

    if not all_rows:
        log.error("No rows to upsert (all topics failed)")
        return 1

    client = create_client(url, key)
    try:
        sent = upsert_rows(client, all_rows)
    except Exception as exc:
        log.exception("Upsert failed: %s", exc)
        return 1

    log.info(
        "Done. Upserted %d rows into %s across %d topics; failures=%d (%s)",
        sent, TABLE, 3 - len(failures), len(failures), failures or "—",
    )
    return 1 if failures else 0


if __name__ == "__main__":
    sys.exit(main())
