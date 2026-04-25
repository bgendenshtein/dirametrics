"""Fetch CBS time-series data and upsert into Supabase.

Source: CBS time-series API (https://apis.cbs.gov.il/series/data/list).
Target: Supabase table `cbs_series`
        UNIQUE (topic, district, frequency, time_period).

Series fetched (see etl/NOTES_CBS.md for catalog provenance and the
code-mapping caveats — different topics use different geographic
coding schemes):
  - permits / starts / completions: monthly, national + 6 districts
  - active: quarterly, 6 districts (national derived as sum of districts)
  - new_sales_total / new_sales_subsidized / second_hand_sales: monthly,
    national + 6 districts
  - new_inventory: monthly, national only
  - new_sales_free: monthly, DERIVED = new_sales_total minus
    new_sales_subsidized, per (district, period)

API specifics:
  - Host is apis.cbs.gov.il (plural — distinct from api.cbs.gov.il/index/
    used by fetch_cbs_price_indices.py).
  - User-Agent header is mandatory; the API may silently reject requests
    without one.
  - We pin to data=1 (נתונים מקוריים, original) by series-id selection.
    Seasonal-adjusted (data=2) and trend (data=3) variants exist for some
    series but are not persisted.
  - Pagination: 100 obs per page; long histories need multiple pages.

Provisional semantics:
  Per (topic, district, frequency) the top 3 most-recent observations are
  flagged provisional. For DERIVED rows the flag propagates: if either
  source observation in the same period is provisional, the derived row
  is provisional too.

Failure handling:
  Individual series fetches that fail (network error, empty payload) are
  logged and accumulated; the run continues for the remaining series.
  Derivations whose sources are missing are skipped rather than producing
  partial/incorrect rows. The exit code reflects whether anything failed.
"""

from __future__ import annotations

import logging
import os
import sys
import time
from collections import defaultdict
from datetime import date
from pathlib import Path
from typing import Any

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
MAX_PAGES = 100  # safety cap to avoid runaway pagination loops
MAX_RETRIES = 3  # gap-recovery attempts after the initial sweep

# Lenient mode for stubborn residual gaps. After the initial paginated
# sweep, if we collected at least this fraction of `paging.total_items`
# but gap recovery still couldn't fill the residual, we persist the
# partial data with a WARNING instead of failing the series. The gate
# is on the *initial sweep* (not the final post-retry count) so that a
# series with deep coverage problems still fails loudly — only "near
# complete but the last few obs are stuck" passes through.
GAP_RECOVERY_MIN_COVERAGE = 0.95

# Per-request retry on transient HTTP/connection errors. Distinct from
# MAX_RETRIES (gap recovery) — this layer handles 5xx and dropped
# connections from a single GET; the gap-recovery layer above handles
# successful-but-incomplete responses.
HTTP_RETRY_ATTEMPTS = 3                       # retries after the initial attempt
HTTP_RETRY_BACKOFF_SEQUENCE = [2, 5, 15]      # seconds between attempts (must align with HTTP_RETRY_ATTEMPTS)
TRANSIENT_HTTP_STATUSES = {500, 502, 503, 504}
TRANSIENT_EXCEPTIONS = (
    requests.exceptions.ConnectionError,
    requests.exceptions.ChunkedEncodingError,
    requests.exceptions.Timeout,
)


class IncompleteSeriesError(RuntimeError):
    """Raised when a series can't be fully fetched after retries."""

DISTRICTS: list[str] = ["jerusalem", "north", "haifa", "center", "tel_aviv", "south"]

# Per-topic series-id mapping (data=1 / original; selection process
# documented in etl/NOTES_CBS.md). `national` is None where it is not
# available as a fetchable series and is derived instead.
TOPICS: dict[str, dict[str, Any]] = {
    "permits": {
        "frequency": "monthly",
        "name_he": "היתרי בנייה",
        "national": 574325,
        "districts": {
            "jerusalem": 553820, "north": 553821, "haifa": 553822,
            "center": 553823, "tel_aviv": 553824, "south": 553825,
        },
    },
    "starts": {
        "frequency": "monthly",
        "name_he": "התחלות בנייה",
        "national": 574272,
        "districts": {
            "jerusalem": 574273, "north": 574274, "haifa": 574275,
            "center": 574276, "tel_aviv": 574277, "south": 574278,
        },
    },
    "completions": {
        "frequency": "monthly",
        "name_he": "גמר בנייה",
        "national": 574280,
        "districts": {
            "jerusalem": 574281, "north": 574282, "haifa": 574283,
            "center": 574284, "tel_aviv": 574285, "south": 574286,
        },
    },
    "active": {
        # No quarterly national in the catalog — derived as sum of the
        # 6 districts. See derive_active_national().
        "frequency": "quarterly",
        "name_he": "בנייה פעילה",
        "national": None,
        "districts": {
            "jerusalem": 574090, "north": 574091, "haifa": 574092,
            "center": 574093, "tel_aviv": 574094, "south": 574095,
        },
    },
    "new_sales_total": {
        "frequency": "monthly",
        "name_he": "דירות חדשות שנמכרו (סך הכל)",
        "national": 574361,
        "districts": {
            "jerusalem": 574367, "north": 574368, "haifa": 574369,
            "center": 574370, "tel_aviv": 574371, "south": 574372,
        },
    },
    "new_sales_subsidized": {
        # Jerusalem uses 574341 (nid=5070 "ירושלים"), NOT 574340 (nid=7047
        # "מחוז ירושלים"). 574340 is a different/narrower geography with
        # incompatible values; using it would break the new_sales_free
        # derivation. See etl/NOTES_CBS.md.
        "frequency": "monthly",
        "name_he": "דירות חדשות שנמכרו בסבסוד ממשלתי",
        "national": 574354,
        "districts": {
            "jerusalem": 574341, "north": 574342, "haifa": 574343,
            "center": 574344, "tel_aviv": 574345, "south": 574346,
        },
    },
    "second_hand_sales": {
        # Uses the 7047-7052 coding scheme; new_sales_* topics use
        # 5070-5075. Same logical districts, distinct historical
        # boundaries — see etl/NOTES_CBS.md for the alignment caveat.
        "frequency": "monthly",
        "name_he": "דירות יד שנייה שנמכרו",
        "national": 574064,
        "districts": {
            "jerusalem": 574070, "north": 574071, "haifa": 574072,
            "center": 574073, "tel_aviv": 574074, "south": 574075,
        },
    },
    "new_inventory": {
        "frequency": "monthly",
        "name_he": "דירות חדשות שנותרו למכירה",
        "national": 574362,
        "districts": {},
    },
}

# Self-documenting Hebrew names for the two derived series.
DERIVED_NAMES = {
    "new_sales_free":
        "דירות חדשות שנמכרו בשוק החופשי (חישוב: סך הכל פחות סבסוד ממשלתי)",
    "active_national":
        "בנייה פעילה - סך כולל (חישוב: סכום ערכי 6 המחוזות)",
}

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s %(levelname)s %(message)s",
)
log = logging.getLogger("cbs_series")


def make_session() -> requests.Session:
    """Build a requests Session with the mandatory User-Agent header."""
    s = requests.Session()
    s.headers.update({"User-Agent": USER_AGENT})
    return s


def _advance_period(d: date, frequency: str) -> date:
    """Return the period date one step after `d`."""
    if frequency == "yearly":
        return date(d.year + 1, 1, 1)
    step = 3 if frequency == "quarterly" else 1
    m, y = d.month + step, d.year
    if m > 12:
        m -= 12
        y += 1
    return date(y, m, 1)


def _retreat_period(d: date, frequency: str) -> date:
    """Return the period date one step before `d`."""
    if frequency == "yearly":
        return date(d.year - 1, 1, 1)
    step = 3 if frequency == "quarterly" else 1
    m, y = d.month - step, d.year
    if m < 1:
        m += 12
        y -= 1
    return date(y, m, 1)


def _format_api_filter(d: date, frequency: str) -> str:
    """Format a period date for startperiod/endperiod params.

    The API uses 'MM-YYYY' here (note: NOT 'YYYY-MM' as in obs.TimePeriod).
    For quarterly, `d` is start-of-quarter; the filter wants the
    end-of-quarter month.
    """
    if frequency == "yearly":
        return f"01-{d.year:04d}"
    if frequency == "quarterly":
        return f"{d.month + 2:02d}-{d.year:04d}"
    return f"{d.month:02d}-{d.year:04d}"


def _find_gaps(
    period_dates: list[date], frequency: str
) -> list[tuple[date, date]]:
    """Detect interior gaps in a sorted-ascending list of period dates.

    Returns a list of (gap_start, gap_end) inclusive date pairs — the
    first and last missing periods within each gap. Edge gaps (before
    the earliest or after the latest collected period) are NOT detected
    here, because we have no way to bound them without total_items
    and a known full date range; in practice the CBS pagination quirk
    that motivates this helper produces interior gaps only.
    """
    gaps: list[tuple[date, date]] = []
    for i in range(len(period_dates) - 1):
        cur = period_dates[i]
        nxt = period_dates[i + 1]
        expected_next = _advance_period(cur, frequency)
        if nxt > expected_next:
            gap_end = _retreat_period(nxt, frequency)
            gaps.append((expected_next, gap_end))
    return gaps


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
    raised immediately without retry. On eventual success after one or
    more retries, logs an INFO line so transient flakiness can be
    distinguished from clean runs in the workflow logs. On final failure,
    logs a WARNING with the persistent error before re-raising.
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
                resp.raise_for_status()  # raises HTTPError -> caller surfaces it
            wait = HTTP_RETRY_BACKOFF_SEQUENCE[attempt]
            log.warning(
                "Series %s: HTTP %d on attempt %d/%d, retrying in %ds",
                series_id, resp.status_code, attempt + 1, total_tries, wait,
            )
            time.sleep(wait)
            continue

        # Non-transient status: success (2xx) or non-retriable error (4xx,
        # other 5xx). Let raise_for_status decide; only log "succeeded"
        # if we actually return cleanly.
        resp.raise_for_status()
        if attempt > 0:
            log.info(
                "Series %s: succeeded after %d retr%s",
                series_id, attempt, "y" if attempt == 1 else "ies",
            )
        return resp

    # Loop should always return or raise above; this is unreachable.
    raise RuntimeError(f"Series {series_id}: HTTP retry loop exited without return")


def _fetch_pages(
    series_id: int,
    session: requests.Session,
    rows_by_period: dict[str, dict],
    params_extra: dict | None = None,
) -> int | None:
    """Paginate /series/data/list and merge new TimePeriods into
    `rows_by_period` (keyed by TimePeriod string). First-occurrence wins
    for any duplicate TimePeriod, regardless of which page returned it.

    Returns the API's `paging.total_items` from the first page (the
    authoritative obs count for the queried scope), or None if the
    response shape is unexpected.
    """
    expected_total: int | None = None
    page = 1
    while page <= MAX_PAGES:
        params: dict[str, Any] = {
            "id": series_id,
            "format": "json",
            "download": "false",
            "Page": page,
            "PageSize": PAGE_SIZE,
        }
        if params_extra:
            params.update(params_extra)
        log.info(
            "GET id=%s page=%d%s",
            series_id, page,
            f" filter={params_extra}" if params_extra else "",
        )
        resp = _http_get_with_retry(session, API_BASE, params, series_id=series_id)
        payload = resp.json()

        dataset = payload.get("DataSet") or {}
        series_list = dataset.get("Series") or []
        if not series_list:
            log.warning("Series %s page %d: no Series object", series_id, page)
            break

        for obs in series_list[0].get("obs") or []:
            tp = obs.get("TimePeriod")
            v = obs.get("Value")
            if tp is None or v is None:
                continue
            if tp not in rows_by_period:
                rows_by_period[tp] = {"time_period": tp, "value": float(v)}

        paging = dataset.get("paging") or {}
        if expected_total is None:
            expected_total = paging.get("total_items")
        last_page = int(paging.get("last_page") or 1)
        if page >= last_page:
            break
        page += 1
    else:
        log.warning("Series %s: hit MAX_PAGES=%d safety cap", series_id, MAX_PAGES)
    return expected_total


def fetch_series(
    series_id: int, frequency: str, session: requests.Session
) -> list[dict]:
    """Fetch all observations for a CBS time-series, with gap recovery.

    The CBS time-series API has non-deterministic pagination — when page 1
    returns fewer than PageSize rows, the cursor for subsequent pages can
    drift, leaving interior gaps in the period sequence. We:

      1. Do an initial paginated sweep, collecting unique TimePeriods.
      2. If the unique count is short of `paging.total_items`, sort the
         collected periods, detect interior gaps, and re-query each gap
         with `startperiod`/`endperiod` (MM-YYYY format).
      3. If no interior gaps are visible but we're still short (rare;
         possible edge gap), do another full sweep to refresh.
      4. Repeat up to MAX_RETRIES times. If still short, raise
         IncompleteSeriesError — the caller should mark this series as
         failed and skip it rather than persist partial data.

    First-occurrence wins for duplicate TimePeriods; the API has been
    observed to return identical values across pages for the same period.
    """
    rows_by_period: dict[str, dict] = {}

    expected_total = _fetch_pages(series_id, session, rows_by_period, None)
    initial_count = len(rows_by_period)
    log.info(
        "Series %s initial sweep: %d unique obs, API total_items=%s",
        series_id, initial_count, expected_total,
    )

    if expected_total is None:
        return list(rows_by_period.values())
    expected_total = int(expected_total)

    try:
        for attempt in range(1, MAX_RETRIES + 1):
            if len(rows_by_period) >= expected_total:
                break
            before = len(rows_by_period)
            period_dates = sorted(
                parse_period(tp, frequency) for tp in rows_by_period
            )
            gaps = _find_gaps(period_dates, frequency)
            if gaps:
                log.warning(
                    "Series %s retry %d/%d: %d unique vs %d expected; %d interior gap(s)",
                    series_id, attempt, MAX_RETRIES,
                    len(rows_by_period), expected_total, len(gaps),
                )
                for gap_start, gap_end in gaps:
                    log.info(
                        "  gap %s..%s (%s)",
                        gap_start.isoformat(), gap_end.isoformat(), frequency,
                    )
                    _fetch_pages(series_id, session, rows_by_period, {
                        "startperiod": _format_api_filter(gap_start, frequency),
                        "endperiod":   _format_api_filter(gap_end,   frequency),
                    })
            else:
                log.warning(
                    "Series %s retry %d/%d: %d unique vs %d expected; no interior "
                    "gaps detected -- doing a full re-sweep to recover edge gap",
                    series_id, attempt, MAX_RETRIES,
                    len(rows_by_period), expected_total,
                )
                _fetch_pages(series_id, session, rows_by_period, None)

            log.info(
                "Series %s retry %d: gained %d (total %d/%d)",
                series_id, attempt,
                len(rows_by_period) - before, len(rows_by_period), expected_total,
            )
    except requests.exceptions.RequestException as exc:
        # Gap recovery hit a transient HTTP/connection error that
        # _http_get_with_retry couldn't recover from. If the initial
        # sweep was already at >= GAP_RECOVERY_MIN_COVERAGE, treat this
        # the same as "couldn't fill the gap" — log a WARNING and persist
        # what we have. Below threshold (or non-transient 4xx) re-raises.
        is_non_transient_4xx = (
            isinstance(exc, requests.exceptions.HTTPError)
            and exc.response is not None
            and 400 <= exc.response.status_code < 500
        )
        if is_non_transient_4xx:
            raise  # real bug (bad params / bad series id) — fail loud
        initial_coverage = initial_count / expected_total
        if initial_coverage < GAP_RECOVERY_MIN_COVERAGE:
            raise  # below threshold — preserve strict behavior
        final_coverage = len(rows_by_period) / expected_total
        log.warning(
            "Series %s: gap recovery failed with %s, but initial sweep had "
            "%.2f%% (>= %.0f%%) — accepting partial data (final=%.2f%%)",
            series_id, type(exc).__name__,
            initial_coverage * 100, GAP_RECOVERY_MIN_COVERAGE * 100,
            final_coverage * 100,
        )
        # Fall through to the post-loop block, which logs the gap detail
        # and the "exhausted; persisting partial data" summary.

    if len(rows_by_period) < expected_total:
        # Recompute residual gaps for diagnostics.
        period_dates = sorted(
            parse_period(tp, frequency) for tp in rows_by_period
        )
        residual_gaps = _find_gaps(period_dates, frequency)
        gap_summary = ", ".join(
            f"{a.isoformat()}..{b.isoformat()}" for a, b in residual_gaps[:5]
        ) or "no interior gaps (likely edge gap)"

        initial_coverage = initial_count / expected_total
        final_coverage = len(rows_by_period) / expected_total

        if initial_coverage >= GAP_RECOVERY_MIN_COVERAGE:
            # Lenient acceptance: initial sweep was near-complete; the
            # missing tail is transient API flakiness, not a structural
            # problem. Persist what we have, log loudly, but do not fail
            # the series.
            log.warning(
                "Series %s: gap recovery exhausted; persisting partial data. "
                "initial=%d/%d (%.2f%%), final=%d/%d (%.2f%%), "
                "min coverage=%.0f%%, residual gaps: %s",
                series_id,
                initial_count, expected_total, initial_coverage * 100,
                len(rows_by_period), expected_total, final_coverage * 100,
                GAP_RECOVERY_MIN_COVERAGE * 100,
                gap_summary,
            )
        else:
            raise IncompleteSeriesError(
                f"series {series_id} ({frequency}): collected "
                f"{len(rows_by_period)}/{expected_total} obs (initial sweep "
                f"{initial_count}, {initial_coverage * 100:.2f}%) — below "
                f"{GAP_RECOVERY_MIN_COVERAGE * 100:.0f}% min coverage threshold "
                f"after {MAX_RETRIES} gap-recovery retries. "
                f"Residual gaps: {gap_summary}"
            )
    else:
        log.info(
            "Series %s: %d obs (complete, matches API total_items)",
            series_id, len(rows_by_period),
        )
    # Return in API-style most-recent-first order (preserves the
    # provisional-tail logic in build_rows).
    return sorted(
        rows_by_period.values(),
        key=lambda r: r["time_period"],
        reverse=True,
    )


def parse_period(time_period: str, frequency: str) -> date:
    """Convert API TimePeriod string to a DATE.

    Monthly:   'YYYY-MM' -> first day of month.
    Quarterly: 'YYYY-MM' where MM is the END-of-quarter month
               (3, 6, 9, 12) -> first day of the quarter.
    Yearly:    'YYYY' or 'YYYY-MM' -> Jan 1 of YYYY.
    """
    parts = time_period.split("-")
    year = int(parts[0])
    month = int(parts[1]) if len(parts) > 1 else 1
    if frequency == "yearly":
        return date(year, 1, 1)
    if frequency == "quarterly":
        # 3->1 (Q1), 6->4 (Q2), 9->7 (Q3), 12->10 (Q4)
        return date(year, month - 2, 1)
    return date(year, month, 1)


def build_rows(
    topic: str,
    district: str,
    frequency: str,
    name_he: str,
    series_id: int,
    observations: list[dict],
) -> list[dict]:
    """Convert API observations to cbs_series row dicts.

    `observations` is most-recent-first as returned by fetch_series; the
    first PROVISIONAL_TAIL entries get is_provisional=true.
    """
    rows: list[dict] = []
    for idx, obs in enumerate(observations):
        period_date = parse_period(obs["time_period"], frequency)
        rows.append({
            "series_id": str(series_id),
            "series_name": name_he,
            "topic": topic,
            "district": district,
            "frequency": frequency,
            "time_period": period_date.isoformat(),
            "value": obs["value"],
            "is_provisional": idx < PROVISIONAL_TAIL,
            "is_derived": False,
        })
    return rows


def derive_new_sales_free(
    cache: dict[tuple[str, str], dict[date, dict]],
    failures: list[str],
) -> list[dict]:
    """new_sales_free = new_sales_total - new_sales_subsidized per
    (district, period). Provisional flag is OR of the two sources.

    Per-district independence: if either source is missing for a given
    district, that district is skipped — other districts still derive.
    """
    derived: list[dict] = []
    name_he = DERIVED_NAMES["new_sales_free"]
    keys = ["national"] + DISTRICTS
    for key in keys:
        total = cache.get(("new_sales_total", key), {})
        subsidized = cache.get(("new_sales_subsidized", key), {})
        if not total or not subsidized:
            msg = (
                f"new_sales_free/{key}: skipped (total obs={len(total)}, "
                f"subsidized obs={len(subsidized)})"
            )
            log.warning(msg)
            failures.append(msg)
            continue
        common = sorted(set(total) & set(subsidized))
        log.info("Deriving new_sales_free/%s: %d common periods", key, len(common))
        for period in common:
            t = total[period]
            s = subsidized[period]
            derived.append({
                "series_id": "DERIVED",
                "series_name": name_he,
                "topic": "new_sales_free",
                "district": key,
                "frequency": "monthly",
                "time_period": period.isoformat(),
                "value": float(t["value"] - s["value"]),
                "is_provisional": bool(t["is_provisional"] or s["is_provisional"]),
                "is_derived": True,
            })
    return derived


def derive_active_national(
    cache: dict[tuple[str, str], dict[date, dict]],
    failures: list[str],
) -> list[dict]:
    """Quarterly active national = sum of the 6 districts.

    Requires ALL 6 districts to be present (otherwise the sum is not a
    legitimate national total). Per-period: a quarter is included only
    if all 6 districts have a value for that quarter.
    """
    district_caches = {d: cache.get(("active", d), {}) for d in DISTRICTS}
    missing = [d for d, c in district_caches.items() if not c]
    if missing:
        msg = f"active/national derivation skipped: missing districts={missing}"
        log.warning(msg)
        failures.append(msg)
        return []

    all_periods = set()
    for c in district_caches.values():
        all_periods.update(c.keys())

    derived: list[dict] = []
    name_he = DERIVED_NAMES["active_national"]
    skipped = 0
    for period in sorted(all_periods):
        rows = [district_caches[d].get(period) for d in DISTRICTS]
        if not all(rows):
            skipped += 1
            continue
        total_value = sum(float(r["value"]) for r in rows)
        is_prov = any(bool(r["is_provisional"]) for r in rows)
        derived.append({
            "series_id": "DERIVED",
            "series_name": name_he,
            "topic": "active",
            "district": "national",
            "frequency": "quarterly",
            "time_period": period.isoformat(),
            "value": total_value,
            "is_provisional": is_prov,
            "is_derived": True,
        })
    log.info(
        "Derived active/national: %d quarters (%d skipped for missing district values)",
        len(derived), skipped,
    )
    return derived


def fetch_topic(
    topic: str,
    cfg: dict[str, Any],
    session: requests.Session,
    cache: dict[tuple[str, str], dict[date, dict]],
    failures: list[str],
) -> list[dict]:
    """Fetch all (national + per-district) series for a topic.

    Populates cache with parsed rows keyed by (topic, district) -> {date:
    row}. Returns the same rows as a flat list for batched upsert.
    """
    out: list[dict] = []
    targets: list[tuple[str, int | None]] = []
    if cfg["national"] is not None:
        targets.append(("national", cfg["national"]))
    for district in DISTRICTS:
        sid = cfg["districts"].get(district)
        if sid is not None:
            targets.append((district, sid))

    for district, sid in targets:
        try:
            obs = fetch_series(sid, cfg["frequency"], session)
        except IncompleteSeriesError as exc:
            msg = f"{topic}/{district} (id={sid}): {exc}"
            log.error(msg)
            failures.append(msg)
            continue
        except Exception as exc:
            msg = f"{topic}/{district} (id={sid}): fetch failed: {exc}"
            log.exception(msg)
            failures.append(msg)
            continue
        if not obs:
            msg = f"{topic}/{district} (id={sid}): no observations returned"
            log.warning(msg)
            failures.append(msg)
            continue
        rows = build_rows(topic, district, cfg["frequency"], cfg["name_he"], sid, obs)
        out.extend(rows)
        bucket = cache[(topic, district)]
        for r in rows:
            bucket[date.fromisoformat(r["time_period"])] = r
    return out


def find_and_dedupe_duplicates(rows: list[dict]) -> list[dict]:
    """Group rows by the upsert conflict key and report any duplicates.

    Postgres rejects ON CONFLICT DO UPDATE when the same conflict key
    appears multiple times in one statement, so we must dedupe before
    upserting. This function logs every duplicate group with full row
    detail (so we can diagnose where they came from) and then returns a
    deduped list — last-write-wins, mirroring ON CONFLICT DO UPDATE
    semantics.
    """
    by_key: dict[tuple, list[dict]] = {}
    for r in rows:
        key = (r["topic"], r["district"], r["frequency"], r["time_period"])
        by_key.setdefault(key, []).append(r)

    dup_groups = {k: v for k, v in by_key.items() if len(v) > 1}
    if not dup_groups:
        log.info("Pre-upsert dedup check: %d rows, no duplicates", len(rows))
        return rows

    log.warning(
        "Pre-upsert dedup check: %d duplicate key group(s) among %d rows",
        len(dup_groups), len(rows),
    )
    for key, group in sorted(dup_groups.items()):
        log.warning("  DUP key=%s -- %d rows:", key, len(group))
        for r in group:
            log.warning(
                "    series_id=%s  value=%s  is_provisional=%s  is_derived=%s  "
                "series_name=%r",
                r["series_id"], r["value"], r["is_provisional"],
                r["is_derived"], r["series_name"],
            )

    # Last-write-wins: dict insertion in iteration order, later writes
    # replace the value for an existing key (and we don't care about row
    # order for upsert correctness).
    key_to_row: dict[tuple, dict] = {}
    for r in rows:
        key = (r["topic"], r["district"], r["frequency"], r["time_period"])
        key_to_row[key] = r
    deduped = list(key_to_row.values())
    log.warning(
        "Removed %d duplicate row(s) (%d -> %d)",
        len(rows) - len(deduped), len(rows), len(deduped),
    )
    return deduped


def upsert_rows(client: Client, rows: list[dict]) -> int:
    """Upsert in batches on (topic, district, frequency, time_period)."""
    sent = 0
    for start in range(0, len(rows), BATCH_SIZE):
        batch = rows[start : start + BATCH_SIZE]
        client.table(TABLE).upsert(
            batch,
            on_conflict="topic,district,frequency,time_period",
        ).execute()
        sent += len(batch)
        log.info("Upserted batch %d-%d (%d total)", start, start + len(batch), sent)
    return sent


def main() -> int:
    load_dotenv(Path(__file__).parent / ".env")

    url = os.environ.get("SUPABASE_URL")
    key = os.environ.get("SUPABASE_SERVICE_ROLE_KEY")
    if not url or not key:
        log.error("SUPABASE_URL and SUPABASE_SERVICE_ROLE_KEY must be set")
        return 2

    session = make_session()
    cache: dict[tuple[str, str], dict[date, dict]] = defaultdict(dict)
    failures: list[str] = []
    all_rows: list[dict] = []

    for topic, cfg in TOPICS.items():
        log.info("=== Topic: %s (%s) ===", topic, cfg["frequency"])
        topic_rows = fetch_topic(topic, cfg, session, cache, failures)
        log.info("Topic %s: %d rows fetched", topic, len(topic_rows))
        all_rows.extend(topic_rows)

    # Derivations (run regardless of upstream failures; each derivation
    # checks its own preconditions and skips if sources are missing).
    all_rows.extend(derive_new_sales_free(cache, failures))
    all_rows.extend(derive_active_national(cache, failures))

    if not all_rows:
        log.error("No rows produced; nothing to upsert")
        return 1

    all_rows = find_and_dedupe_duplicates(all_rows)

    client = create_client(url, key)
    try:
        sent = upsert_rows(client, all_rows)
    except Exception as exc:
        log.exception("Upsert failed: %s", exc)
        return 1

    log.info("Done. Upserted %d rows into %s.", sent, TABLE)
    if failures:
        log.warning("Run completed with %d failure(s)/skip(s):", len(failures))
        for f in failures:
            log.warning("  - %s", f)
        return 1
    return 0


if __name__ == "__main__":
    sys.exit(main())
