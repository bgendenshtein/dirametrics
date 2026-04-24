"""Fetch CBS price indices and upsert into Supabase.

Source: CBS price-index API (https://api.cbs.gov.il/index/data/price).
Target: Supabase table `cbs_price_indices`
        (series_id, date, value, is_provisional) UNIQUE (series_id, date).

Series fetched (see etl/NOTES_CBS.md for provenance and cross-checks):
  - 40010  : מדד מחירי דירות (housing prices)
  - 120460 : מדד שכר דירה (rent — private, public, long-term regulated)
  - 120010 : מדד המחירים לצרכן - כללי (CPI general)

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
from pathlib import Path

import requests
from dotenv import load_dotenv
from supabase import Client, create_client

API_BASE = "https://api.cbs.gov.il/index/data/price"
TABLE = "cbs_price_indices"
BATCH_SIZE = 500
PROVISIONAL_TAIL = 3

# Series to fetch. Each entry's `id` is the CBS codeId used by
# /data/price?id=...; `name` is stored in the `series_name` column.
# See etl/NOTES_CBS.md for how these were identified.
SERIES = [
    {"id": 40010,  "name": "מדד מחירי דירות"},
    {"id": 120460, "name": "מדד שכר דירה"},
    {"id": 120010, "name": "מדד המחירים לצרכן - כללי"},
]

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s %(levelname)s %(message)s",
)
log = logging.getLogger("cbs_price_indices")


def fetch_series(series_id: int) -> list[dict]:
    """Fetch all paginated observations for a CBS series.

    Returns a list of {year, month, value} dicts ordered most-recent first
    (preserving API order). Callers can rely on that order for provisional
    marking.
    """
    rows: list[dict] = []
    page = 1
    while True:
        params = {
            "id": series_id,
            "format": "json",
            "Page": page,
            "PageSize": 100,
        }
        log.info("GET %s page=%d", API_BASE, page)
        resp = requests.get(API_BASE, params=params, timeout=60)
        resp.raise_for_status()
        payload = resp.json()

        # `month` is a list with (usually) a single element whose `date`
        # field holds the observations for this page.
        month_buckets = payload.get("month") or []
        for bucket in month_buckets:
            for obs in bucket.get("date", []) or []:
                value = (obs.get("currBase") or {}).get("value")
                year = obs.get("year")
                month = obs.get("month")
                if value is None or year is None or month is None:
                    continue
                rows.append({"year": int(year), "month": int(month), "value": float(value)})

        paging = payload.get("paging") or {}
        last_page = paging.get("last_page") or 1
        if page >= last_page:
            break
        page += 1

    log.info("Series %s: fetched %d observations", series_id, len(rows))
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

    all_rows: list[dict] = []
    for series in SERIES:
        try:
            observations = fetch_series(series["id"])
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
