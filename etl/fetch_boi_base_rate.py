"""Fetch Bank of Israel base interest rate (ריבית בנק ישראל) and upsert into Supabase.

Source: Bank of Israel SDMX v2 API, dataflow BOI.STATISTICS:BR(1.0), series 0:0:0.
Target: Supabase table `boi_base_rate` (date DATE UNIQUE, rate NUMERIC).

The SDMX feed publishes the BoI base rate at daily granularity (~12K rows
back to 1994). The dashboard's minimum chart resolution is monthly, so
storing daily values 30× the rows we need and trips Supabase's 1000-row
default fetch limit on the consumer side. We aggregate to monthly here,
storing one row per month with date = last calendar day of the month and
rate = the rate in effect on that day.

Because the BoI base rate is a step function (changes only on Monetary
Committee decisions), the rate "in effect" on month-end is just the most
recent prior decision's rate. Months in which no decision happened still
get a row, carrying the prior month's rate forward.

Before re-running on an existing daily-data table, TRUNCATE boi_base_rate.
The (date, rate) UNIQUE constraint on date will otherwise leave the old
mid-month rows untouched alongside the new month-end rows.
"""

from __future__ import annotations

import calendar
import logging
import os
import sys
from datetime import date as Date
from pathlib import Path

import requests
from dotenv import load_dotenv
from supabase import Client, create_client

SDMX_URL = (
    "https://edge.boi.gov.il/FusionEdgeServer/sdmx/v2/data/dataflow/"
    "BOI.STATISTICS/BR/1.0/?format=sdmx-json"
)
TABLE = "boi_base_rate"
BATCH_SIZE = 500

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s %(levelname)s %(message)s",
)
log = logging.getLogger("boi_base_rate")


def fetch_observations() -> list[dict]:
    """Fetch observations from BoI SDMX and return [{date, rate}, ...]."""
    log.info("Fetching %s", SDMX_URL)
    resp = requests.get(SDMX_URL, timeout=60)
    resp.raise_for_status()
    payload = resp.json()

    # SDMX-JSON 2.0 structure: data.dataSets[0].series["0:0:0"].observations
    # and data.structure.dimensions.observation[*] holds the TIME_PERIOD values
    # in the same index order used as observation keys.
    data = payload["data"]
    structure = data["structures"][0] if "structures" in data else data["structure"]

    obs_dims = structure["dimensions"]["observation"]
    time_dim = next(d for d in obs_dims if d["id"] in ("TIME_PERIOD", "TIME"))
    time_values = [v["id"] if isinstance(v, dict) and "id" in v else v["value"]
                   for v in time_dim["values"]]

    dataset = data["dataSets"][0]
    series = dataset["series"]
    if not series:
        raise RuntimeError("SDMX response contained no series")

    # There is one series in this dataflow; take the first.
    series_key, series_obj = next(iter(series.items()))
    log.info("Series key: %s", series_key)
    observations = series_obj["observations"]

    rows: list[dict] = []
    for obs_idx_str, obs_value in observations.items():
        idx = int(obs_idx_str)
        date = time_values[idx]
        # Some SDMX periods come back as "YYYY-MM-DD" already; keep as-is if so.
        raw_rate = obs_value[0] if isinstance(obs_value, list) else obs_value
        if raw_rate is None:
            continue
        rows.append({"date": date, "rate": float(raw_rate)})

    rows.sort(key=lambda r: r["date"])
    log.info("Parsed %d observations", len(rows))
    return rows


def aggregate_to_monthly(observations: list[dict]) -> list[dict]:
    """Collapse daily observations into one row per month at end-of-month.

    Walks every month from the first observation's month through the last
    observation's month. For each month, finds the most recent observation
    on or before the last calendar day of that month and emits a row with
    date = last-day-of-month, rate = that observation's rate.

    A month with no decisions in it carries forward the previous month's
    rate (the BoI base rate is a step function — between decisions the
    rate is unchanged, so end-of-month equals the most recent decision).

    Months before the first observation are skipped entirely (no prior
    rate to carry); months after the last observation are not emitted
    either (we don't extrapolate into the future).
    """
    if not observations:
        return []

    parsed: list[tuple[Date, float]] = []
    for r in observations:
        try:
            d = Date.fromisoformat(r["date"])
        except ValueError:
            log.warning("Skipping unparseable date: %r", r["date"])
            continue
        parsed.append((d, float(r["rate"])))
    if not parsed:
        return []
    parsed.sort(key=lambda x: x[0])

    first = parsed[0][0]
    last = parsed[-1][0]

    out: list[dict] = []
    obs_idx = 0
    last_rate: float | None = None

    cur_year = first.year
    cur_month = first.month
    end_year = last.year
    end_month = last.month

    while (cur_year, cur_month) <= (end_year, end_month):
        last_day = calendar.monthrange(cur_year, cur_month)[1]
        eom = Date(cur_year, cur_month, last_day)
        # Advance through observations whose date <= eom; carry the rate
        # of the most recent one as the month-end value. Any prior
        # carry-forward survives if no new decision happens this month.
        while obs_idx < len(parsed) and parsed[obs_idx][0] <= eom:
            last_rate = parsed[obs_idx][1]
            obs_idx += 1
        if last_rate is not None:
            out.append({"date": eom.isoformat(), "rate": last_rate})
        if cur_month == 12:
            cur_year += 1
            cur_month = 1
        else:
            cur_month += 1

    log.info(
        "Aggregated %d daily observations to %d monthly rows (%s..%s)",
        len(parsed),
        len(out),
        out[0]["date"] if out else "n/a",
        out[-1]["date"] if out else "n/a",
    )
    return out


def upsert_rows(client: Client, rows: list[dict]) -> int:
    """Upsert rows on `date`. Returns count of rows sent."""
    sent = 0
    for start in range(0, len(rows), BATCH_SIZE):
        batch = rows[start : start + BATCH_SIZE]
        client.table(TABLE).upsert(batch, on_conflict="date").execute()
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

    try:
        rows = fetch_observations()
    except Exception as exc:
        log.exception("Failed to fetch BoI data: %s", exc)
        return 1

    if not rows:
        log.warning("No rows fetched; nothing to upsert")
        return 0

    monthly = aggregate_to_monthly(rows)
    if not monthly:
        log.warning("Aggregation produced no rows; nothing to upsert")
        return 0

    client = create_client(url, key)
    try:
        sent = upsert_rows(client, monthly)
    except Exception as exc:
        log.exception("Upsert failed: %s", exc)
        return 1

    log.info(
        "Done. Fetched %d daily rows, aggregated to %d monthly rows, upserted %d into %s",
        len(rows),
        len(monthly),
        sent,
        TABLE,
    )
    return 0


if __name__ == "__main__":
    sys.exit(main())
