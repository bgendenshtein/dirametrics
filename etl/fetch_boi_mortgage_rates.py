"""Fetch Bank of Israel mortgage interest rates (ריביות משכנתא) and upsert into Supabase.

Source: Bank of Israel SDMX v2 API, dataflow BOI.STATISTICS:BIR_MRTG_99(1.0).
Target: Supabase table `boi_mortgage_rates`
        (series_id, series_name, is_indexed, rate_type, date, rate)
        UNIQUE (series_id, date).

Scope (decided after investigating the SDMX structure):
  Two series for newly-issued housing loans to households, total banking
  system, total across all maturities (INT_CHN_PER=A). BIR_MRTG_99 does
  not publish mortgage rates broken down by duration bucket via SDMX;
  only aggregate-across-maturities series exist.

  - BNK_99034_LR_BIR_MRTG_1492 : CPI-indexed, fixed rate
  - BNK_99034_LR_BIR_MRTG_467  : Not indexed,  fixed rate
"""

from __future__ import annotations

import logging
import os
import sys
from pathlib import Path

import requests
from dotenv import load_dotenv
from supabase import Client, create_client

DATAFLOW_URL = (
    "https://edge.boi.gov.il/FusionEdgeServer/sdmx/v2/data/dataflow/"
    "BOI.STATISTICS/BIR_MRTG_99/1.0/"
)
TABLE = "boi_mortgage_rates"
BATCH_SIZE = 500

# The dataflow has 14 dimensions; pinning only SERIES_CODE (position 1)
# and leaving the other 13 blank resolves to exactly one series per code.
NUM_DIMENSIONS = 14

SERIES = [
    {
        "code": "BNK_99034_LR_BIR_MRTG_1492",
        "name_he": "ריבית משכנתא צמודה קבועה",
        "is_indexed": True,
        "rate_type": "fixed",
    },
    {
        "code": "BNK_99034_LR_BIR_MRTG_467",
        "name_he": "ריבית משכנתא לא צמודה קבועה",
        "is_indexed": False,
        "rate_type": "fixed",
    },
]

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s %(levelname)s %(message)s",
)
log = logging.getLogger("boi_mortgage_rates")


def series_key(code: str) -> str:
    """Build the SDMX series key: SERIES_CODE in position 1, rest blank."""
    return ".".join([code] + [""] * (NUM_DIMENSIONS - 1))


def parse_month_to_date(period: str) -> str:
    """Convert SDMX TIME_PERIOD (typically 'YYYY-MM') to the first of the month."""
    if len(period) == 7 and period[4] == "-":
        return f"{period}-01"
    # Some endpoints return 'YYYY-MM-DD' directly; accept that too.
    if len(period) == 10 and period[4] == "-" and period[7] == "-":
        return period
    raise ValueError(f"Unexpected TIME_PERIOD format: {period!r}")


def fetch_series(code: str) -> list[tuple[str, float]]:
    """Fetch one BIR_MRTG series and return [(date_str, rate), ...] sorted ascending."""
    url = f"{DATAFLOW_URL}{series_key(code)}?format=sdmx-json"
    log.info("GET %s", url)
    resp = requests.get(url, timeout=60)
    resp.raise_for_status()
    payload = resp.json()

    data = payload["data"]
    structure = data["structures"][0] if "structures" in data else data["structure"]

    time_dim = next(
        d for d in structure["dimensions"]["observation"]
        if d["id"] in ("TIME_PERIOD", "TIME")
    )
    time_values = [
        v["id"] if isinstance(v, dict) and "id" in v else v["value"]
        for v in time_dim["values"]
    ]

    dataset_series = data["dataSets"][0]["series"]
    if not dataset_series:
        raise RuntimeError(f"No series returned for {code}")
    # Pinning SERIES_CODE yields exactly one series.
    _, series_obj = next(iter(dataset_series.items()))

    rows: list[tuple[str, float]] = []
    for obs_idx_str, obs_value in series_obj["observations"].items():
        raw = obs_value[0] if isinstance(obs_value, list) else obs_value
        if raw is None:
            continue
        rows.append((parse_month_to_date(time_values[int(obs_idx_str)]), float(raw)))

    rows.sort(key=lambda r: r[0])
    return rows


def upsert_rows(client: Client, rows: list[dict]) -> int:
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
            observations = fetch_series(series["code"])
        except Exception as exc:
            log.exception("Failed to fetch series %s: %s", series["code"], exc)
            return 1
        log.info(
            "Series %s (%s): fetched %d observations",
            series["code"], series["name_he"], len(observations),
        )
        for date_str, rate in observations:
            all_rows.append({
                "series_id": series["code"],
                "series_name": series["name_he"],
                "is_indexed": series["is_indexed"],
                "rate_type": series["rate_type"],
                "date": date_str,
                "rate": rate,
            })

    if not all_rows:
        log.warning("No rows fetched; nothing to upsert")
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
