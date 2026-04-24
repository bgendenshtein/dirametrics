"""Fetch Bank of Israel base interest rate (ריבית בנק ישראל) and upsert into Supabase.

Source: Bank of Israel SDMX v2 API, dataflow BOI.STATISTICS:BR(1.0), series 0:0:0.
Target: Supabase table `boi_base_rate` (date DATE UNIQUE, rate NUMERIC).
"""

from __future__ import annotations

import logging
import os
import sys
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

    client = create_client(url, key)
    try:
        sent = upsert_rows(client, rows)
    except Exception as exc:
        log.exception("Upsert failed: %s", exc)
        return 1

    log.info("Done. Fetched %d rows, upserted %d rows into %s", len(rows), sent, TABLE)
    return 0


if __name__ == "__main__":
    sys.exit(main())
