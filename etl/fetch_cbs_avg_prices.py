"""Fetch CBS average apartment prices, splice legacy + current
methodology, and compute the monthly affordability index.

PRICE DATA SOURCE — CBS Boards Generator (the public time-series and
price-index APIs do not expose this data; the headline "average new
apartment price" lives only inside the Boards Generator's web app).

  Endpoint: POST https://boardsgenerator.cbs.gov.il/Handlers/Prices/GridHandler.ashx?mode=Init

  Anti-bot signature required for the response to contain rows:
    - Form-encoded body (NOT JSON), with the JSON payload URL-encoded
      inside a `query` field.
    - X-TS-AJAX-Request: true header.
    - Referer matching the WizardPage URL.
    - Session cookies obtained via initial GET on the WizardPage.

  Two CBS series feed the spliced topic:
    - 51000 (subjectId=165): current methodology, 2017-Q1 → present.
                              Quarterly, in thousands of new shekels.
    - 20000 (subjectId=7):   legacy methodology, 1983-Q1 → 2017-Q4.
                              Quarterly, with mixed unit labels:
                                pre-2003: 'שקלים חדשים'    (raw NIS)
                                2003+:    'אלפי שקלים חדשים' (thousands NIS)
                              Normalize at parse time to a single scale.

  Splice approach: factor from 2017 yearly overlap.
    factor = mean(51000 in 2017) / mean(20000 in 2017)
    Multiply 20000 values from 1983-Q1 through 2016-Q4 by factor.
    Use 51000 as-is for 2017+ (current methodology preferred).

  Storage: rows in cbs_series with topic='avg_apartment_price',
  district='national', frequency='quarterly', value in raw NIS (so the
  chart's K/M tick formatter renders ~1.5M apartments as "1.5M" without
  needing to know the unit was thousands at source).

AFFORDABILITY INDEX — computed after the price series is built.

  Two regimes:
    - 2011-07 onwards: real-data calculation, monthly resolution.
    - 2008-01 to 2011-06: ESTIMATED via regression on BoI's 7-year
      nominal zero-coupon government bond yield. Annual resolution
      (one estimated value per year, expanded to 12 monthly rows).
      Marked is_estimated=true so the chart renders the segment as
      a dashed line.

  Real-data inputs (2011-07+):
    - avg_apartment_price (quarterly, NIS), from the spliced price
      series above.
    - average_wage (monthly, NIS), from cbs_series topic='average_wage'.
    - mortgage rate (monthly, %), from boi_mortgage_rates with
      rate_type='fixed' AND is_indexed=false.

  For each month t with all three inputs available:
    quarter = the quarter containing t (Q1=Jan-Mar, etc.)
    price = avg_apartment_price[quarter]                  (NIS)
    loan = 0.70 × price                                   (NIS)
    monthly_rate = mortgage_rate / 100 / 12               (decimal)
    n = 25 × 12 = 300                                     (months)
    monthly_payment = loan × (mr × (1+mr)^n) / ((1+mr)^n - 1)
    net_household_income = 2 × wage × 0.78                (NIS, post-tax estimate)
    affordability = monthly_payment / net_household_income × 100    (%)

  The 0.78 multiplier converts the pre-tax wage to an estimate of
  post-tax disposable income for a two-earner household at the wage
  average — see WAGE_TO_NET_HOUSEHOLD_MULTIPLIER below for the
  empirical anchoring.

  Step-function on price: each month within a quarter uses the same
  price level. The step appears as small visible jumps in the
  affordability series at quarter boundaries. This avoids inventing
  monthly precision in price data CBS only publishes quarterly.

  Estimated regime (2008-2010, plus 2011 H1):
    BoI's machine-readable mortgage-rate feed only starts Jul 2011.
    For pre-Jul-2011 we estimate the mortgage rate from the 7-year
    nominal yield via:

        mortgage_rate = α + β × yield_7y

    Coefficients α, β are refit each ETL run on the post-Nov-2018
    sub-sample (where the relationship is tightest, R²≈0.92 vs ~0.65
    for 2011-2018). See methodology page for the rationale on why
    we use a recent-period regression rather than a full-sample one.

    For each year in 2008-2010:
      annual_yield  = mean of monthly 7Y yields in that year
      est_mortgage  = α + β × annual_yield
      annual_price  = mean of quarterly avg_apartment_price values
      annual_wage   = mean of monthly average_wage values
      affordability = standard formula above, applied at annual scale
    The same affordability value is written to all 12 months of the
    year (with is_estimated=true). 2011-Jan to 2011-Jun also fall in
    the estimated regime since real mortgage data starts Jul 2011;
    those 6 months use the 2011 annual estimate.

  Storage: rows in cbs_series with topic='affordability_index',
  district='national', frequency='monthly', value in % of household
  income for mortgage payment. is_estimated flag distinguishes the
  two regimes; chart engine applies dashed styling to estimated
  segments.

  Coverage: 2008-01 (limited by BoI 7Y yield start) through latest
  monthly mortgage observation.

HOUSING ALTERNATIVE RATIO — third derived series, also computed here.

  Compares the cost of renting vs. buying as a quarterly cash-flow
  ratio:

      housing_alternative_ratio = (avg_monthly_rent / monthly_mortgage_payment) × 100

  Inputs (all quarterly aligned at quarter-start):
    - avg_monthly_rent: arithmetic mean of cbs_rent_prices for
      geography_type='national', geography='national',
      room_group IN ('3.5-4', '4.5-6'). The two larger room-group
      categories are averaged to match the typical size of new
      apartments in the spliced apartment-price series — using
      the 'all' bucket would mix in 1-2 and 2.5-3 rentals and
      drag the rent down, making the ratio look artificially low.
      The table stores quarterly only (no frequency column).
      Ingested separately by etl/ingest_rent_data.py.
    - monthly_mortgage_payment: same standard amortization formula
      used for affordability (70% LTV, 25-year, non-indexed fixed
      rate). Mortgage rate sourced from boi_mortgage_rates at the
      first month of each quarter.

  Interpretation: <100% means renting is cheaper than the mortgage
  cash flow on a comparable apartment; >100% means renting is more
  expensive. Pure cash-flow comparison — does not account for
  property tax, maintenance, foregone interest on down payment,
  capital appreciation, or eventual ownership.

  Storage: rows in cbs_series with topic='housing_alternative_ratio',
  district='national', frequency='quarterly'. Coverage 2017+ (limited
  by rent data start).

DB CONSTRAINT REMINDER: cbs_series_topic_valid must include all four
topic strings ('avg_apartment_price', 'affordability_index',
'housing_alternative_ratio'; the rent prices live in their own
table cbs_rent_prices, no constraint there). See etl/NOTES_CBS.md.

Failure handling: each step (price fetch / splice / affordability /
housing alternative) runs independently. If one fails, the others
still upsert.
"""

from __future__ import annotations

import json
import logging
import math
import os
import sys
from datetime import date
from pathlib import Path
from urllib.parse import urlencode

import requests
from dotenv import load_dotenv
from supabase import Client, create_client

# --- Constants ---

WIZARD_URL = 'https://boardsgenerator.cbs.gov.il/pages/Prices/WizardPage.aspx?r='
HANDLER_URL = 'https://boardsgenerator.cbs.gov.il/Handlers/Prices/GridHandler.ashx?mode=Init'
TABLE = 'cbs_series'
BATCH_SIZE = 500
PROVISIONAL_TAIL = 3
DEFAULT_TIMEOUT = 60

# Browser-style UA so the boards-generator's anti-bot middleware
# accepts the request. Generic Python UAs get silently rejected
# (200 OK + empty body).
USER_AGENT = (
    'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 '
    '(KHTML, like Gecko) Chrome/130.0.0.0 Safari/537.36'
)

# Series IDs and their subject classifications (different sub-tree
# per series; investigation in chat → 2026-05-01).
CURRENT_PRICE_ID = 51000
CURRENT_PRICE_SUBJECT = '165'   # מחירים ממוצעים של דירות לפי מחוזות (2017+)
LEGACY_PRICE_ID = 20000
LEGACY_PRICE_SUBJECT = '7'      # מחירים ממוצעים של דירות בבעלות הדיירים (1983-2017)

PRICE_NAME = 'מחיר דירה ממוצעת'
AFFORDABILITY_NAME = 'מדד אפורדביליות'
HOUSING_ALT_NAME = 'יחס שכירות למשכנתא'

# Mortgage parameters for the affordability calculation.
LTV = 0.70                       # 70% loan-to-value
TERM_MONTHS = 25 * 12            # 25-year term

# Empirical conversion from pre-tax wage to post-tax disposable income
# for a two-earner household at the wage average. Combines income tax,
# National Insurance (Bituach Leumi), health tax (Mas Briut), and other
# payroll deductions. Anchored against the CBS Household Expenditure
# Survey: 2022 average net household income of ≈18,237 NIS for a
# typical 2-earner-at-avg-wage gross of ≈24,000 NIS gives a 76-78%
# net-to-gross ratio. Stable to ±2pp across 2008-2026 — the Trajtenberg
# 2017 reforms trimmed marginal rates modestly but the effective rate
# at this income band has stayed in the 18-22% deduction band
# throughout. See methodology page for the derivation.
WAGE_TO_NET_HOUSEHOLD_MULTIPLIER = 0.78

# Estimation window for pre-Jul-2011 mortgage rates.
ESTIMATED_FIRST_YEAR = 2008      # earliest year of BoI yield data
ESTIMATED_THROUGH = '2011-06'    # last estimated month (mortgage data starts 2011-07)
REGRESSION_FIT_FROM = '2018-11'  # post-2018 regression sub-sample (R²≈0.92)

# BoI SDMX endpoint for 7-year nominal zero-coupon yield curve.
BOI_SDMX_BASE = 'https://edge.boi.gov.il/FusionEdgeServer/sdmx/v2/data/dataflow/BOI.STATISTICS/ZCM/1.0'
BOI_YIELD_7Y_CODE = 'ZC_TSB_ZND_07Y_MA'

# Year ranges to request from CBS. Narrow enough to avoid unnecessary
# data, wide enough to allow CBS to add years before we'd notice.
CURRENT_FYEAR = 2017
CURRENT_TYEAR = date.today().year + 1
LEGACY_FYEAR = 1983
LEGACY_TYEAR = 2017

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s %(levelname)s %(message)s',
)
log = logging.getLogger('cbs_avg_prices')


# --- Boards-generator session ---

def make_session() -> requests.Session:
    """Warm up a session with the boards-generator's WizardPage to
    obtain the cookies (AuthToken, ASP.NET_SessionId, the TS* Akamai
    cookies) that the GridHandler validates on every request."""
    s = requests.Session()
    s.headers.update({
        'User-Agent': USER_AGENT,
        'Accept-Language': 'he,en-US;q=0.9,en;q=0.8',
    })
    r = s.get(WIZARD_URL, timeout=DEFAULT_TIMEOUT)
    r.raise_for_status()
    log.info('session warmed up; cookies=%s', sorted(s.cookies.keys()))
    return s


def fetch_grid(
    sess: requests.Session,
    series_code: int,
    subject_id: str,
    fyear: int,
    tyear: int,
) -> list[dict]:
    """POST to GridHandler and return the parsed `data` rows.

    Body must be form-encoded with the JSON payload nested in a
    `query` field. The empty-string `mode` form field is harmless;
    the URL's `?mode=Init` is what the server reads. Returns the
    list of year-rows (each contains Q1/Q2/Q3/Q4 quarterly values
    for that year)."""
    payload = {
        'model': None,
        'dataTypeId': '2',
        'subjectId': subject_id,
        'SeriesCodes': [series_code],
        'Fyear': fyear,
        'Tyear': tyear,
        'IndicesTypeOption': 'Prices',
        'Language': 'Hebrew',
        'Fquarter': 1,
        'Tquarter': 4,
    }
    form_body = {
        'query': json.dumps(payload, ensure_ascii=False),
        'mode': 'Init',
    }
    headers = {
        'Content-Type': 'application/x-www-form-urlencoded; charset=UTF-8',
        'Accept': 'application/json, text/javascript, */*; q=0.01',
        'X-TS-AJAX-Request': 'true',
        'X-Requested-With': 'XMLHttpRequest',
        'Origin': 'https://boardsgenerator.cbs.gov.il',
        'Referer': WIZARD_URL,
    }
    r = sess.post(
        HANDLER_URL,
        data=urlencode(form_body, encoding='utf-8'),
        headers=headers,
        timeout=DEFAULT_TIMEOUT,
    )
    r.raise_for_status()
    if not r.text:
        # 200 OK with empty body = anti-bot rejection. Fail loudly.
        raise RuntimeError(
            f'GridHandler returned empty body for series {series_code}; '
            'likely missing/incorrect headers or stale session.'
        )
    data = r.json()
    rows = data.get('data') or []
    log.info(
        'grid %s (subject=%s, %s-%s): %d year-rows',
        series_code, subject_id, fyear, tyear, len(rows),
    )
    return rows


# --- Parsing + unit normalization ---

def _to_float(s) -> float | None:
    """Parse comma-separated string ('1,531.5') to float. None or
    empty string returns None so callers can skip absent quarters."""
    if s is None or s == '':
        return None
    if isinstance(s, (int, float)):
        return float(s)
    return float(str(s).replace(',', ''))


def normalize_grid_rows(rows: list[dict]) -> dict[str, float]:
    """Convert grid rows (one per year, with Q1-Q4 fields) to a flat
    {quarter-start-date: value_in_NIS} dict.

    Unit handling: the legacy 20000 series labels its rows
    'שקלים חדשים' for 1983-2002 (raw NIS) and 'אלפי שקלים חדשים' for
    2003+ (thousands NIS). The current 51000 series is uniformly the
    latter. Multiply by 1000 when the source label says "thousands"
    so the stored value is in raw NIS regardless of source convention.
    A 2017 apartment thus stores as ~1,500,000 (NIS), which the chart
    engine's formatTickKM renders naturally as "1.5M"."""
    out: dict[str, float] = {}
    for row in rows:
        currency = row.get('Currency') or ''
        # Source unit → raw NIS multiplier
        if currency == 'אלפי שקלים חדשים':
            multiplier = 1000.0  # thousands → NIS
        elif currency == 'שקלים חדשים':
            multiplier = 1.0     # already NIS
        else:
            log.warning('row currency=%r not recognized; assuming raw NIS', currency)
            multiplier = 1.0
        year = int(row['Year'])
        for q_field, q_month in [('Q1', 1), ('Q2', 4), ('Q3', 7), ('Q4', 10)]:
            v = _to_float(row.get(q_field))
            if v is None:
                continue
            key = f'{year:04d}-{q_month:02d}-01'
            out[key] = v * multiplier
    return out


# --- Splicing ---

def compute_price_splice(
    legacy: dict[str, float],
    current: dict[str, float],
) -> dict[str, float]:
    """Splice legacy 20000 (1983-2017) into current 51000 (2017+).

    Splice factor = mean(51000_2017) / mean(20000_2017). Multiply
    legacy values from 1983-Q1 through 2016-Q4 by factor; for 2017
    Q1-Q4, prefer the current-methodology values (51000) since they
    represent the active definition going forward.

    Empirically the factor is ~1.04 — the new survey reads about 4%
    higher than the legacy survey in the overlap year.
    """
    if not current:
        log.warning('price splice: current series empty; returning legacy only')
        return legacy or {}
    if not legacy:
        log.warning('price splice: legacy series empty; returning current only')
        return current

    legacy_2017 = [v for k, v in legacy.items() if k.startswith('2017-')]
    current_2017 = [v for k, v in current.items() if k.startswith('2017-')]
    if not legacy_2017 or not current_2017:
        log.warning(
            'price splice: missing 2017 overlap (legacy=%d, current=%d); '
            'returning current only', len(legacy_2017), len(current_2017),
        )
        return current

    legacy_mean = sum(legacy_2017) / len(legacy_2017)
    current_mean = sum(current_2017) / len(current_2017)
    factor = current_mean / legacy_mean
    log.info(
        'price splice: legacy 2017 mean=%.0f NIS, current 2017 mean=%.0f NIS, '
        'factor=%.4f', legacy_mean, current_mean, factor,
    )

    spliced: dict[str, float] = {}
    # Factored legacy for periods strictly before 2017
    for k, v in legacy.items():
        if k.startswith('2017-'):
            continue
        spliced[k] = v * factor
    # Current series wins for 2017+ (covers the overlap year and onward).
    for k, v in current.items():
        spliced[k] = v
    return spliced


# --- DB row builders ---

def parse_period(p: str) -> date:
    parts = p.split('-')
    return date(int(parts[0]), int(parts[1]), 1)


def to_price_db_rows(period_to_value: dict[str, float]) -> list[dict]:
    """Build cbs_series rows for the avg_apartment_price topic.
    Quarterly frequency; series_id = current series ID (51000) since
    that's the active methodology. Provisional flag marks the latest
    PROVISIONAL_TAIL quarters. Price is never estimated."""
    if not period_to_value:
        return []
    sorted_recent_first = sorted(period_to_value.keys(), reverse=True)
    rows: list[dict] = []
    for idx, p in enumerate(sorted_recent_first):
        rows.append({
            'series_id': str(CURRENT_PRICE_ID),
            'series_name': PRICE_NAME,
            'topic': 'avg_apartment_price',
            'district': 'national',
            'frequency': 'quarterly',
            'time_period': parse_period(p).isoformat(),
            'value': period_to_value[p],
            'is_provisional': idx < PROVISIONAL_TAIL,
            'is_derived': True,
            'is_estimated': False,
        })
    return rows


def to_affordability_db_rows(
    period_to_value: dict[str, float],
    estimated_periods: set[str],
) -> list[dict]:
    """Build cbs_series rows for the affordability_index topic.
    `estimated_periods` is the set of YYYY-MM-DD keys in
    period_to_value that are regression-estimated rather than
    real-data computed; those rows get is_estimated=true so the
    chart engine renders them with a dashed line."""
    if not period_to_value:
        return []
    sorted_recent_first = sorted(period_to_value.keys(), reverse=True)
    rows: list[dict] = []
    for idx, p in enumerate(sorted_recent_first):
        rows.append({
            'series_id': 'DERIVED',
            'series_name': AFFORDABILITY_NAME,
            'topic': 'affordability_index',
            'district': 'national',
            'frequency': 'monthly',
            'time_period': parse_period(p).isoformat(),
            'value': period_to_value[p],
            'is_provisional': idx < PROVISIONAL_TAIL,
            'is_derived': True,
            'is_estimated': p in estimated_periods,
        })
    return rows


def to_housing_alternative_db_rows(
    period_to_value: dict[str, float],
) -> list[dict]:
    """Build cbs_series rows for the housing_alternative_ratio topic.
    Quarterly. Always derived (depends on rent + price + rate);
    never estimated (no pre-2017 imputation — no rent data exists
    that far back)."""
    if not period_to_value:
        return []
    sorted_recent_first = sorted(period_to_value.keys(), reverse=True)
    rows: list[dict] = []
    for idx, p in enumerate(sorted_recent_first):
        rows.append({
            'series_id': 'DERIVED',
            'series_name': HOUSING_ALT_NAME,
            'topic': 'housing_alternative_ratio',
            'district': 'national',
            'frequency': 'quarterly',
            'time_period': parse_period(p).isoformat(),
            'value': period_to_value[p],
            'is_provisional': idx < PROVISIONAL_TAIL,
            'is_derived': True,
            'is_estimated': False,
        })
    return rows


# --- Reading existing data from supabase ---

def fetch_topic_series(
    client: Client,
    topic: str,
    *,
    frequency: str,
    district: str = 'national',
) -> dict[str, float]:
    """Read an entire cbs_series topic into a {YYYY-MM-DD: value} dict.
    Paginates 1000 rows at a time to bypass the default fetch limit."""
    out: dict[str, float] = {}
    offset = 0
    chunk = 1000
    while True:
        result = (
            client.table(TABLE)
            .select('time_period, value')
            .eq('topic', topic)
            .eq('district', district)
            .eq('frequency', frequency)
            .order('time_period')
            .range(offset, offset + chunk - 1)
            .execute()
        )
        rows = result.data or []
        for r in rows:
            out[str(r['time_period'])[:10]] = float(r['value'])
        if len(rows) < chunk:
            break
        offset += chunk
    return out


def fetch_mortgage_rates(client: Client) -> dict[str, float]:
    """Read fixed non-indexed monthly mortgage rates as
    {YYYY-MM-DD: rate_pct}. Source already monthly (one row per
    month, period-start date) per fetch_boi_mortgage_rates.py."""
    out: dict[str, float] = {}
    offset = 0
    chunk = 1000
    while True:
        result = (
            client.table('boi_mortgage_rates')
            .select('date, rate')
            .eq('rate_type', 'fixed')
            .eq('is_indexed', False)
            .order('date')
            .range(offset, offset + chunk - 1)
            .execute()
        )
        rows = result.data or []
        for r in rows:
            out[str(r['date'])[:10]] = float(r['rate'])
        if len(rows) < chunk:
            break
        offset += chunk
    return out


def fetch_national_quarterly_rent(
    client: Client, room_group: str,
) -> dict[str, float]:
    """Read national rent prices in NIS for the given room_group
    from cbs_rent_prices as {YYYY-MM-DD: rent_nis}. Populated by
    etl/ingest_rent_data.py (one-time / on-demand from manually
    extracted CBS PDFs).

    The rent table is keyed on (geography_type, geography,
    room_group, time_period); this fetcher pins the first two to
    'national' and the third to the caller-specified group. All
    persisted rows are quarterly — the table does not store
    annuals.

    Used by the registry's avg-rent-price series with
    room_group='all' (display headline), and by the housing-
    alternative-ratio computation with room_group='3.5-4' and
    '4.5-6' which are averaged to match new-apartment size."""
    out: dict[str, float] = {}
    offset = 0
    chunk = 1000
    while True:
        result = (
            client.table('cbs_rent_prices')
            .select('time_period, value')
            .eq('geography_type', 'national')
            .eq('geography', 'national')
            .eq('room_group', room_group)
            .order('time_period')
            .range(offset, offset + chunk - 1)
            .execute()
        )
        rows = result.data or []
        for r in rows:
            out[str(r['time_period'])[:10]] = float(r['value'])
        if len(rows) < chunk:
            break
        offset += chunk
    return out


# --- Affordability calculation ---

def quarter_start_for_month(period: str) -> str:
    """Return the quarter-start key (YYYY-{01|04|07|10}-01) that
    contains the given month-start key."""
    y = int(period[:4])
    m = int(period[5:7])
    qm = ((m - 1) // 3) * 3 + 1
    return f'{y:04d}-{qm:02d}-01'


def compute_monthly_payment(price_nis: float, mortgage_rate_pct: float) -> float:
    """Standard mortgage amortization formula. Returns the monthly
    payment in NIS for a 70% LTV / 25-year term loan against a
    `price_nis` apartment at `mortgage_rate_pct` annual rate.

    Defensive against a near-zero rate: when r → 0 the formula's
    denominator collapses to zero; fall back to the limiting
    case payment = loan / n."""
    loan = LTV * price_nis
    if mortgage_rate_pct < 1e-3:
        return loan / TERM_MONTHS
    r = mortgage_rate_pct / 100.0 / 12.0
    growth = (1.0 + r) ** TERM_MONTHS
    return loan * (r * growth) / (growth - 1.0)


def fetch_boi_7y_yield_monthly() -> dict[str, float]:
    """Fetch BoI's 7-year nominal zero-coupon yield curve monthly
    series. Returns {YYYY-MM-01: yield_pct}.

    Source: BoI SDMX `ZCM` dataflow, series `ZC_TSB_ZND_07Y_MA`.
    Coverage: Jan 2008 onwards. No pre-2008 data exists for any
    nominal-yield maturity in this dataflow (verified during
    investigation phase). Used both as the regression target's
    independent variable AND as the input for the 2008-2011 H1
    affordability backfill.

    Keys are normalized to YYYY-MM-01 (period-start dates) to match
    the YYYY-MM-DD convention used by every other monthly dict in
    this ETL (mortgage rates, wages). SDMX returns IDs as YYYY-MM;
    we append `-01` so set-intersection joins work without per-call
    key-shape massaging at the call sites.

    Fetched on-the-fly each ETL run rather than persisted — small
    and self-contained, with no other consumer in the codebase.
    Adds one network call to BoI per run."""
    url = f'{BOI_SDMX_BASE}/{BOI_YIELD_7Y_CODE}.M'
    headers = {'User-Agent': USER_AGENT}
    r = requests.get(url, params={'format': 'sdmx-json'},
                     headers=headers, timeout=DEFAULT_TIMEOUT)
    r.raise_for_status()
    d = r.json()
    series = d['data']['dataSets'][0].get('series', {})
    if not series:
        return {}
    first_val = next(iter(series.values()))
    periods = d['data']['structure']['dimensions']['observation'][0]['values']
    out: dict[str, float] = {}
    for k, v in (first_val.get('observations') or {}).items():
        if v and v[0] is not None:
            out[f"{periods[int(k)]['id']}-01"] = float(v[0])
    log.info('BoI 7Y yield: %d monthly obs (%s..%s)',
             len(out), min(out) if out else '—', max(out) if out else '—')
    return out


def fit_recent_regression(
    yields_monthly: dict[str, float],
    rates_monthly: dict[str, float],
) -> tuple[float, float, float]:
    """Refit `mortgage_rate = α + β × yield_7y` on the post-Nov-2018
    sub-sample (where R² is tight at ~0.92, vs the noisier ~0.65
    of the 2011-2018 portion). Returns (alpha, beta, r²).

    The sub-sample choice is documented in the methodology page —
    earlier periods reflect different banking-competition and
    macroprudential regimes that don't carry forward cleanly. Using
    recent-window coefficients essentially asks "what would
    today's-style mortgage cost given historical yields?" which is
    a more useful question than trying to model the actual 2008-era
    mortgage market.

    Refits each ETL run rather than hard-coding constants — keeps
    the coefficients self-updating as new data arrives, at the cost
    of tiny drift in historical estimates between runs.

    Logs α, β, R² so the operator can spot drift over time.
    """
    pairs = sorted(
        (k, yields_monthly[k], rates_monthly[k])
        for k in rates_monthly
        if k in yields_monthly and k >= REGRESSION_FIT_FROM
    )
    if len(pairs) < 12:
        raise RuntimeError(
            f'regression refit: only {len(pairs)} months in fit window — '
            f'need 12+. Check that mortgage rates and yields are both '
            f'populated through {REGRESSION_FIT_FROM}+.'
        )
    xs = [p[1] for p in pairs]
    ys = [p[2] for p in pairs]
    n = len(xs)
    mx = sum(xs) / n
    my = sum(ys) / n
    sxx = sum((x - mx) ** 2 for x in xs)
    syy = sum((y - my) ** 2 for y in ys)
    sxy = sum((x - mx) * (y - my) for x, y in zip(xs, ys))
    if sxx <= 0:
        raise RuntimeError('regression refit: zero variance in yields')
    beta = sxy / sxx
    alpha = my - beta * mx
    rss = sum((y - (alpha + beta * x)) ** 2 for x, y in zip(xs, ys))
    r2 = 1 - rss / syy if syy > 0 else 0.0
    log.info(
        'regression refit (%s..%s, n=%d): alpha=%.4f, beta=%.4f, R²=%.4f',
        pairs[0][0], pairs[-1][0], n, alpha, beta, r2,
    )
    return alpha, beta, r2


def annual_average(monthly: dict[str, float], year: int) -> float | None:
    """Mean of monthly values within `year`. None if no observations."""
    vals = [v for k, v in monthly.items() if k.startswith(f'{year:04d}-')]
    if not vals:
        return None
    return sum(vals) / len(vals)


def annual_average_quarterly(quarterly: dict[str, float], year: int) -> float | None:
    """Mean of the (up to 4) quarterly values for `year`. Quarterly
    keys are 'YYYY-{01|04|07|10}-01'."""
    vals = [v for k, v in quarterly.items()
            if k.startswith(f'{year:04d}-') and k[5:7] in ('01', '04', '07', '10')]
    if not vals:
        return None
    return sum(vals) / len(vals)


def estimate_pre2011_affordability(
    prices_quarterly: dict[str, float],
    wages_monthly: dict[str, float],
    yields_monthly: dict[str, float],
    alpha: float,
    beta: float,
) -> dict[str, float]:
    """Compute estimated affordability for 2008-Jan through 2011-Jun
    (the last month before real mortgage data starts).

    Annual resolution: for each year, mean the monthly yields ⇒
    estimated annual mortgage rate ⇒ standard affordability formula
    using annual averages of price and wage. The single annual value
    is then written to every month within that year, so the chart
    sees a step-function during the estimated regime (no fake
    monthly variation) and a smooth value transition at 2011-07
    where real-data resolution kicks in.

    Returns {YYYY-MM-01: affordability_pct}. 2011-Jul through 2011-Dec
    are NOT included here — those use the real-data calculation.
    """
    out: dict[str, float] = {}
    end_year = int(ESTIMATED_THROUGH[:4])
    end_month = int(ESTIMATED_THROUGH[5:7])
    for year in range(ESTIMATED_FIRST_YEAR, end_year + 1):
        yield_avg = annual_average(yields_monthly, year)
        price_avg = annual_average_quarterly(prices_quarterly, year)
        wage_avg = annual_average(wages_monthly, year)
        if yield_avg is None or price_avg is None or wage_avg is None:
            log.warning(
                'estimated %d skipped: yield=%s, price=%s, wage=%s',
                year, yield_avg, price_avg, wage_avg,
            )
            continue
        if wage_avg <= 0:
            log.warning('estimated %d skipped: zero/negative wage', year)
            continue
        est_mortgage_pct = alpha + beta * yield_avg
        monthly_payment = compute_monthly_payment(price_avg, est_mortgage_pct)
        net_household_income = (
            2.0 * wage_avg * WAGE_TO_NET_HOUSEHOLD_MULTIPLIER
        )
        affordability = monthly_payment / net_household_income * 100.0
        log.info(
            'estimated %d: yield=%.2f%%, est_mortgage=%.2f%%, '
            'price=%.0f, wage=%.0f, affordability=%.2f%%',
            year, yield_avg, est_mortgage_pct,
            price_avg, wage_avg, affordability,
        )
        # Expand annual value to monthly rows for visual continuity.
        # In the final year (2011), only emit Jan..Jun — Jul onwards
        # uses real mortgage data via the standard path.
        last_month = end_month if year == end_year else 12
        for m in range(1, last_month + 1):
            out[f'{year:04d}-{m:02d}-01'] = affordability
    return out


def compute_affordability(
    prices_quarterly: dict[str, float],
    wages_monthly: dict[str, float],
    rates_monthly: dict[str, float],
) -> dict[str, float]:
    """Per month with all three inputs available, compute the
    affordability ratio (% of estimated NET two-earner household
    income spent on the monthly mortgage payment). The denominator
    applies WAGE_TO_NET_HOUSEHOLD_MULTIPLIER to convert the pre-tax
    wage to post-tax disposable income. Step-function on price (the
    quarter's value covers all three of its months)."""
    months_with_inputs = sorted(set(wages_monthly.keys()) & set(rates_monthly.keys()))
    out: dict[str, float] = {}
    skipped_no_price = 0
    skipped_zero_wage = 0
    for month in months_with_inputs:
        q_key = quarter_start_for_month(month)
        if q_key not in prices_quarterly:
            skipped_no_price += 1
            continue
        wage = wages_monthly[month]
        if wage <= 0:
            skipped_zero_wage += 1
            continue
        price = prices_quarterly[q_key]
        rate = rates_monthly[month]
        monthly_payment = compute_monthly_payment(price, rate)
        net_household_income = 2.0 * wage * WAGE_TO_NET_HOUSEHOLD_MULTIPLIER
        out[month] = monthly_payment / net_household_income * 100.0
    log.info(
        'affordability: %d months computed (skipped %d for missing price, '
        '%d for zero/missing wage)',
        len(out), skipped_no_price, skipped_zero_wage,
    )
    return out


def compute_housing_alternative_ratio(
    prices_quarterly: dict[str, float],
    rates_monthly: dict[str, float],
    rents_3_4: dict[str, float],
    rents_4_6: dict[str, float],
) -> dict[str, float]:
    """Compute the rent-to-mortgage cash-flow ratio per quarter.

    For each quarter where BOTH rent series and price are available,
    locate the mortgage rate for the FIRST month of that quarter
    (Jan/Apr/Jul/Oct), compute the standard 70% LTV / 25-year
    monthly mortgage payment, and divide the (3.5-4 + 4.5-6)/2 rent
    average by that payment.

    The rent inputs are the two larger room-group categories — not
    the 'all' bucket — to match the typical size of new apartments
    in the spliced apartment-price series. Using 'all' would mix
    in 1-2 and 2.5-3 rentals and drag the rent down by ~25-30%,
    artificially deflating the ratio.

    Result is in percent (×100): values <100 mean renting is
    cheaper, >100 means renting costs more than the mortgage cash
    flow. Pure cash-flow comparison — see methodology page for
    the explicit caveats (no property tax, maintenance, foregone
    interest on down payment, capital appreciation, ownership).

    Skips quarters where any input is missing — partial coverage
    (one rent group present, the other not) is dropped rather
    than computed with a half-mean. Skips count is logged but not
    warned per-row."""
    out: dict[str, float] = {}
    skipped_no_rate = 0
    skipped_no_price = 0
    skipped_partial_rent = 0
    quarters = sorted(set(rents_3_4) | set(rents_4_6))
    for q_key in quarters:
        if q_key not in rents_3_4 or q_key not in rents_4_6:
            skipped_partial_rent += 1
            continue
        if q_key not in prices_quarterly:
            skipped_no_price += 1
            continue
        if q_key not in rates_monthly:
            skipped_no_rate += 1
            continue
        avg_rent = (rents_3_4[q_key] + rents_4_6[q_key]) / 2.0
        price = prices_quarterly[q_key]
        rate = rates_monthly[q_key]
        monthly_payment = compute_monthly_payment(price, rate)
        if monthly_payment <= 0:
            continue
        out[q_key] = avg_rent / monthly_payment * 100.0
    log.info(
        'housing alternative ratio: %d quarters computed (skipped %d for '
        'missing price, %d for missing rate at quarter start, %d for '
        'partial rent coverage [only one of 3.5-4 / 4.5-6])',
        len(out), skipped_no_price, skipped_no_rate, skipped_partial_rent,
    )
    return out


# --- Upsert ---

def upsert_rows(client: Client, rows: list[dict]) -> int:
    if not rows:
        return 0
    sent = 0
    for start in range(0, len(rows), BATCH_SIZE):
        batch = rows[start : start + BATCH_SIZE]
        client.table(TABLE).upsert(
            batch, on_conflict='topic,district,frequency,time_period'
        ).execute()
        sent += len(batch)
        log.info('upserted batch %d-%d (%d total)', start, start + len(batch), sent)
    return sent


# --- Entry point ---

def main() -> int:
    load_dotenv(Path(__file__).parent / '.env')
    url = os.environ.get('SUPABASE_URL')
    key = os.environ.get('SUPABASE_SERVICE_ROLE_KEY')
    if not url or not key:
        log.error('SUPABASE_URL and SUPABASE_SERVICE_ROLE_KEY must be set')
        return 2

    failures: list[str] = []
    spliced_prices: dict[str, float] = {}

    # 1. Fetch + splice avg_apartment_price
    try:
        sess = make_session()
    except Exception as exc:
        log.exception('boards-generator session warmup failed: %s', exc)
        failures.append('avg_apartment_price (session)')
        sess = None

    prices_current: dict[str, float] = {}
    prices_legacy: dict[str, float] = {}
    if sess is not None:
        try:
            rows_current = fetch_grid(
                sess, CURRENT_PRICE_ID, CURRENT_PRICE_SUBJECT,
                CURRENT_FYEAR, CURRENT_TYEAR,
            )
            prices_current = normalize_grid_rows(rows_current)
        except Exception as exc:
            log.exception('51000 fetch/parse failed: %s', exc)
            failures.append('avg_apartment_price (current)')
        try:
            rows_legacy = fetch_grid(
                sess, LEGACY_PRICE_ID, LEGACY_PRICE_SUBJECT,
                LEGACY_FYEAR, LEGACY_TYEAR,
            )
            prices_legacy = normalize_grid_rows(rows_legacy)
        except Exception as exc:
            log.exception('20000 fetch/parse failed: %s', exc)
            failures.append('avg_apartment_price (legacy)')

    if prices_current or prices_legacy:
        spliced_prices = compute_price_splice(prices_legacy, prices_current)

    price_rows = to_price_db_rows(spliced_prices)
    log.info('avg_apartment_price: %d quarterly rows ready', len(price_rows))

    # 2. Compute affordability_index
    client = create_client(url, key)
    affordability_rows: list[dict] = []
    try:
        wages = fetch_topic_series(client, 'average_wage', frequency='monthly')
        rates = fetch_mortgage_rates(client)
        log.info('affordability inputs: wages=%d, rates=%d', len(wages), len(rates))
        # If avg_apartment_price wasn't fetched fresh this run (e.g.,
        # session failure), fall back to whatever was previously
        # upserted so we can still recompute affordability with the
        # fresher wage/rate data.
        prices_for_calc = spliced_prices
        if not prices_for_calc:
            log.info('no fresh price data; loading avg_apartment_price from DB')
            prices_for_calc = fetch_topic_series(
                client, 'avg_apartment_price', frequency='quarterly',
            )

        # 2a. Real-data affordability (months with all three direct
        # inputs available — typically Jul 2011 onwards).
        real_affordability: dict[str, float] = {}
        if prices_for_calc and wages and rates:
            real_affordability = compute_affordability(
                prices_for_calc, wages, rates,
            )
        else:
            log.warning(
                'real-data affordability skipped: prices=%d, wages=%d, rates=%d',
                len(prices_for_calc), len(wages), len(rates),
            )
            failures.append('affordability_index (missing real-data inputs)')

        # 2b. Estimated affordability (2008-2010 + 2011 H1). Fetches
        # BoI 7Y yields, refits the regression on the recent window,
        # and applies α + β·yield to the annual yield averages. See
        # estimate_pre2011_affordability for the full math.
        estimated: dict[str, float] = {}
        try:
            yields = fetch_boi_7y_yield_monthly()
            if yields and rates and prices_for_calc and wages:
                alpha, beta, _ = fit_recent_regression(yields, rates)
                estimated = estimate_pre2011_affordability(
                    prices_for_calc, wages, yields, alpha, beta,
                )
            else:
                log.warning(
                    'estimated-affordability skipped: yields=%d, rates=%d, '
                    'prices=%d, wages=%d',
                    len(yields), len(rates), len(prices_for_calc), len(wages),
                )
        except Exception as exc:
            log.exception('estimated-affordability calc failed: %s', exc)
            # Don't bail — real-data rows can still upsert.

        # 2c. Merge: real-data rows win where they exist (for any
        # period we have direct mortgage data, prefer it over the
        # estimate). estimated_periods records which keys came from
        # the estimation path so the row builder can flag them.
        merged: dict[str, float] = {}
        merged.update(estimated)
        merged.update(real_affordability)
        estimated_periods = {k for k in estimated if k not in real_affordability}

        affordability_rows = to_affordability_db_rows(merged, estimated_periods)
        log.info(
            'affordability rows: %d total (%d estimated, %d real-data)',
            len(affordability_rows), len(estimated_periods),
            len(affordability_rows) - len(estimated_periods),
        )
    except Exception as exc:
        log.exception('affordability computation failed: %s', exc)
        failures.append('affordability_index')

    # 3. Compute housing_alternative_ratio (rent ÷ mortgage payment).
    # Independent of affordability — runs even if (2) failed, since
    # only price + rate + rent are needed (no wage). Stays empty if
    # the rent table hasn't been ingested yet (`ingest_rent_data.py`).
    # Two rent room-groups are fetched separately and averaged inside
    # the compute function — see its docstring for why we don't use
    # 'all'.
    housing_alt_rows: list[dict] = []
    try:
        rents_3_4 = fetch_national_quarterly_rent(client, '3.5-4')
        rents_4_6 = fetch_national_quarterly_rent(client, '4.5-6')
        log.info(
            'housing-alt inputs: rents 3.5-4=%d quarters, rents 4.5-6=%d quarters',
            len(rents_3_4), len(rents_4_6),
        )
        if rents_3_4 and rents_4_6 and prices_for_calc and rates:
            ratio = compute_housing_alternative_ratio(
                prices_for_calc, rates, rents_3_4, rents_4_6,
            )
            housing_alt_rows = to_housing_alternative_db_rows(ratio)
            log.info('housing_alternative_ratio rows: %d', len(housing_alt_rows))
        else:
            log.warning(
                'housing_alternative_ratio skipped: rents_3_4=%d, '
                'rents_4_6=%d, prices=%d, rates=%d',
                len(rents_3_4), len(rents_4_6), len(prices_for_calc), len(rates),
            )
            if not rents_3_4 or not rents_4_6:
                # Not a hard failure — the rent table simply isn't
                # (fully) ingested yet. Don't append to `failures`
                # for that case; ETL exits 0 if everything else
                # worked.
                pass
            else:
                failures.append('housing_alternative_ratio')
    except Exception as exc:
        log.exception('housing_alternative_ratio computation failed: %s', exc)
        failures.append('housing_alternative_ratio')

    # 4. Upsert everything
    all_rows = price_rows + affordability_rows + housing_alt_rows
    if not all_rows:
        log.error('no rows to upsert; failures=%s', failures)
        return 1

    try:
        sent = upsert_rows(client, all_rows)
    except Exception as exc:
        log.exception('upsert failed: %s', exc)
        return 1

    log.info(
        'Done. Upserted %d rows (%d prices + %d affordability + %d housing-alt); '
        'failures=%s',
        sent, len(price_rows), len(affordability_rows), len(housing_alt_rows),
        failures or '—',
    )
    return 1 if failures else 0


if __name__ == '__main__':
    sys.exit(main())
