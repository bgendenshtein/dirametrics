# CBS Price-Index ETL Notes

Operational notes for `etl/fetch_cbs_price_indices.py`.

**Last updated:** 2026-04-24

## Table rename

The Supabase table was renamed `cbs_housing_prices` → `cbs_price_indices`.
The original name was too narrow: we want to ingest rent and headline CPI
into the same table alongside housing prices, since they share the same
schema (`series_id, series_name, date, value, is_provisional`) and the
same provisional-revision semantics.

The script was renamed to match: `fetch_cbs_housing_prices.py` →
`fetch_cbs_price_indices.py`.

## Series fetched

| codeId | Name (he)                              | Chapter / subject             | Notes |
|--------|----------------------------------------|-------------------------------|-------|
| 40010  | מדד מחירי דירות                         | chapter `aa` / subject 45     | Housing prices, monthly back to 1994. |
| 120460 | מדד שכר דירה                           | chapter `a` (CPI)             | Rent paid by tenants — see cross-check below. |
| 120010 | מדד המחירים לצרכן - כללי                | chapter `a` (CPI)             | Headline CPI, base 2024 average. |

## Cross-check: 120460 is rent (not imputed owner-occupier cost)

Between candidates 120440 and 120460:

- **`120460`** resolves to `שכר דירה פרטי, ציבורי ושכירות ארוכת טווח בפיקוח ממשלתי`
  ("Private rent + public rent + long-term rent under government
  supervision"). The name literally says שכר דירה (rent), and the
  three sub-populations it enumerates are all tenant-paid categories.
  This is the index we want.
- **`120440`** resolves to `מצרכים בקיוסקים ובחנויות נוחות` ("Goods at
  kiosks and convenience stores") — a consumer-goods index, unrelated
  to rent. It was a red herring from earlier research.

CBS also publishes a separate **imputed** housing-services index for
owner-occupiers (variants of "שירותי דיור בבעלות הדיירים"). That is
*not* what 120460 is, and we specifically want the tenant-paid number.

**Value cross-check:** 120460's YoY for 2026-03 came back at **+3.3%**,
consistent with the CBS Nov-2025 press release combining ~2.5% on lease
renewals and ~5.5% on new tenants (weighted average lands ~3.3%).

## Known API issues

### New-housing series (70000) — HTTP 500

The new-housing index (`מדד מחירי דירות חדשות`, chapter `aa` subject 167,
codeId 70000) is correctly cataloged but the data endpoint returns
HTTP 500 `{"Message":"Error: Price Data"}` as of **2026-04-24**.

Retries with prefixed variants (`1170000`, `11070000`) return HTTP 200
but empty payloads, confirming the catalog ID is correct and the issue
is server-side. Not currently fetched. Re-check periodically; if CBS
fixes the endpoint, add an entry to `SERIES` with this codeId.

## Provisional flag semantics

CBS publishes the 3 most recent observations per series as provisional.
The script flags `is_provisional=true` on the top 3 *per series* (not
across the combined batch), which matters now that multiple series are
fetched in one run.

Upsert on `(series_id, date)` means re-running the job:
- overwrites a provisional value with the latest revision;
- promotes a previously-provisional row to `is_provisional=false` once a
  newer month pushes it out of that series's top-3 window.
