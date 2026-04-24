# DiraMetrics TODO

## Infrastructure

- [ ] **By June 2026:** Migrate GitHub Actions to Node 24-compatible action versions.
  - Affects: `.github/workflows/daily-etl.yml`
  - Currently using: `actions/checkout@v4`, `actions/setup-python@v5`
  - Need to upgrade when maintainers release Node 24 versions
  - Deadline context: forced transition June 2026, full removal September 2026

## Data sources

- [ ] Resolve CBS new housing price index (series 70000) — API returns HTTP 500. 
  See etl/NOTES_CBS.md for details.

## Deferred decisions

- [ ] Define affordability index formula (include monthly payment vs income + equity requirement)
- [ ] Decide which mortgage rate to show as headline KPI (indexed fixed vs unindexed fixed)
- [ ] Refine CPI "provisional" flagging — currently top-3 are marked provisional, but CPI may not actually have provisional values

## Future ETLs (not yet implemented)

- [ ] CBS building permits (XLSX)
- [ ] CBS construction starts (XLSX)
- [ ] CBS construction completions (XLSX)
- [ ] CBS active construction (XLSX)
- [ ] CBS new sales - free market (XLSX)
- [ ] CBS new sales - subsidized (XLSX)
- [ ] CBS second-hand sales (XLSX)
- [ ] CBS new apartment inventory (XLSX)
- [ ] CBS housing price index by district